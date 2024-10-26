# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import logging
import uuid

from raff.common.db.redshift_db import RedshiftDb
from raff.burst.burst_test import setup_teardown_burst
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode,\
    prepare_burst, get_burst_conn_params
from raff.common.simulated_helper import create_localhost_snapshot
from raff.storage.alter_table_suite import AlterTableSuite
from test_burst_write_equivalence import TestBurstWriteBlockEquivalenceBase
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions
from raff.common.host_type import HostType

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst]

DROP_TABLE = "DROP TABLE IF EXISTS burst_tbl_{};"
DROP_CTAS_TABLE = "DROP TABLE IF EXISTS ctas_burst_tbl_{};"
CREATE_TABLE = "create table public.burst_tbl_{}(c0 int, c1 int) {} {};"
CTAS_CMD = ("create table public.ctas_burst_tbl_{table} {} {} as select *"
            " from burst_tbl_{table}")
GRANT_CMD = "GRANT ALL ON burst_tbl_{} TO PUBLIC;"
INSERT_SELECT_CMD = ("insert into burst_tbl_{} " "select * from burst_tbl_{};")
S3_PATH = 's3://cookie-monster-s3-ingestion/schema-quota/2-cols-21-rows.csv'
COPY_STMT = ("COPY {} "
             "FROM "
             "'{}' "
             "DELIMITER ',' "
             "CREDENTIALS "
             "'aws_iam_role=arn:aws:iam::467896856988:role/Redshift-S3';")
TABLE_EXISTS_QUERY = ("select * from "
                      "pg_catalog.pg_class where relname='{}';")


class BurstWriteRestartBase(
        AlterTableSuite, TestBurstWriteBlockEquivalenceBase, BurstWriteTest):
    def insert_select(self, cluster, cursor, tbl, num=1, burst=True):
        if burst:
            cursor.execute("set query_group to burst;")
        else:
            cursor.execute("set query_group to metrics;")
        for i in range(num):
            cursor.execute(INSERT_SELECT_CMD.format(tbl, tbl))
            if burst:
                self._check_last_query_bursted(cluster, cursor)
            else:
                self._check_last_query_didnt_burst(cluster, cursor)

    def do_ctas(self, cluster, cursor, tbl, num=1, burst=True,
                sortkey='', diststyle=''):
        if burst:
            cursor.execute("set query_group to burst;")
        else:
            cursor.execute("set query_group to metrics;")
        for i in range(num):
            cursor.execute(CTAS_CMD.format(sortkey, diststyle, table=tbl))
            if burst:
                self._check_last_query_bursted(cluster, cursor)
            else:
                self._check_last_query_didnt_burst(cluster, cursor)

    def _copy_tables(self, cluster, cursor, base_tbl_name):
        # Copy when set not bursted.
        cursor.execute("set query_group to metrics;")
        cursor.execute(COPY_STMT.format(base_tbl_name + "_not_burst", S3_PATH))
        copy_rows = cursor.last_copy_row_count()
        assert copy_rows == 21
        self._check_last_query_didnt_burst(cluster, cursor)
        # Copy when set bursted.
        cursor.execute("set query_group to burst;")
        cursor.execute(COPY_STMT.format(base_tbl_name + "_burst", S3_PATH))
        copy_rows = cursor.last_copy_row_count()
        assert copy_rows == 21
        self._check_last_copy_bursted(cluster, cursor)

    def setup_tables(self, cursor, diststyle, sortkey):
        cursor.execute("set query_group to metrics;")
        cursor.execute(DROP_TABLE.format("burst"))
        cursor.execute(DROP_CTAS_TABLE.format("burst"))
        cursor.execute(CREATE_TABLE.format("burst", diststyle, sortkey))
        cursor.execute(GRANT_CMD.format("burst"))
        cursor.execute(DROP_TABLE.format("not_burst"))
        cursor.execute(DROP_CTAS_TABLE.format("not_burst"))
        cursor.execute(CREATE_TABLE.format("not_burst", diststyle, sortkey))
        cursor.execute(GRANT_CMD.format("not_burst"))

    def setup_data(self, cluster, cursor, cursor_bs):
        base_tbl_name = "burst_tbl"
        self._insert_tables(cluster, cursor, base_tbl_name)
        self.insert_select(cluster, cursor, "burst", 3, True)
        self.insert_select(cluster, cursor, "not_burst", 3, False)
        self._update_tables_1(cluster, cursor, base_tbl_name)
        self._update_tables_2(cluster, cursor, base_tbl_name)
        self._copy_tables(cluster, cursor, base_tbl_name)
        self._delete_tables(cluster, cursor, base_tbl_name)
        self._start_and_wait_for_refresh(cluster)
        self._validate_table_data(cluster, cursor, cursor_bs)

    def _validate_table_data(self, cluster, cursor, cursor_bs):
        base_tbl_name = "burst_tbl"
        burst_tbl_name = "burst_tbl_burst"
        gold_tbl_name = "burst_tbl_not_burst"
        super(TestBurstWriteBlockEquivalenceBase,
              self)._validate_content_equivalence(
                  cluster, cursor, burst_tbl_name, gold_tbl_name)
        # Metadata can only check under super simulated mode.
        if cluster.host_type != HostType.CLUSTER:
            self._validate_metadata_equivalence(cursor_bs, base_tbl_name)

    def _validate_ctas_table_data(self, cluster, cursor, cursor_bs):
        base_tbl_name = "ctas_burst_tbl"
        burst_tbl_name = "ctas_burst_tbl_burst"
        gold_tbl_name = "ctas_burst_tbl_not_burst"
        super(TestBurstWriteBlockEquivalenceBase,
              self)._validate_content_equivalence(
                  cluster, cursor, burst_tbl_name, gold_tbl_name)
        # Metadata can only check under super simulated mode.
        if cluster.host_type != HostType.CLUSTER:
            self._validate_metadata_equivalence(cursor_bs, base_tbl_name)

    def _check_table_does_not_exist(self, cursor, table):
        cursor.execute(TABLE_EXISTS_QUERY.format(table))
        assert cursor.fetchall() == []

    def base_test_burst_write_restart(self, db_session, vector, cluster,
                                      bg_commit):
        """
        The test crash PADB at burst write commit and validate table content
        after restart. The test does the following:
        1. Setup two tables: one for burst write, the other only run DMLs on
           main cluster.
        2. Run same DMLs on the tables.
        3. When running the last burst write, crash PADB manually and restart
           at different crash position.
        4. Compare two tables content and confirm they are the same.
        """
        fixed_slice_set = "xpx 'event set EtSimulateStartFromFixedSlice'"
        fixed_slice_unset = "xpx 'event set EtSimulateStartFromFixedSlice'"
        if cluster.host_type != HostType.CLUSTER:
            burst_db = RedshiftDb(conn_params=get_burst_conn_params())
            with burst_db.cursor() as burst_cursor:
                burst_cursor.execute(fixed_slice_set)
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        with db_session_master.cursor() as cursor, \
                db_session.cursor() as cursor_bs:
            self.setup_tables(cursor_bs, vector.diststyle, vector.sortkey)
            self._start_and_wait_for_refresh(cluster)
            if cluster.host_type != HostType.CLUSTER:
                cursor_bs.execute(fixed_slice_set)
            self.setup_data(cluster, cursor, cursor_bs)
            if vector.cmd == 'ctas':
                self.do_ctas(cluster, cursor, "not_burst", 1, False,
                             sortkey=vector.sortkey, diststyle=vector.diststyle)
            else:
                self.insert_select(cluster, cursor, "not_burst", 2, False)
            # To make sure content are the same in 2 tables at last. The insert
            # here should have different times based on crash pos.
            # 1. Insert 2 rows in non-burst table on main cluster.
            #    For CTAS there is no initial table setup needed.
            # 2. For EtCrashCommitBeforeP1, insert 2 rows on burst-table.
            #    3-rd insert below will crash before its persisted.
            #    For CTAS we will only run the query that crashes. Expect
            #    CTAS created table not to persist.
            # 3. For EtCrashCommitAfterP1, insert 1 row on burst-table.
            #    2-nd insert below will crash after its persisted.
            #    For CTAS we will only run the query that crashes. Expect
            #    CTAS created table to be persisted.
            # 4. Restart cluster and check both tables - burst
            #    and non-burst - have same content.
            if vector.crash_pos == "EtCrashCommitBeforeP1":
                # For EtCrashCommitBeforeP1, PADB is crashed after data is not
                # persisted. Insert 2 times before restart on insert.
                if vector.cmd == 'ctas':
                    pass
                else:
                    self.insert_select(cluster, cursor, "burst", 2, True)
            else:
                # For EtCrashCommitAfterP1, PADB is crashed after data is
                # persisted. Insert 1 time before restart on insert.
                if vector.cmd == 'ctas':
                    pass
                else:
                    self.insert_select(cluster, cursor, "burst", 1, True)

            # Trigger crash and restart when burst write
            try:
                cluster.set_event(vector.crash_pos)
                if vector.cmd == 'ctas':
                    self.do_ctas(
                        cluster,
                        cursor,
                        "burst",
                        1,
                        True,
                        sortkey=vector.sortkey,
                        diststyle=vector.diststyle)
                else:
                    self.insert_select(cluster, cursor, "burst", 1, True)
                if bg_commit == "True":
                    cluster.run_xpx('hello')

            except Exception as e:
                log.info("error message: " + str(e))
                assert "non-std exception" in str(e)

            conn_params = cluster.get_conn_params()
            control_conn = RedshiftDb(conn_params)
            self.wait_for_crash(conn_params, control_conn)
            cluster.reboot_cluster()

        cluster.wait_for_cluster_available(180)
        # Take snapshot to run validation on burst
        snapshot_id = "{}-{}".format('burst-snapshot', str(uuid.uuid4().hex))
        if cluster.host_type == HostType.CLUSTER:
            cluster.backup_cluster(snapshot_id)
        else:
            create_localhost_snapshot(snapshot_id, wait=True)
            # Restart super simulated mode after restart
            prepare_burst({'burst_enable_write': 'true'})
            log.info("Restart super simulated Burst cluster is ready.")
        with db_session_master.cursor() as cursor, \
                db_session.cursor() as cursor_bs:
            if cluster.host_type != HostType.CLUSTER:
                cursor_bs.execute(fixed_slice_unset)
            # allow us to validate metadata on main and burst
            self._start_and_wait_for_refresh(cluster)
            if vector.cmd == 'ctas':
                # If crash before commit p1 check that the
                # table does not exist.
                if vector.crash_pos == "EtCrashCommitBeforeP1":
                    self._check_table_does_not_exist(cursor_bs,
                                                     "ctas_burst_tbl_burst")
                else:
                    cluster.run_xpx("sortedness_checker {} 0 1"
                                    .format("ctas_burst_tbl_burst"))
                    self._validate_ctas_table_data(cluster, cursor, cursor_bs)
            else:
                cluster.run_xpx("sortedness_checker {} 0 1".format("burst_tbl_burst"))
                self._validate_table_data(cluster, cursor, cursor_bs)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.encrypted_only
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs={
    'burst_enable_write': 'true'
})
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.session_ctx(user_type='bootstrap')
class TestBurstWriteRestartSS(BurstWriteRestartBase):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['distkey(c1)', 'diststyle even'],
                sortkey=['sortkey(c0)'],
                crash_pos=['EtCrashCommitBeforeP1', 'EtCrashCommitAfterP1'],
                cmd=['insert', 'ctas']
            ))

    @pytest.mark.parametrize("bg_commit", ['True', 'False'])
    def test_burst_write_restart(self, db_session, vector, cluster, bg_commit):
        self.base_test_burst_write_restart(db_session, vector, cluster,
                                           bg_commit)
