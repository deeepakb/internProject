# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import datetime
import getpass
import logging
import pytest
from contextlib import contextmanager
from query_profiler.profiler.retry.retrying import retry
from raff.common.db.redshift_db import RedshiftDb
from raff.common.db.session import DbSession
from raff.burst.burst_super_simulated_mode_helper import (
    super_simulated_mode,
    get_burst_conn_params
    )
from raff.burst.burst_write import BurstWriteTest
from raff.common.dimensions import Dimensions

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

TEST_TABLE = "notes"
AUTOVACUUM_XIDS = ("select xid, status from stl_vacuum "
                   "where status ilike '%bg%' and eventtime >= '{}' and "
                   "table_id in "
                   "(select distinct id from stv_tbl_perm "
                   "where name = '{}')")
CTR_TABLE_LIST = ("select btrim(tables) from stl_burst_refresh_tables_list where "
                  "list_type = 1 and record_time >= '{}' and "
                  "tables ilike '%{}%'")
GET_TABLE_ID_CMD = ("""
                   select c.oid from pg_class c
                   join pg_namespace n ON c.relnamespace = n.oid
                   where c.relname = '{}' and n.nspname = '{}'
                   """)
CREATE_TABLE = ("""
               create table {} (
               col_1 varchar(255) encode text255,
               col_2 varchar(400) encode lzo,
               col_3 varchar(50) encode text255,
               col_4 timestamp encode lzo,
               col_5 varchar(255) encode text255,
               col_6 timestamp encode lzo,
               col_7 timestamp encode lzo,
               col_8 character varying(30) encode text255,
               col_9   character varying(255) encode text255,
               col_10 character varying(255) encode text255,
               col_11 character varying(255) encode text255,
               col_12 character varying(510) encode lzo,
               col_13 timestamp encode lzo,
               col_14 timestamp encode lzo,
               col_15 bigint encode az64,
               col_16 bigint encode az64,
               col_17 bigint encode az64
               ) diststyle even;
               """)
COPY_QUERY = """
            copy {} from 's3://cookiemonster-burst-write-copy-test/notes.csv.gz'
            credentials 'aws_iam_role=arn:aws:iam::467896856988:role/Redshift-S3'
            delimiter '|' ignoreheader 1 truncatecolumns trimblanks acceptinvchars
            emptyasnull COMPUPDATE
            OFF TIMEFORMAT AS 'yyyy-mm-dd hh:mi:ss' format csv gzip;
             """
VACUUM = "VACUUM {} TO 100 PERCENT;"

@contextmanager
def abort_vacuum_runner_after_pre_vacuum_run(cluster):
    cluster.set_event('EtAbortVacuumRunnerAfterPreVacuumRun')
    yield
    cluster.unset_event('EtAbortVacuumRunnerAfterPreVacuumRun')


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.session_ctx(user_type='bootstrap')
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(gucs={'burst_enable_write': 'true'})
@pytest.mark.custom_local_gucs(
    gucs={
        'vacuum_auto_enable': 'true',
        'vacuum_auto_schedule_interval_sec': '10',
        'vacuum_auto_worker_enable': 'true',
        'vacuum_auto_min_activation_threshold': '1',
        'restore_structure_change_for_read_only_vac': 'true',
        'auto_tuner_vacuum_sort_worker_enable': 'false',
        'auto_tuner_vacuum_delete_worker_enable': 'false',
        'enable_optima': 'false',
        'skip_burst_refresh_after_read_only_vac': 'true'
    })
class TestBurstWriteWithNoOpBgVacuum(BurstWriteTest):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(dict(commit_type=["commit", "abort"]))

    @retry(
        # Wait for maximum of 3 minutes
        retry_on_result=lambda res: len(res) < 2,
        wait_fixed=10 * 1000,
        stop_max_attempt_number=18)
    def wait_for_vacuum_exec(self, cursor, event_time, table_name):
        """
        Check stl_vacuum table periodically until start and finish are logged

        Args:
            cursor: db cursor.
            event_time: filter out records prior to the timestamp.
        """
        query = AUTOVACUUM_XIDS.format(event_time, table_name)
        cursor.execute(query)
        res = cursor.fetchall()
        log.info("Got {} records for query\n{}\n Expect {}".format(
            len(res), query, 2))
        return res

    def _setup_table(self, cursor, tbl_name):
        """
        Creates the table and insert some data.
        """
        cursor.execute("drop table if exists {};".format(tbl_name))
        cursor.execute("create table if not exists {} (c1 int, c2 int) "
                       "distkey(c2);".format(tbl_name))
        cursor.execute(
            "insert into {} values (15213, 15410);".format(tbl_name))
        cursor.execute(
            "insert into {} values (18746, 15440);".format(tbl_name))
        cursor.execute(
            "insert into {} values (15645, 15721);".format(tbl_name))
        cursor.execute("insert into {} values (1, 1);".format(tbl_name))
        for i in range(10):
            cursor.execute("insert into {} select * from {}".format(
                tbl_name, tbl_name))
        cursor.execute("insert into {} values (2, 2);".format(tbl_name))

    def _get_table_id(self, cursor, schema, table_name):
        """
            Returns table id from pg_class given table name
        """
        query = GET_TABLE_ID_CMD.format(table_name, schema)
        cursor.execute(query)
        return cursor.fetchall()[0][0]

    def _trigger_bg_vacuum(self, cluster, cursor, start_str, vector):
        # Delete some data to trigger bg vacuum.
        cursor.execute("delete from {} where c1 = 2;".format(TEST_TABLE))
        cluster.run_xpx("auto_worker enable vacuum")
        xids = self.wait_for_vacuum_exec(cursor, start_str, TEST_TABLE)
        if vector.commit_type == 'commit':
            assert len(xids) == 2, xids
            assert xids[0][0] == xids[1][0]
        return xids[0][0]

    def _verify_structure_change_closed_and_query_can_burst(
            self, cluster, cursor, vacuum_xid, start_str, vector):
        # Verify it's a no op vacuum.
        cursor.execute("select btrim(querytxt) from stl_query where xid = {} "
                       "order by starttime;".format(vacuum_xid))
        vac_queries = cursor.fetchall()
        assert len(vac_queries) == 2, vac_queries
        assert "Mark Table Structure Changed" in vac_queries[0][
            0], vac_queries[0][0]
        assert "integrity check before vacuum execution" in vac_queries[1][
            0], vac_queries[1][0]
        # Verify that structure change is restored
        cursor.execute("select btrim(event), structure_change_sb "
                       "from stl_burst_write_query_event "
                       "where event ilike '%structure%' and xid = {} and "
                       "eventtime >= '{}' order by eventtime;".format(
                           vacuum_xid, start_str))
        bw_events = cursor.fetchall()
        assert len(bw_events) == 2
        assert "Marking as StructureChanged" in bw_events[0][0], bw_events[0][
            0]
        if vector.commit_type == 'abort':
            event = "Aborting Read Only StructureChanged"
        else:
            event = "Committing Read Only StructureChanged"
        assert event in bw_events[1][0], bw_events[1][0]
        assert bw_events[0][1] == -1, bw_events[0][1]
        assert bw_events[1][1] == 0, bw_events[0][1]
        # Verify that write query can burst.
        self._start_and_wait_for_refresh(cluster)
        cursor.execute("set query_group to burst;")
        cursor.execute("set session authorization master;")
        cursor.execute("insert into {} values (2)".format(TEST_TABLE))
        self.check_last_query_bursted(cluster, cursor)

    def test_burst_write_after_no_op_bg_vacuum(self, cluster, db_session,
                                               vector):
        """
        Test that after a no op vacuum, structure change states made by that
        vacuum is closed and restored, and also the write query is qualified
        to burst.

        Args:
            cluster: cluster object.
            db_session: Connection to a db on the cluster.
            vector: the dimensions for whether it's testing a no op vacuum
                    commit, or no op vacuum abort.
        """
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to noburst;")
            self._setup_table(cursor, TEST_TABLE)
            # Get start time
            start_time = datetime.datetime.now().replace(microsecond=0)
            start_str = start_time.isoformat(' ')
            if vector.commit_type == 'abort':
                with abort_vacuum_runner_after_pre_vacuum_run(cluster):
                    xid = self._trigger_bg_vacuum(cluster, cursor, start_str,
                                                  vector)
                    self._verify_structure_change_closed_and_query_can_burst(
                        cluster, cursor, xid, start_str, vector)
            else:
                xid = self._trigger_bg_vacuum(cluster, cursor, start_str,
                                              vector)
                self._verify_structure_change_closed_and_query_can_burst(
                    cluster, cursor, xid, start_str, vector)

    def test_burst_refresh_after_no_op_bg_vacuum(self, cluster, db_session):
        """
        Test that no-op vacuumed table after normal structured changed vacuum and
        burst refresh is not refreshed again.
        1. Create a table and load large dataset such that we have atleast
           two data-blocks for some column of the table.
        2. Run manual full delete vacuum on owned burst table.
        3. Trigger a refresh and confirm that owned table was included in CTR list.
        4. Burst delete query on the table.
        5. Trigger a background no-op vacuum.
        6. Begin a transaction and run update query on burst which modifies all row
           of column that has atleast two data blocks.
        7. Trigger a refresh in-between transaction and check table was not in CTR list
        8. Trigger a burst write query and commit transaction.

        Args:
            cluster: cluster object.
            db_session: Connection to a db on the cluster.
        """
        conn_params = cluster.get_conn_params(user='master')
        db_session = DbSession(conn_params)
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        schema = db_session.session_ctx.schema
        with db_session.cursor() as dml_cursor, \
                self.db.cursor() as boot_cursor, \
                burst_session.cursor() as burst_cursor:
            # disable bg no-op vacuum until manual vacuum is triggered
            cluster.run_xpx("auto_worker disable vacuum")
            dml_cursor.execute("drop table if exists {};".format(TEST_TABLE))
            dml_cursor.execute(CREATE_TABLE.format(TEST_TABLE))
            dml_cursor.execute(COPY_QUERY.format(TEST_TABLE))
            dml_cursor.execute("select col_5 from {} limit 3;".format(TEST_TABLE))
            unique_col_5 = dml_cursor.fetchall()
            # load data so that the table is large enough to have atleast two
            # data blocks for a column
            for i in range(6):
                dml_cursor.execute(
                    "insert into {} (select * from {} "
                    "where col_5 not in ('{}','{}','{}'))"
                    .format(TEST_TABLE, TEST_TABLE, unique_col_5[0][0],
                            unique_col_5[1][0], unique_col_5[2][0]))
            self._start_and_wait_for_refresh(cluster)
            dml_cursor.execute("set query_group to burst;")
            dml_cursor.execute("delete from {} where col_5 ilike '{}'"
                               .format(TEST_TABLE, unique_col_5[0][0]))
            self._check_last_query_bursted(cluster, dml_cursor)
            dml_cursor.execute(VACUUM.format(TEST_TABLE))
            # get start time for refresh
            start_time = datetime.datetime.now().replace(microsecond=0)
            self._start_and_wait_for_refresh(cluster)
            # confirm that vacuumed table was included in the changed table list
            table_id = self._get_table_id(boot_cursor, schema, TEST_TABLE)
            burst_cursor.execute(CTR_TABLE_LIST.format(start_time, table_id))
            ctr_tables = burst_cursor.fetchall()
            assert len(ctr_tables) == 1
            # burst delete query
            dml_cursor.execute("begin;")
            dml_cursor.execute("delete from {} where col_5 ilike '{}'"
                               .format(TEST_TABLE, unique_col_5[1][0]))
            self._check_last_query_bursted(cluster, dml_cursor)
            dml_cursor.execute("commit;")
            # get start time for bg no-op vacuum
            start_time = datetime.datetime.now().replace(microsecond=0)
            # run bg no-op vaccum
            cluster.run_xpx("auto_worker enable vacuum")
            xids = self.wait_for_vacuum_exec(boot_cursor, start_time, TEST_TABLE)
            # confirm that bg vacuum was triggered
            assert len(xids) == 2, xids
            assert xids[0][0] == xids[1][0]
            vacuum_xid = xids[0][0]
            # Verify that structure change is restored
            boot_cursor.execute("select btrim(event), structure_change_sb "
                                "from stl_burst_write_query_event "
                                "where event ilike '%structure%' and xid = {} and "
                                "eventtime >= '{}' order by eventtime;"
                                .format(vacuum_xid, start_time))
            bw_events = boot_cursor.fetchall()
            assert len(bw_events) == 2
            assert "Marking as StructureChanged" in bw_events[0][0], bw_events[0][
                0]
            event = "Committing Read Only StructureChanged"
            assert event in bw_events[1][0], bw_events[1][0]
            # begin transaction run update query that modifies all rows
            dml_cursor.execute("begin;")
            # update all rows for col_5 as this column has data in atleast
            # two data blocks, manual repro showed data in 11 datablocks
            dml_cursor.execute("update {} set col_5 = '{}'"
                               .format(TEST_TABLE, unique_col_5[2][0]))
            self._check_last_query_bursted(cluster, dml_cursor)
            # trigger refresh in middle of trasaction
            start_time = datetime.datetime.now().replace(microsecond=0)
            # we shouldn't commit refresh transaction
            self._start_and_wait_for_refresh(cluster, no_commit=True)
            # confirm that CTR list doesn't contain no-op vacuum table
            burst_cursor.execute(CTR_TABLE_LIST.format(start_time, table_id))
            ctr_tables = burst_cursor.fetchall()
            assert len(ctr_tables) == 0
            # if CTR list contains table in refresh list then next BW query
            # will hit block header mismatch xcheck.
            dml_cursor.execute("insert into {} (select * from {} limit 2)"
                               .format(TEST_TABLE, TEST_TABLE))
            dml_cursor.execute("commit;")
            dml_cursor.execute("drop table if exists {};".format(TEST_TABLE))
