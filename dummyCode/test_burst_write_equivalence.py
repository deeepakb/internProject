# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode,\
                                                         get_burst_conn_params
from raff.common.db.redshift_db import RedshiftDb
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.common.dimensions import Dimensions
from raff.util.utils import run_bootstrap_sql
from raff.burst.burst_test import get_burst_cluster_name
from raff.common.burst_helper import (get_cluster_by_identifier,
                                      get_cluster_by_arn, identifier_from_arn)
from raff.common.host_type import HostType

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

INSERT_SELECT_CMD = "INSERT INTO {} SELECT * FROM {}"
QUERY_CMD = "SELECT * FROM {}"
CREATE_STMT = "CREATE TABLE {} (c0 int, c1 int) {} {}"
DROP_STMT = "DROP TABLE IF EXISTS {}"
TBL_CHECK = ("select rows, sorted_rows, insert_pristine, "
             "delete_pristine from stv_tbl_perm where id =  "
             "'{}'::regclass::oid and slice < 12811 order by 1,2,3,4")
INSERT_CMD = "INSERT INTO {} values (1,2), (3, 4), (5, 6), (7, 8), (9, 10)"
BLOCK_CHECK = ("select col, count(*) "
               "from stv_blocklist, stv_tbl_perm "
               "where stv_blocklist.tbl = stv_tbl_perm.id "
               "and stv_blocklist.slice = stv_tbl_perm.slice "
               " and stv_tbl_perm.name = '{}' group by col order by col")

BLOCKS_COL_1 = ("select slice, col, blocknum, unsorted "
                " from stv_blocklist where tombstone=0 "
                "and tbl='{}'::regclass::oid order by 1,2,3")

# skip check equivalence of min/max value for deletexid and
# insertxid since for the testing of the test, we run non-bursted
# first and then identical bursted. The xid will not be the same.
BLOCKS_COL_2 = (
    "select slice, col, blocknum, "
    "minvalue, maxvalue from stv_blocklist where tombstone=0 and col in (0, 1, 4) "
    "and tbl='{}'::regclass::oid order by 1,2,3")

DELETE_CMD = "delete from {} where c0 < 3"
UPDATE_CMD_1 = "update {} set c0 = c0*2 where c0 > 5"
UPDATE_CMD_2 = "update {} set c0 = c0 + 2"


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.custom_local_gucs(
    gucs={'enable_workstealing_for_compress': 'false'}
)
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '3',
        'burst_enable_write': 'true',
        'burst_enable_write_copy': 'true',
        'burst_enable_write_insert': 'true',
        'burst_enable_write_delete': 'true',
        'burst_enable_write_update': 'true',
        'vacuum_auto_worker_enable': 'false',
        'enable_workstealing_for_compress': 'false'
    })
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteBlockEquivalenceBase(BurstWriteTest):
    def _setup_tables(self, db_session, base_tbl_name, vector):
        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")

        with db_session.cursor() as cursor:
            cursor.execute(
                CREATE_STMT.format(base_tbl_name + "_not_burst",
                                   vector.diststyle, vector.sortkey))
            cursor.execute(
                CREATE_STMT.format(base_tbl_name + "_burst", vector.diststyle,
                                   vector.sortkey))
            if vector.size == 'large':
                cursor.execute(INSERT_CMD.format(base_tbl_name + "_not_burst"))
                cursor.execute(INSERT_CMD.format(base_tbl_name + "_burst"))
                for i in range(10):
                    cursor.execute(
                        INSERT_SELECT_CMD.format(base_tbl_name + "_not_burst",
                                                 base_tbl_name + "_not_burst"))
                    cursor.execute(
                        INSERT_SELECT_CMD.format(base_tbl_name + "_burst",
                                                 base_tbl_name + "_burst"))

    def _drop_tables(self, db_session, base_tbl_name):
        with db_session.cursor() as cursor:
            cursor.execute(DROP_STMT.format(base_tbl_name + "_not_burst"))
            cursor.execute(DROP_STMT.format(base_tbl_name + "_burst"))

    def _insert_tables(self, cluster, cursor, base_tbl_name):
        # Insert when set not bursted.
        cursor.execute("set query_group to metrics;")
        cursor.execute(INSERT_CMD.format(base_tbl_name + "_not_burst"))
        self._check_last_query_didnt_burst(cluster, cursor)
        # Insert when set bursted.
        cursor.execute("set query_group to burst;")
        cursor.execute(INSERT_CMD.format(base_tbl_name + "_burst"))
        self._check_last_query_bursted(cluster, cursor)

    def _delete_tables(self, cluster, cursor, base_tbl_name):
        # Delete when set not bursted.
        cursor.execute("set query_group to metrics;")
        cursor.execute(DELETE_CMD.format(base_tbl_name + "_not_burst"))
        self._check_last_query_didnt_burst(cluster, cursor)
        # Delete when set bursted.
        cursor.execute("set query_group to burst;")
        cursor.execute(DELETE_CMD.format(base_tbl_name + "_burst"))
        self._check_last_query_bursted(cluster, cursor)

    def _update_tables_1(self, cluster, cursor, base_tbl_name):
        # Update when set not bursted.
        cursor.execute("set query_group to metrics;")
        cursor.execute(UPDATE_CMD_1.format(base_tbl_name + "_not_burst"))
        self._check_last_query_didnt_burst(cluster, cursor)
        # Update when set bursted.
        cursor.execute("set query_group to burst;")
        cursor.execute(UPDATE_CMD_1.format(base_tbl_name + "_burst"))
        self._check_last_query_bursted(cluster, cursor)

    def _update_tables_2(self, cluster, cursor, base_tbl_name):
        # Update when set not bursted.
        cursor.execute("set query_group to metrics;")
        cursor.execute(UPDATE_CMD_2.format(base_tbl_name + "_not_burst"))
        self._check_last_query_didnt_burst(cluster, cursor)
        # Update when set bursted.
        cursor.execute("set query_group to burst;")
        cursor.execute(UPDATE_CMD_2.format(base_tbl_name + "_burst"))
        self._check_last_query_bursted(cluster, cursor)

    def _copy_tables(self, cluster, cursor, base_tbl_name):
        # Copy when set not bursted.
        cursor.execute("set query_group to metrics;")
        cursor.run_copy(
            base_tbl_name + "_not_burst",
            's3://tpc-h/tpc-ds/1/catalog_returns.',
            gzip=True,
            delimiter="|")
        copy_rows = cursor.last_copy_row_count()
        assert copy_rows == 144067
        self._check_last_query_didnt_burst(cluster, cursor)
        # Copy when set bursted.
        cursor.execute("set query_group to burst;")
        cursor.run_copy(
            base_tbl_name + "_burst",
            's3://tpc-h/tpc-ds/1/catalog_returns.',
            gzip=True,
            delimiter="|")
        copy_rows = cursor.last_copy_row_count()
        assert copy_rows == 144067
        self._check_last_copy_bursted(cluster, cursor)

    def _validate_content_equivalence(self, cursor, base_tbl_name):
        # Check1: check content of case when burst
        # and when not burst are the same.
        log.info("Start checking content.")
        content_check = "select * from {} except select * from {}"
        cursor.execute(
            content_check.format(base_tbl_name + "_not_burst",
                                 base_tbl_name + "_burst"))
        res = cursor.fetchall()
        try:
            assert res == []
        except AssertionError:
            log.info("check result: {}".format(res))
            assert res == []
        cursor.execute(
            content_check.format(base_tbl_name + "_burst",
                                 base_tbl_name + "_not_burst"))
        res = cursor.fetchall()
        try:
            assert res == []
        except AssertionError:
            log.info("check result: {}".format(res))
            assert res == []

    def _validate_metadata_equivalence(self, cursor, base_tbl_name):
        cursor.execute("set query_group to health;")
        # Check 1: sorted/unsorted label for each block are the same.
        log.info("Start to check block metadata, unsorted.")
        cursor.execute(BLOCKS_COL_1.format(base_tbl_name + "_not_burst"))
        blocks_check1_for_col_not_burst = cursor.fetchall()
        cursor.execute(BLOCKS_COL_1.format(base_tbl_name + "_burst"))
        blocks_check1_for_col_burst = cursor.fetchall()
        try:
            assert blocks_check1_for_col_not_burst == blocks_check1_for_col_burst
        except AssertionError:
            for i in range(len(blocks_check1_for_col_not_burst)):
                if blocks_check1_for_col_not_burst[i] != blocks_check1_for_col_burst[i]:
                    log.info("blockchain check unsorted, not burst case: %s" %
                             str(blocks_check1_for_col_not_burst[i]))
                    log.info("blockchain check unsorted, burst case: %s" % str(
                        blocks_check1_for_col_burst[i]))
            assert blocks_check1_for_col_not_burst == blocks_check1_for_col_burst

        # Check 2: min/max value for each block are the same
        log.info("Start to check block metadata,min/max values.")
        cursor.execute(BLOCKS_COL_2.format(base_tbl_name + "_not_burst"))
        blocks_check2_for_col_not_burst = cursor.fetchall()
        cursor.execute(BLOCKS_COL_2.format(base_tbl_name + "_burst"))
        blocks_check2_for_col_burst = cursor.fetchall()
        try:
            assert blocks_check2_for_col_not_burst == blocks_check2_for_col_burst
        except AssertionError:
            log.info("blockchain check min/max values, not burst case: %s" %
                     blocks_check2_for_col_not_burst)
            log.info("blockchain check min/max values, burst case: %s" %
                     blocks_check2_for_col_burst)
            assert blocks_check2_for_col_not_burst == blocks_check2_for_col_burst

        # Check 3: num of blocks used by each column are the same.
        cursor.execute(BLOCK_CHECK.format(base_tbl_name + "_not_burst"))
        block_not_burst = cursor.fetchall()
        cursor.execute(BLOCK_CHECK.format(base_tbl_name + "_burst"))
        block_burst = cursor.fetchall()
        try:
            assert block_not_burst == block_burst
        except AssertionError:
            log.info("block metadata for not burst case: %s" % block_not_burst)
            log.info("block metadata for burst case: %s" % block_burst)

            assert block_not_burst == block_burst

        # Check 4: pristine state, num of rows, and num of sorted rows
        # are the same.
        log.info("Start checking table metadata.")
        cursor.execute(TBL_CHECK.format(base_tbl_name + "_burst"))
        metadata_burst = cursor.fetchall()
        cursor.execute(TBL_CHECK.format(base_tbl_name + "_not_burst"))
        metadata_not_burst = cursor.fetchall()

        try:
            assert metadata_not_burst == metadata_burst
        except AssertionError:
            log.info(
                "table metadata for not burst case: %s" % metadata_not_burst)
            log.info("table metadata for burst case : %s" % metadata_burst)
            assert metadata_not_burst == metadata_burst


class TestBurstWriteBlockEquivalenceDML(TestBurstWriteBlockEquivalenceBase):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['diststyle even', 'distkey(c0)'],
                sortkey=['', 'sortkey(c0)'],
                size=['small', 'large']))

    def test_burst_insert_equivalence_checking_ss_mode(self, cluster, vector):
        """
        Test: test metadata are equivalent for insert select
              when burst and not burst.
        """
        test_schema = 'test_schema'
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp26453"

        burst_db = RedshiftDb(conn_params=get_burst_conn_params())
        with burst_db.cursor() as burst_cursor:
            CMD = "xpx 'event set EtSimulateStartFromFixedSlice'"
            burst_cursor.execute(CMD)
        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")

        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1:
            dml_cursor = session1.cursor()
            with DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2:
                check_cursor = session2.cursor()
                check_cursor.execute(
                    "set search_path to {}".format(test_schema))
                check_cursor.execute(
                    "xpx 'event set EtSimulateStartFromFixedSlice'")
                self._setup_tables(session1, base_tbl_name, vector)
                self._start_and_wait_for_refresh(cluster)

                log.info("check metadata before experiments")
                self._validate_metadata_equivalence(check_cursor,
                                                    base_tbl_name)
                self._insert_tables(cluster, dml_cursor, base_tbl_name)
                check_cursor.execute(
                    "xpx 'event unset EtSimulateStartFromFixedSlice'")

                log.info("check metadata after experiments")
                self._validate_content_equivalence(dml_cursor, base_tbl_name)

                self._validate_metadata_equivalence(check_cursor,
                                                    base_tbl_name)
                self._validate_table(cluster, test_schema, base_tbl_name + "_burst",
                                     vector.diststyle)
                self._validate_table(cluster, test_schema, base_tbl_name + "_not_burst",
                                     vector.diststyle)

    def test_burst_delete_equivalence_checking_ss_mode(self, cluster, vector):
        """
        Test: test metadata are equivalent for delete
              when burst and not burst.
        """
        test_schema = 'test_schema'
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp26453"

        burst_db = RedshiftDb(conn_params=get_burst_conn_params())
        with burst_db.cursor() as burst_cursor:
            CMD = "xpx 'event set EtSimulateStartFromFixedSlice'"
            burst_cursor.execute(CMD)
        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")

        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1:
            dml_cursor = session1.cursor()
            with DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2:
                check_cursor = session2.cursor()
                check_cursor.execute(
                    "set search_path to {}".format(test_schema))
                check_cursor.execute(
                    "xpx 'event set EtSimulateStartFromFixedSlice'")
                self._setup_tables(session1, base_tbl_name, vector)
                self._start_and_wait_for_refresh(cluster)

                log.info("check metadata before experiments")
                self._validate_metadata_equivalence(check_cursor,
                                                    base_tbl_name)
                self._delete_tables(cluster, dml_cursor, base_tbl_name)
                check_cursor.execute(
                    "xpx 'event unset EtSimulateStartFromFixedSlice'")

                log.info("check metadata after experiments")
                self._validate_content_equivalence(dml_cursor, base_tbl_name)

                self._validate_metadata_equivalence(check_cursor,
                                                    base_tbl_name)
                self._validate_table(cluster, test_schema, base_tbl_name + "_burst",
                                     vector.diststyle)
                self._validate_table(cluster, test_schema, base_tbl_name + "_not_burst",
                                     vector.diststyle)

    def test_burst_update_1_equivalence_checking_ss_mode(
            self, cluster, vector):
        """
        Test: test metadata are equivalent for update
              when burst and not burst.
              Covers: insert only, delete only slice for
              the distkey case.
        """
        test_schema = 'test_schema'
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp26453"

        burst_db = RedshiftDb(conn_params=get_burst_conn_params())
        with burst_db.cursor() as burst_cursor:
            CMD = "xpx 'event set EtSimulateStartFromFixedSlice'"
            burst_cursor.execute(CMD)

        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")

        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1:
            dml_cursor = session1.cursor()
            with DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2:
                check_cursor = session2.cursor()
                check_cursor.execute(
                    "set search_path to {}".format(test_schema))
                check_cursor.execute(
                    "xpx 'event set EtSimulateStartFromFixedSlice'")
                self._setup_tables(session1, base_tbl_name, vector)
                self._start_and_wait_for_refresh(cluster)

                log.info("check metadata before experiments")
                self._validate_metadata_equivalence(check_cursor,
                                                    base_tbl_name)
                self._update_tables_1(cluster, dml_cursor, base_tbl_name)
                check_cursor.execute(
                    "xpx 'event unset EtSimulateStartFromFixedSlice'")

                log.info("check metadata after experiments")
                self._validate_content_equivalence(dml_cursor, base_tbl_name)

                self._validate_metadata_equivalence(check_cursor,
                                                    base_tbl_name)
                self._validate_table(cluster, test_schema, base_tbl_name + "_burst",
                                     vector.diststyle)
                self._validate_table(cluster, test_schema, base_tbl_name + "_not_burst",
                                     vector.diststyle)

    def test_burst_update_2_equivalence_checking_ss_mode(
            self, cluster, vector):
        """
        Test: test metadata are equivalent for update
              when burst and not burst.
              Covers: insert + delete slices for the distkey case.
        """
        test_schema = 'test_schema'
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp26453"

        burst_db = RedshiftDb(conn_params=get_burst_conn_params())
        with burst_db.cursor() as burst_cursor:
            CMD = "xpx 'event set EtSimulateStartFromFixedSlice'"
            burst_cursor.execute(CMD)

        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")

        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1:
            dml_cursor = session1.cursor()
            with DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2:
                check_cursor = session2.cursor()
                check_cursor.execute(
                    "set search_path to {}".format(test_schema))
                check_cursor.execute(
                    "xpx 'event set EtSimulateStartFromFixedSlice'")
                self._setup_tables(session1, base_tbl_name, vector)
                self._start_and_wait_for_refresh(cluster)

                log.info("check metadata before experiments")
                self._validate_metadata_equivalence(check_cursor,
                                                    base_tbl_name)
                self._update_tables_2(cluster, dml_cursor, base_tbl_name)
                check_cursor.execute(
                    "xpx 'event unset EtSimulateStartFromFixedSlice'")

                log.info("check metadata after experiments")
                self._validate_content_equivalence(dml_cursor, base_tbl_name)

                self._validate_metadata_equivalence(check_cursor,
                                                    base_tbl_name)
                self._validate_table(cluster, test_schema, base_tbl_name + "_burst",
                                     vector.diststyle)
                self._validate_table(cluster, test_schema, base_tbl_name + "_not_burst",
                                     vector.diststyle)


class TestBurstWriteBlockEquivalenceCOPY(TestBurstWriteBlockEquivalenceBase):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['even', 'key distkey cr_returned_date_sk'],
                sortkey=['', 'cr_item_sk']))

    def test_burst_copy_equivalence_checking_ss_mode(self, cluster, vector):
        """
        Test: test metadata are equivalent for copy
              when burst and not burst.
        """
        test_schema = 'test_schema'
        base_tbl_name = 'catalog_returns'
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        burst_db = RedshiftDb(conn_params=get_burst_conn_params())
        with burst_db.cursor() as burst_cursor:
            CMD = "xpx 'event set EtSimulateStartFromFixedSlice'"
            burst_cursor.execute(CMD)
        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")
        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1:
            dml_cursor = session1.cursor()
            with DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2:
                check_cursor = session2.cursor()
                check_cursor.execute(
                    "set search_path to {}".format(test_schema))
                check_cursor.execute(
                    "xpx 'event set EtSimulateStartFromFixedSlice'")
                self._setup_table(session1, test_schema, base_tbl_name,
                                  'tpcds', '1', vector.diststyle,
                                  vector.sortkey, '_burst')
                self._setup_table(session1, test_schema, base_tbl_name,
                                  'tpcds', '1', vector.diststyle,
                                  vector.sortkey, '_not_burst')
                self._start_and_wait_for_refresh(cluster)

                log.info("check metadata before experiments")
                self._validate_metadata_equivalence(check_cursor,
                                                    base_tbl_name)
                self._copy_tables(cluster, dml_cursor, base_tbl_name)
                check_cursor.execute(
                    "xpx 'event unset EtSimulateStartFromFixedSlice'")

                log.info("check metadata after experiments")
                self._validate_content_equivalence(dml_cursor, base_tbl_name)

                self._validate_metadata_equivalence(check_cursor,
                                                    base_tbl_name)
                self._validate_table(cluster, test_schema, base_tbl_name + "_burst",
                                     vector.diststyle)
                self._validate_table(cluster, test_schema, base_tbl_name + "_not_burst",
                                     vector.diststyle)
