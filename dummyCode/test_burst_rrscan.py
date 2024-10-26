# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import datetime
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode,\
                                                         get_burst_conn_params, \
                                                         read_file_in_container
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
from io import open

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

INSERT_SELECT_CMD = "INSERT INTO {} SELECT * FROM {}"
CREATE_STMT = "CREATE TABLE {} (c0 int, c1 int) {} {}"
INSERT_CMD = "INSERT INTO {} values (1,2),(3,4),(5,6),(7,8),(9,10),(11,12),(13,14),(15,16),(17,18),(19,20);"
DELETE_CMD = "delete from {} where c0 < 3"
UPDATE_CMD_1 = "update {} set c0 = c0 + 2"
UPDATE_CMD_2 = "update {} set c0 = c0*2 where c0 > 5"
SELECT_CMD = "select * from {} where c0 > 3"

S3_PATH = 's3://cookie-monster-s3-ingestion/schema-quota/2-cols-21-rows.csv'
COPY_STMT = ("COPY {} "
             "FROM "
             "'{}' "
             "DELIMITER ',' "
             "CREDENTIALS "
             "'aws_iam_role=arn:aws:iam::467896856988:role/Redshift-S3';")

STL_SCAN_METRICS = """ SELECT is_rrscan,
       segment,
       step,
       is_delayed_scan,
       is_rlf_scan,
       is_rlf_scan_reason,
       runtime_filtering,
       scan_region,
       num_sortkey_as_predicate,
       row_fetcher_state,
       is_vectorized_scan,
       is_vectorized_scan_reason,
       row_fetcher_reason,
       max(work_stealing_reason) as work_stealing_reason,
       Sum(fetches)              AS fetches,
       Sum(rows_pre_filter)      AS rows_pre_filter,
       Sum(rows_pre_user_filter) AS rows_pre_user_filter
FROM   stl_scan
WHERE  query = {}
and perm_table_name <> 'Internal Worktable'
GROUP  BY 1,
          2,
          3,
          4,
          5,
          6,
          7,
          8,
          9,
          10,
          11,
          12,
          13
order by segment, step
"""

MAPPING = """ select concurrency_scaling_query
from stl_concurrency_scaling_query_mapping
where primary_query = {}
"""

STL_COMPILE_METRICS = """ SELECT query, segment, btrim(path) as path
from stl_compile_info where query = {}
order by segment
"""

STL_EVENT_TRACE = """ select btrim(message) from stl_event_trace where
pid = (select pid from stl_query where query = {})
and eventtime >= (
    -- This filter needs to be added to avoid codegen repetition
    select max(eventtime) from stl_event_trace where
    pid = (select pid from stl_query where query = {})
    and message like 'Visibility checks%'
)
and eventtime >= '{}'
and event_name = '{}'
order by eventtime
"""

GET_TABLE_OID = "select oid from pg_class where relname = '{}' "

STV_BLOCKLIST = """select * from stv_blocklist where tbl = {}
order by tbl, col, slice, id"""


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_local_gucs(
    gucs={'slices_per_node': '2',
          'vacuum_auto_worker_enable': 'false',
          'enable_workstealing_for_compress': 'false',
          # CE INSERT is disabled for burst clusters.
          # This add a new segment and cause the test
          # to fail.
          'enable_ce_insert_mem_opt': 'false',
          'redshift_row_fetcher_enable_workstealing': 'false',
          'comm_aligned_payload_size': '8880',
          'enable_packet_encryption': 'false',
          'max_num_rows_per_precompiled_executor_batch': '102400'
    })
@pytest.mark.custom_burst_gucs(
    gucs={'burst_enable_write': 'true',
          'slices_per_node': '2',
          'vacuum_auto_worker_enable': 'false',
          'enable_workstealing_for_compress': 'false',
          'burst_redshift_row_fetcher_enable_workstealing': 'false',
          'redshift_row_fetcher_enable_workstealing': 'false',
          'comm_aligned_payload_size': '8880',
          'enable_packet_encryption': 'false',
          'max_num_rows_per_precompiled_executor_batch': '102400'
    })
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstRRScan(BurstWriteTest):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['diststyle even', 'distkey(c0)'],
                sortkey=['', 'sortkey(c0)'],
                size=['pristine', 'small', 'large']))

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
            cursor.execute("begin;")
            if vector.size == 'small':
                cursor.execute(INSERT_CMD.format(base_tbl_name + "_not_burst"))
                cursor.execute(INSERT_CMD.format(base_tbl_name + "_burst"))
            elif vector.size == 'large':
                cursor.execute(INSERT_CMD.format(base_tbl_name + "_not_burst"))
                cursor.execute(INSERT_CMD.format(base_tbl_name + "_burst"))
                for i in range(17):
                    cursor.execute(
                        INSERT_SELECT_CMD.format(base_tbl_name + "_not_burst",
                                                 base_tbl_name + "_not_burst"))
                    cursor.execute(
                        INSERT_SELECT_CMD.format(base_tbl_name + "_burst",
                                                 base_tbl_name + "_burst"))
            cursor.execute("commit;")

    def _verify_rrscan_node_equivalence(self, cursor, starttime, base_tbl_name,
                                        burst_qid):
        '''
        Compares the rrscan Node generated on main, with the one generated on burst
        for the same query
        '''
        with self.db.cursor() as cursor:
            cursor.execute(
                STL_EVENT_TRACE.format(burst_qid, burst_qid, starttime,
                                       "EtStepScanPrintRRScanNode"))
            main_stats = cursor.fetchall()
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        with burst_session.cursor() as burst_cursor:
            burst_cursor.execute(MAPPING.format(burst_qid))
            burst_qid = burst_cursor.fetch_scalar()
            burst_cursor.execute("set volt_decorr_csq to false;")
            burst_cursor.execute(
                STL_EVENT_TRACE.format(burst_qid, burst_qid, starttime,
                                       "EtStepScanPrintRRScanNode"))
            burst_stats = burst_cursor.fetchall()
            if not main_stats == burst_stats:
                self._print_blocks_info(cursor, base_tbl_name)
                log.info("Main stats: {}".format(main_stats))
                log.info("Burst stats: {}".format(burst_stats))
                assert False, (
                    "Difference in rrscan stats. See RAFF log output for details"
                )

    def _verify_code_equivalence(self, cursor, main_qid, burst_qid):
        '''
        Verifies that the generated code of the query running on main
        is the same as the query running on burst
        '''
        with self.db.cursor() as cursor:
            cursor.execute(STL_COMPILE_METRICS.format(main_qid))
            main_stats = cursor.fetchall()
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        with burst_session.cursor() as burst_cursor:
            burst_cursor.execute("set volt_decorr_csq to false;")
            burst_cursor.execute(MAPPING.format(burst_qid))
            burst_qid = burst_cursor.fetch_scalar()
            burst_cursor.execute(STL_COMPILE_METRICS.format(burst_qid))
            burst_stats = burst_cursor.fetchall()
            assert len(main_stats) == len(
                burst_stats
            ), "Main has {} segments, while burst has {} segments: {}, {}".format(
                len(main_stats), len(burst_stats), main_stats, burst_stats)
            for i in range(len(main_stats)):
                main_path = main_stats[i][2] + ".cpp"
                burst_path = burst_stats[i][2] + ".cpp"
                if main_path == burst_path:
                    # Code is the same, verification is succesfull
                    return
                # If files are different print the content and skip the test
                with open(main_path) as mf:
                    mlines = mf.read()
                log.info("Main generated code {}: {}".format(
                    main_path, mlines))
                blines = read_file_in_container(burst_path)
                log.info("Burst generated code {}: {}".format(
                    burst_path, blines))
                # Generated code can be different for multiple reasons and
                # might lead to rrscan difference.
                pytest.skip("Difference in generated code")

    def _verify_scan_equivalence(self, cursor, main_qid, base_tbl_name,
                                 burst_qid):
        '''
        Verifies that the rows scanned during the query running on main
        are the same as the query running on burst
        '''
        cursor.execute(STL_SCAN_METRICS.format(main_qid))
        main_stats = cursor.fetchall()
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        with burst_session.cursor() as burst_cursor:
            burst_cursor.execute("set volt_decorr_csq to false;")
            burst_cursor.execute(MAPPING.format(burst_qid))
            burst_qid = burst_cursor.fetch_scalar()
            burst_cursor.execute(STL_SCAN_METRICS.format(burst_qid))
            burst_stats = burst_cursor.fetchall()
            if not main_stats == burst_stats:
                self._print_blocks_info(cursor, base_tbl_name)
                log.info("Scans on main: {}".format(main_stats))
                log.info("Scans on burst: {}".format(burst_stats))
                assert False, (
                    "Difference in stl_scan stats. See RAFF log output for details"
                )

    def _print_blocks_info(self, cursor, base_tbl_name):
        '''
        Prints stv blocklist info to help debugging in case of failure
        '''
        cursor.execute(GET_TABLE_OID.format(base_tbl_name + "_not_burst"))
        main_table_oid = cursor.fetch_scalar()

        cursor.execute(GET_TABLE_OID.format(base_tbl_name + "_burst"))
        burst_table_oid = cursor.fetch_scalar()

        with self.db.cursor() as cursor:
            cursor.execute(STV_BLOCKLIST.format(main_table_oid))
            main_blocks = cursor.fetchall()
            log.info("Blocks on main: {}".format(main_blocks))

        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        with burst_session.cursor() as burst_cursor:
            burst_cursor.execute("set volt_decorr_csq to false;")
            burst_cursor.execute(STV_BLOCKLIST.format(burst_table_oid))
            burst_blocks = burst_cursor.fetchall()
            log.info("Blocks on burst: {}".format(burst_blocks))

    def _run_query(self, cluster, cursor, base_tbl_name, cmd):
        # Run the query on main.
        cursor.execute("set query_group to metrics")
        cursor.execute(cmd.format(base_tbl_name + "_not_burst"))
        main_qid = self.last_query_id(cursor)
        self.verify_query_didnt_bursted(cluster, main_qid)

        # Run the query on burst.
        starttime = datetime.datetime.now().replace(microsecond=0)
        start_str = starttime.isoformat(' ')
        cursor.execute("set query_group to burst")
        cursor.execute(cmd.format(base_tbl_name + "_burst"))
        burst_qid = self.last_query_id(cursor)
        self.verify_query_bursted(cluster, burst_qid)

        self._verify_code_equivalence(cursor, main_qid, burst_qid)
        self._verify_rrscan_node_equivalence(cursor, start_str, base_tbl_name,
                                             burst_qid)
        self._verify_scan_equivalence(cursor, main_qid, base_tbl_name,
                                      burst_qid)

    def _insert_tables(self, cluster, cursor, base_tbl_name):
        self._run_query(cluster, cursor, base_tbl_name, INSERT_CMD)

    def _select_tables(self, cluster, cursor, base_tbl_name):
        self._run_query(cluster, cursor, base_tbl_name, SELECT_CMD)

    def _delete_tables(self, cluster, cursor, base_tbl_name):
        self._run_query(cluster, cursor, base_tbl_name, DELETE_CMD)

    def _update_tables_1(self, cluster, cursor, base_tbl_name):
        self._run_query(cluster, cursor, base_tbl_name, UPDATE_CMD_1)

    def _update_tables_2(self, cluster, cursor, base_tbl_name):
        self._run_query(cluster, cursor, base_tbl_name, UPDATE_CMD_2)

    def _copy_tables(self, cluster, cursor, base_tbl_name):
        # Run the query on main.
        cursor.execute("set query_group to metrics;")
        cursor.execute(COPY_STMT.format(base_tbl_name + "_not_burst", S3_PATH))
        main_qid = self.last_query_id(cursor)
        self.verify_query_didnt_bursted(cluster, main_qid)
        copy_rows = cursor.last_copy_row_count()
        assert copy_rows == 21

        # Run the query on burst.
        starttime = datetime.datetime.now().replace(microsecond=0)
        start_str = starttime.isoformat(' ')
        cursor.execute("set query_group to burst;")
        cursor.execute(COPY_STMT.format(base_tbl_name + "_burst", S3_PATH))
        burst_qid = self.last_query_id(cursor)
        self.verify_query_bursted(cluster, burst_qid)
        copy_rows = cursor.last_copy_row_count()
        assert copy_rows == 21

        self._verify_code_equivalence(cursor, main_qid, burst_qid)
        self._verify_scan_equivalence(cursor, main_qid, base_tbl_name,
                                      burst_qid)

    def test_burst_rrscan(self, cluster, vector):
        """
        Test: test mixed insert, delete, update and copy on
        the same table without txn.
        """
        test_schema = 'test_schema'
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "burst3465"

        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")
            cursor.execute("xpx 'event set EtSimulateStartFromFixedSlice'")

        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1, \
            DbSession(
                cluster.get_conn_params(),
                session_ctx=session_ctx2) as session2, \
            cluster.event("EtStepScanPrintRRScanNode"), \
            cluster.event("EtSimulateStartFromFixedSlice"):

            burst_session = RedshiftDb(conn_params=get_burst_conn_params())
            with burst_session.cursor() as burst_cursor:
                burst_cursor.execute(
                    "xpx 'event set EtStepScanPrintRRScanNode'")
                burst_cursor.execute(
                    "xpx 'event set EtSimulateStartFromFixedSlice'")

            dml_cursor = session1.cursor()
            check_cursor = session2.cursor()
            check_cursor.execute("set search_path to {}".format(test_schema))
            self._setup_tables(session1, base_tbl_name, vector)
            self._start_and_wait_for_refresh(cluster)

            # First round: iduc
            log.info("first round")
            self._insert_tables(cluster, dml_cursor, base_tbl_name)
            self._delete_tables(cluster, dml_cursor, base_tbl_name)
            self._update_tables_1(cluster, dml_cursor, base_tbl_name)
            self._copy_tables(cluster, dml_cursor, base_tbl_name)
            self._select_tables(cluster, dml_cursor, base_tbl_name)

            # Second round: change the sequence, iduc
            log.info("second round")
            self._insert_tables(cluster, dml_cursor, base_tbl_name)
            self._update_tables_2(cluster, dml_cursor, base_tbl_name)
            self._delete_tables(cluster, dml_cursor, base_tbl_name)
            self._copy_tables(cluster, dml_cursor, base_tbl_name)
            self._select_tables(cluster, dml_cursor, base_tbl_name)

            # Third round: change the sequence, duic
            log.info("third round")
            self._delete_tables(cluster, dml_cursor, base_tbl_name)
            self._update_tables_1(cluster, dml_cursor, base_tbl_name)
            self._insert_tables(cluster, dml_cursor, base_tbl_name)
            self._copy_tables(cluster, dml_cursor, base_tbl_name)
            self._select_tables(cluster, dml_cursor, base_tbl_name)

            # Fourth round: change the sequence, ucid
            log.info('fourth round')
            self._update_tables_2(cluster, dml_cursor, base_tbl_name)
            self._copy_tables(cluster, dml_cursor, base_tbl_name)
            self._insert_tables(cluster, dml_cursor, base_tbl_name)
            self._delete_tables(cluster, dml_cursor, base_tbl_name)
            self._select_tables(cluster, dml_cursor, base_tbl_name)
