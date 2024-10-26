# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import datetime
import pytest
import uuid
from contextlib import contextmanager

from raff.burst.burst_super_simulated_mode_helper import (
    super_simulated_mode_method,
    get_burst_conn_params
    )
from raff.common.dimensions import Dimensions
from raff.common.db.redshift_db import RedshiftDb
from raff.storage.storage_test import disable_all_autoworkers
from test_burst_write_mixed_workload import TestBurstWriteMixedWorkloadBase,\
    DELETE_CMD, COPY_STMT, S3_PATH


log = logging.getLogger(__name__)
__all__ = [super_simulated_mode_method, disable_all_autoworkers]


CREATE_STMT = "CREATE TABLE {} (c0 int, c1 int) {} {}"
CTAS_CMD = "CREATE TABLE ctas_{} AS SELECT * FROM {}"
SELECT_INTO_CMD = "SELECT * INTO select_into_{} FROM {}"
INSERT_CMD = "INSERT INTO {} values (1,2), (3, 4), (5, 6), (7, 8), (9, 10)"
INSERT_SELECT_CMD = "INSERT INTO {} SELECT * FROM {};"
UPDATE_CMD = "UPDATE {} set c0 = c0 + 1, c1 = c1 + 1;"

LARGE = "large"
SMALL = "small"

burst_write_no_retry_gucs = {
    'burst_enable_write_user_ctas': 'true',
    'enable_burst_failure_handling': 'false',
    'burst_enable_insert_failure_handling': 'false',
    'burst_enable_delete_failure_handling': 'false',
    'burst_enable_update_failure_handling': 'false',
    'burst_enable_copy_failure_handling': 'false'
}

burst_write_retry_gucs = {
    'burst_enable_write_user_ctas': 'true',
    'enable_burst_failure_handling': 'true',
    'burst_enable_ctas_failure_handling': 'true',
    'burst_enable_insert_failure_handling': 'true',
    'burst_enable_delete_failure_handling': 'true',
    'burst_enable_update_failure_handling': 'true',
    'burst_enable_copy_failure_handling': 'true'
}


class BaseTestBurstClusterError(TestBurstWriteMixedWorkloadBase):
    def _setup_tables(self, db_session, burst_table, vector):
        with db_session.cursor() as cursor:
            cursor.execute(
                CREATE_STMT.format(burst_table, vector.diststyle,
                                   vector.sortkey))
            cursor.execute("begin;")
            cursor.execute(INSERT_CMD.format(burst_table))
            if vector.size == 'large':
                for i in range(10):
                    cursor.execute(
                        INSERT_SELECT_CMD.format(burst_table, burst_table))
            cursor.execute("commit;")

    def _generate_dml_cmd(self, vector, tbl_name):
        if vector.dml == 'insert':
            return INSERT_CMD.format(tbl_name)
        elif vector.dml == 'delete':
            return DELETE_CMD.format(tbl_name)
        elif vector.dml == 'update':
            return UPDATE_CMD.format(tbl_name)
        elif vector.dml == 'ctas':
            return CTAS_CMD.format(tbl_name, tbl_name)
        elif vector.dml == 'select_into':
            return SELECT_INTO_CMD.format(tbl_name, tbl_name)
        else:
            return COPY_STMT.format(tbl_name, S3_PATH)

    def _check_table_content(self, cursor, tbl, expected_result):
        cursor.execute("select count(*), sum(c0), sum(c1) "
                       "from {};".format(tbl))
        result = cursor.fetchall()
        log.info("check result {} expected is {}".format(result, expected_result))
        assert result == expected_result

    def _get_result(self, size, stage):
        if size == 'large':
            return [(5120, 25600, 30720)] if stage == 1 else \
                    [(8217, 82002, 57414)]
        else:
            return [(5, 25, 30)] if stage == 1 else [(33, 162, 126)]

    @contextmanager
    def verify_failed_burst_query_status(self, cluster, dml, status):
        try:
            start_time = datetime.datetime.now().replace(microsecond=0)
            start_str = start_time.isoformat(' ')
            yield
        except Exception as e:
            log.info(e)
        finally:
            query = """
                select query from stl_query
                where userid > 1
                and starttime >= '{}'
                and querytxt ilike '%{}%'
                order by starttime asc
                limit 1
                """.format(start_str, dml)
            with self.db.cursor() as cur:
                cur.execute(query)
                qid = cur.fetch_scalar()
                self.verify_query_status(cluster, qid, status)

    # Full dimensions will run about 19 hours, thus skip some cases.
    def _should_skip(self, vector):
        if ((vector.dml in ['select_into', 'ctas', 'insert', 'copy'])
            and vector.event == 'EtFakeBurstErrorStepDelete') \
            or (vector.dml == 'delete'
                and vector.event == 'EtFakeBurstErrorStepInsert') \
            or ((vector.dml in ['insert', 'update'])
                and vector.event == 'EtFakeBurstErrorBeforeStreamHeader') \
            or ((vector.dml == ['copy', 'delete'])
                and vector.event == 'EtFakeBurstErrorAfterStreamHeader'):
            return True
        else:
            return False

    def _verify_ctas_table_cleanup(self):
        with self.db.cursor() as cur:
            cur.execute("SELECT DISTINCT table_id FROM stl_tbl_perm_at_drop"
                        " order by eventtime desc limit 1;")
            table_id = cur.fetch_scalar()
            cur.execute("SELECT relname FROM"
                        " pg_catalog.pg_class where relfilenode = {};"
                        .format(table_id))
            assert cur.fetchall() == []

    def base_test_burst_cluster_error_in_txn(self, cluster, db_session, vector,
                                             retry):
        burst_table = "burst_cluster_error_burst_" + str(uuid.uuid4().hex[:8])
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        schema = db_session.session_ctx.schema
        with db_session.cursor() as dml_cursor, \
                burst_session.cursor() as burst_cursor:
            log.info("Setting up tables")
            self._setup_tables(db_session, burst_table, vector)

            log.info("Start backup and wait for burst refresh")
            self._start_and_wait_for_refresh(cluster)

            expect_result = self._get_result(vector.size, 1)
            self._check_table_content(dml_cursor, burst_table, expect_result)
            self._validate_table(
                    cluster, schema, burst_table, vector.diststyle)

            dml_cursor.execute("set query_group to burst;")
            log.info("run dml")
            dml_cmd = self._generate_dml_cmd(vector, burst_table)
            dml_cursor.execute("set query_group to burst;")
            dml_cursor.execute("begin;")
            dml_cursor.execute(
                INSERT_SELECT_CMD.format(burst_table, burst_table))
            self._check_last_query_bursted(cluster, dml_cursor)

            log.info("set event on burst cluster")
            burst_cursor.execute("xpx 'event set {}'".format(vector.event))

            if retry:
                status = 27 if 'AfterStreamHeader' in vector.event else 29
                with self.verify_failed_burst_query_status(
                        cluster, vector.dml, status):
                    dml_cursor.execute(dml_cmd)
            else:
                with self.verify_failed_burst_query_status(
                        cluster, vector.dml, 25):
                    dml_cursor.execute_failing_query(dml_cmd,
                                                     "Simulate burst error")
            dml_cursor.execute("abort;")
            burst_cursor.execute("xpx 'event unset {}'".format(vector.event))
            # CTAS created table should be cleaned up, so if the query doesn't
            # rerun on main there is no reason to check table properties or
            # contents.
            if (not retry or 'AfterStreamHeader' in vector.event) \
                    and vector.dml in ['ctas', 'select_into']:
                # check that ctas table is cleaned up if no retry
                # or we can't retry because results already returning
                self._verify_ctas_table_cleanup()
            else:
                self._validate_ownership_state(schema, burst_table, [('Main',
                                                                      'Undo')])

                dml_cursor.execute("set query_group to metrics;")
                self._check_table_content(dml_cursor, burst_table, expect_result)
                self._validate_table(
                        cluster, schema, burst_table, vector.diststyle)

                log.info("Validation: Start backup and wait for burst refresh")
                self._start_and_wait_for_refresh(cluster)
                if vector.validate_query_mode == 'burst':
                    dml_cursor.execute("begin;")
                    self._insert_select_bursted(cluster, dml_cursor, burst_table)
                    self._insert_tables_bursted(cluster, dml_cursor, burst_table)
                    self._delete_tables_bursted(cluster, dml_cursor, burst_table)
                    self._update_tables_2_bursted(cluster, dml_cursor, burst_table)
                    self._copy_tables_bursted(cluster, dml_cursor, burst_table)
                    dml_cursor.execute("commit;")
                else:
                    dml_cursor.execute("begin;")
                    self._insert_select_not_bursted(cluster, dml_cursor,
                                                    burst_table)
                    self._insert_tables_not_bursted(cluster, dml_cursor,
                                                    burst_table)
                    self._delete_tables_not_bursted(cluster, dml_cursor,
                                                    burst_table)
                    self._update_tables_2_not_bursted(cluster, dml_cursor,
                                                      burst_table)
                    self._copy_tables_not_bursted(cluster, dml_cursor, burst_table)
                    dml_cursor.execute("commit;")
                expect_result = self._get_result(vector.size, 2)
                self._check_table_content(dml_cursor, burst_table, expect_result)
                self._validate_table(
                        cluster, schema, burst_table, vector.diststyle)
                log.info("drop table")
                dml_cursor.execute("drop table {};".format(burst_table))

    def base_test_burst_cluster_error_out_txn(self, cluster, db_session,
                                              vector, retry):
        burst_table = "burst_cluster_error_burst" + str(uuid.uuid4().hex[:8])
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        schema = db_session.session_ctx.schema
        with db_session.cursor() as dml_cursor, \
                burst_session.cursor() as burst_cursor:
            log.info("Setting up tables")
            self._setup_tables(db_session, burst_table, vector)

            log.info("Start backup and wait for burst refresh")
            self._start_and_wait_for_refresh(cluster)
            if 'AfterSnapIn' in vector.event:
                log.info("set event on main cluster")
                db_session.cursor().execute("set query_group to 'noburst';")
                with self.db.cursor() as bootstrap_cursor:
                    bootstrap_cursor.execute("xpx 'event set {}'".format(
                        vector.event))
            else:
                log.info("set event on burst cluster")
                burst_cursor.execute("xpx 'event set {}'".format(vector.event))
            expect_result = self._get_result(vector.size, 1)
            self._check_table_content(dml_cursor, burst_table, expect_result)
            self._validate_table(
                    cluster, schema, burst_table, vector.diststyle)

            dml_cursor.execute("set query_group to burst;")
            log.info("run dml")
            dml_cmd = self._generate_dml_cmd(vector, burst_table)
            if retry:
                # For delete, it can retry since it does not reach snap in step.
                status = 27 if ('AfterStreamHeader' in vector.event
                                and vector.dml != 'delete'
                                ) or 'AfterSnapIn' in vector.event else 29
                with self.verify_failed_burst_query_status(
                        cluster, vector.dml, status):
                    dml_cursor.execute(dml_cmd)
            else:
                with self.verify_failed_burst_query_status(
                        cluster, vector.dml, 25):
                    dml_cursor.execute_failing_query(dml_cmd,
                                                     "Simulate burst error")
            if (not retry or 'AfterStreamHeader' in vector.event or
                    'AfterSnapIn' in vector.event) \
                    and vector.dml in ['ctas', 'select_into']:
                # check that ctas table is cleaned up if no retry
                # or we can't retry because results already returning
                self._verify_ctas_table_cleanup()
            if 'AfterSnapIn' in vector.event:
                log.info("set event on main cluster")
                db_session.cursor().execute("set query_group to 'noburst';")
                with self.db.cursor() as bootstrap_cursor:
                    bootstrap_cursor.execute("xpx 'event unset {}'".format(
                        vector.event))
            else:
                log.info("set event on burst cluster")
                burst_cursor.execute("xpx 'event unset {}'".format(
                    vector.event))
            if retry and ('AfterStreamHeader' not in vector.event or vector.dml
                          == 'delete') and 'AfterSnapIn' not in vector.event:
                if vector.dml not in ['select_into', 'ctas']:
                    self._validate_ownership_state(schema, burst_table,
                                                   [('Burst', 'Dirty')])
                else:
                    self._validate_ownership_state(schema, burst_table,
                                                   [])
            elif vector.dml in ['select_into', 'ctas']:
                # Table is cleaned up no reason to check ownership state
                pass
            else:
                self._validate_ownership_state(schema, burst_table,
                                               [('Main', 'Undo')])

            dml_cursor.execute("set query_group to metrics;")
            retry_expect_result = {
                'insert': [(5125, 25625, 30750)],
                'delete': [(4096, 24576, 28672)],
                'update': [(5120, 30720, 35840)],
                'copy': [(5141, 25642, 30762)],
                'select_into': [(5120, 25600, 30720)],
                'ctas': [(5120, 25600, 30720)]
            }
            if retry and ('AfterStreamHeader' not in vector.event or vector.dml
                          == 'delete') and 'AfterSnapIn' not in vector.event:
                expect_result = retry_expect_result[vector.dml]
            self._check_table_content(dml_cursor, burst_table, expect_result)
            self._validate_table(
                    cluster, schema, burst_table, vector.diststyle)
            log.info("Validation: Start backup and wait for burst refresh")
            self._start_and_wait_for_refresh(cluster)
            if vector.validate_query_mode == 'burst':
                self._insert_select_bursted(cluster, dml_cursor, burst_table)
                self._insert_tables_bursted(cluster, dml_cursor, burst_table)
                self._delete_tables_bursted(cluster, dml_cursor, burst_table)
                self._update_tables_2_bursted(cluster, dml_cursor, burst_table)
                self._copy_tables_bursted(cluster, dml_cursor, burst_table)
            else:
                self._insert_select_not_bursted(cluster, dml_cursor,
                                                burst_table)
                self._insert_tables_not_bursted(cluster, dml_cursor,
                                                burst_table)
                self._delete_tables_not_bursted(cluster, dml_cursor,
                                                burst_table)
                self._update_tables_2_not_bursted(cluster, dml_cursor,
                                                  burst_table)
                self._copy_tables_not_bursted(cluster, dml_cursor, burst_table)
            retry_expect_result = {
                'insert': [(8225, 82082, 57470)],
                'delete': [(8217, 82002, 57414)],
                'update': [(8217, 106578, 65606)],
                'copy': [(8231, 82044, 57456)],
                'select_into': [(8217, 82002, 57414)],
                'ctas': [(8217, 82002, 57414)]
            }
            if retry and ('AfterStreamHeader' not in vector.event or vector.dml
                          == 'delete') and 'AfterSnapIn' not in vector.event:
                expect_result = retry_expect_result[vector.dml]
            else:
                expect_result = self._get_result(vector.size, 2)
            self._check_table_content(dml_cursor, burst_table, expect_result)
            self._validate_table(
                    cluster, schema, burst_table, vector.diststyle)
            log.info("drop table")
            dml_cursor.execute("drop table {};".format(burst_table))


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_no_stable_rc
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '3',
        'burst_enable_write': 'true',
        'vacuum_auto_worker_enable': 'false',
        'burst_blk_hdr_stream_threshold': 1,
        'burst_enable_write_user_ctas': 'true'
    })
@pytest.mark.usefixtures("disable_all_autoworkers")
@pytest.mark.custom_local_gucs(gucs=burst_write_no_retry_gucs)
class TestBurstClusterErrorInTxnNoRetry(BaseTestBurstClusterError):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['diststyle even'],
                sortkey=['sortkey(c0)'],
                validate_query_mode=['main', 'burst'],
                size=[SMALL],
                dml=['select_into', 'ctas', 'insert', 'delete', 'update', 'copy'],
                event=[
                    'EtFakeBurstErrorStepDelete', 'EtFakeBurstErrorStepInsert',
                    'EtFakeBurstErrorBeforeStreamHeader',
                    'EtFakeBurstErrorAfterStreamHeader'
                ]))

    @pytest.mark.usefixtures("super_simulated_mode_method")
    def test_burst_cluster_error_in_txn_no_retry(self, cluster, db_session,
                                                 vector):
        """
        Test: Fails write query on different position in burst cluster, ensure
              query failure, then run write on main/burst cluster and check table
              content for further validation.
        """
        if self._should_skip(vector):
            pytest.skip("Unsupported dimension")

        assert cluster.get_guc_value(
            'burst_enable_insert_failure_handling') == 'off'

        self.base_test_burst_cluster_error_in_txn(cluster, db_session, vector,
                                                  False)
