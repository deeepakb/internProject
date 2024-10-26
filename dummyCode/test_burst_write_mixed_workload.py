# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid
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
from raff.burst.burst_write import burst_write_basic_gucs
from raff.burst.burst_test import setup_teardown_burst
from raff.cred_broadcasting.cred_broadcasting import CredBroadcasting

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst]

INSERT_SELECT_CMD = "INSERT INTO {} SELECT * FROM {}"
CREATE_STMT = "CREATE TABLE {} (c0 int, c1 int) {} {}"
INSERT_CMD = "INSERT INTO {} values (1,2), (3, 4), (5, 6), (7, 8), (9, 10)"
DELETE_CMD = "delete from {} where c0 < 3"
UPDATE_CMD_1 = "update {} set c0 = c0 + 2"
UPDATE_CMD_2 = "update {} set c0 = c0*2 where c0 > 5"
READ_CMD = "select count(*) from {}"

S3_PATH = 's3://cookie-monster-s3-ingestion/schema-quota/2-cols-21-rows.csv'
COPY_STMT = ("COPY {} (c0, c1) "
             "FROM "
             "'{}' "
             "DELIMITER ',' "
             "CREDENTIALS "
             "'aws_iam_role=arn:aws:iam::467896856988:role/Redshift-S3';")


class TestBurstWriteMixedWorkloadBase(BurstWriteTest):
    def _setup_tables(self, cluster, db_session, base_tbl_name, vector):
        cluster.run_xpx('auto_worker disable both')

        with db_session.cursor() as cursor:
            cursor.execute(
                CREATE_STMT.format(base_tbl_name, vector.diststyle,
                                   vector.sortkey))
            cursor.execute("begin;")
            if vector.size == 'small':
                cursor.execute(INSERT_CMD.format(base_tbl_name))
            elif vector.size == 'large':
                cursor.execute(INSERT_CMD.format(base_tbl_name))
                for i in range(20):
                    cursor.execute(
                        INSERT_SELECT_CMD.format(base_tbl_name, base_tbl_name))
            cursor.execute("commit;")

    def _insert_tables_not_bursted(self, cluster, cursor, base_tbl_name):
        # Insert when set not bursted.
        cursor.execute("set query_group to metrics;")
        cursor.execute(INSERT_CMD.format(base_tbl_name))
        self._check_last_query_didnt_burst(cluster, cursor)

    def _insert_select_not_bursted(self, cluster, cursor, base_tbl_name):
        # Insert select when set not bursted.
        cursor.execute("set query_group to metrics;")
        cursor.execute(INSERT_SELECT_CMD.format(base_tbl_name, base_tbl_name))
        self._check_last_query_didnt_burst(cluster, cursor)

    def _insert_select_bursted(self, cluster, cursor, base_tbl_name):
        # Insert select when set bursted.
        cursor.execute("set query_group to burst;")
        cursor.execute(INSERT_SELECT_CMD.format(base_tbl_name, base_tbl_name))
        self._check_last_query_bursted(cluster, cursor)

    def _insert_tables_bursted(self, cluster, cursor, base_tbl_name):
        # Insert when set bursted.
        cursor.execute("set query_group to burst;")
        cursor.execute(INSERT_CMD.format(base_tbl_name))
        self._check_last_query_bursted(cluster, cursor)

    def _delete_tables_not_bursted(self, cluster, cursor, base_tbl_name):
        # Delete when set not bursted.
        cursor.execute("set query_group to metrics;")
        cursor.execute(DELETE_CMD.format(base_tbl_name))
        self._check_last_query_didnt_burst(cluster, cursor)

    def _delete_tables_bursted(self, cluster, cursor, base_tbl_name):
        # Delete when set bursted.
        cursor.execute("set query_group to burst;")
        cursor.execute(DELETE_CMD.format(base_tbl_name))
        self._check_last_query_bursted(cluster, cursor)

    def _update_tables_1_not_bursted(self, cluster, cursor, base_tbl_name):
        # Update when set not bursted.
        cursor.execute("set query_group to metrics;")
        cursor.execute(UPDATE_CMD_1.format(base_tbl_name))
        self._check_last_query_didnt_burst(cluster, cursor)

    def _update_tables_1_bursted(self, cluster, cursor, base_tbl_name):
        # Update when set bursted.
        cursor.execute("set query_group to burst;")
        cursor.execute(UPDATE_CMD_1.format(base_tbl_name))
        self._check_last_query_bursted(cluster, cursor)

    def _update_tables_2_not_bursted(self, cluster, cursor, base_tbl_name):
        # Update when set not bursted.
        cursor.execute("set query_group to metrics;")
        cursor.execute(UPDATE_CMD_2.format(base_tbl_name))
        self._check_last_query_didnt_burst(cluster, cursor)

    def _update_tables_2_bursted(self, cluster, cursor, base_tbl_name):
        # Update when set bursted.
        cursor.execute("set query_group to burst;")
        cursor.execute(UPDATE_CMD_2.format(base_tbl_name))
        self._check_last_query_bursted(cluster, cursor)

    def _read_bursted(self, cluster, cursor, base_tbl_name):
        # read when set bursted.
        cursor.execute("set query_group to burst;")
        cursor.execute(READ_CMD.format(base_tbl_name))
        self._check_last_query_bursted(cluster, cursor)

    def _copy_tables_not_bursted(self, cluster, cursor, base_tbl_name):
        # Copy when set not bursted.
        cursor.execute("set query_group to metrics;")
        cursor.execute(COPY_STMT.format(base_tbl_name, S3_PATH))
        copy_rows = cursor.last_copy_row_count()
        assert copy_rows == 21
        self._check_last_query_didnt_burst(cluster, cursor)

    def _copy_tables_bursted(self, cluster, cursor, base_tbl_name):
        # Copy when set bursted.
        cursor.execute("set query_group to burst;")
        cursor.execute(COPY_STMT.format(base_tbl_name, S3_PATH))
        copy_rows = cursor.last_copy_row_count()
        assert copy_rows == 21
        self._check_last_copy_bursted(cluster, cursor)

    def _insert_tables_cannot_bursted(self, cluster, cursor, base_tbl_name):
        cursor.execute("set query_group to burst;")
        cursor.execute(INSERT_CMD.format(base_tbl_name))
        self._check_last_query_didnt_burst(cluster, cursor)

    def _delete_tables_cannot_bursted(self, cluster, cursor, base_tbl_name):
        cursor.execute("set query_group to burst;")
        cursor.execute(DELETE_CMD.format(base_tbl_name))
        self._check_last_query_didnt_burst(cluster, cursor)

    def _update_tables_1_cannot_bursted(self, cluster, cursor, base_tbl_name):
        cursor.execute("set query_group to burst;")
        cursor.execute(UPDATE_CMD_1.format(base_tbl_name))
        self._check_last_query_didnt_burst(cluster, cursor)

    def _read_cannot_bursted(self, cluster, cursor, base_tbl_name):
        cursor.execute("set query_group to burst;")
        cursor.execute(READ_CMD.format(base_tbl_name))
        self._check_last_query_didnt_burst(cluster, cursor)

    def _copy_tables_cannot_bursted(self, cluster, cursor, base_tbl_name):
        cursor.execute("set query_group to burst;")
        cursor.execute(COPY_STMT.format(base_tbl_name, S3_PATH))
        copy_rows = cursor.last_copy_row_count()
        assert copy_rows == 21
        self._check_last_query_didnt_burst(cluster, cursor)

    def _validate_content_equivalence(self, cluster, cursor, base_tbl_name):
        # For the bursted case, compare the content on burst cluster
        # with that on the main cluster.
        # Get content on main.
        content_check = "select * from {} order by 1,2"
        cursor.execute("set query_group to metrics;")
        cursor.execute(content_check.format(base_tbl_name))
        self._check_last_query_didnt_burst(cluster, cursor)
        res_main = cursor.fetchall()
        # Get content on burst.
        cursor.execute("set query_group to burst;")
        cursor.execute(content_check.format(base_tbl_name))
        self._check_last_query_bursted(cluster, cursor)
        res_burst = cursor.fetchall()
        try:
            assert res_main == res_burst
        except AssertionError:
            log.info(
                "compared content between main and burst, main: {}, burst:{}".
                format(res_main, res_burst))
            assert res_main == res_burst


class TestBurstWriteMixedWorkload(TestBurstWriteMixedWorkloadBase):
    def base_test_burst_write_mixed_workload_without_txn(
            self, cluster, vector):
        """
        Test: for a single table, conduct:
              write(burst)
              write(main);
              write(burst);
              write(main);
              write(burst);
              read(main);
              read(burst);
        without txn. insert/delete/update/copy are covered.
        """
        test_schema = 'test_mixed_workload_schema_{}'.format(
            str(uuid.uuid4().hex))
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp28532"

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
                self._setup_tables(cluster, session1, base_tbl_name, vector)
                self._start_and_wait_for_refresh(cluster)
                # interleaved bursted and not bursted write on the same table.
                self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._insert_tables_not_bursted(cluster, dml_cursor,
                                                base_tbl_name)
                self._start_and_wait_for_refresh(cluster)
                self._delete_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._delete_tables_not_bursted(cluster, dml_cursor,
                                                base_tbl_name)
                self._start_and_wait_for_refresh(cluster)
                self._update_tables_1_bursted(cluster, dml_cursor,
                                              base_tbl_name)
                self._update_tables_1_not_bursted(cluster, dml_cursor,
                                                  base_tbl_name)
                self._start_and_wait_for_refresh(cluster)
                self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._copy_tables_not_bursted(cluster, dml_cursor,
                                              base_tbl_name)
                self._start_and_wait_for_refresh(cluster)
                # Validate content
                self._validate_content_equivalence(cluster, dml_cursor,
                                                   base_tbl_name)
                self._validate_table(cluster, test_schema, base_tbl_name,
                                     vector.diststyle)
                self._validate_table(cluster, test_schema, base_tbl_name,
                                     vector.diststyle)

                # Reversed order
                self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._copy_tables_not_bursted(cluster, dml_cursor,
                                              base_tbl_name)
                self._start_and_wait_for_refresh(cluster)
                self._update_tables_2_bursted(cluster, dml_cursor,
                                              base_tbl_name)
                self._update_tables_2_not_bursted(cluster, dml_cursor,
                                                  base_tbl_name)
                self._start_and_wait_for_refresh(cluster)
                self._delete_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._delete_tables_not_bursted(cluster, dml_cursor,
                                                base_tbl_name)
                self._start_and_wait_for_refresh(cluster)
                self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._insert_tables_not_bursted(cluster, dml_cursor,
                                                base_tbl_name)
                self._start_and_wait_for_refresh(cluster)
                # Validate content
                self._validate_content_equivalence(cluster, dml_cursor,
                                                   base_tbl_name)
                self._validate_table(cluster, test_schema, base_tbl_name,
                                     vector.diststyle)
                self._validate_table(cluster, test_schema, base_tbl_name,
                                     vector.diststyle)

    def base_test_burst_write_mixed_workload_within_txn(self, cluster, vector):
        """
        Test: for a single table, conduct:
              write(burst)
              write(main);
              write(burst);
              write(main);
              write(burst);
              read(main);
              read(burst);
        within txn. insert/delete/update/copy are covered.
        """
        test_schema = 'test_mixed_workload_schema_{}'.format(
            str(uuid.uuid4().hex))
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp28532"

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
                self._setup_tables(cluster, session1, base_tbl_name, vector)
                self._start_and_wait_for_refresh(cluster)
                dml_cursor.execute('begin;')
                self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._insert_tables_not_bursted(cluster, dml_cursor,
                                                base_tbl_name)
                dml_cursor.execute('commit')
                self._start_and_wait_for_refresh(cluster)
                dml_cursor.execute('begin;')
                self._delete_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._delete_tables_not_bursted(cluster, dml_cursor,
                                                base_tbl_name)
                dml_cursor.execute('commit')
                self._start_and_wait_for_refresh(cluster)
                dml_cursor.execute('begin;')
                self._update_tables_1_bursted(cluster, dml_cursor,
                                              base_tbl_name)
                self._update_tables_1_not_bursted(cluster, dml_cursor,
                                                  base_tbl_name)
                dml_cursor.execute('commit')
                self._start_and_wait_for_refresh(cluster)
                dml_cursor.execute('begin;')
                self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._copy_tables_not_bursted(cluster, dml_cursor,
                                              base_tbl_name)
                dml_cursor.execute('commit')
                self._start_and_wait_for_refresh(cluster)
                # Validate content
                self._validate_content_equivalence(cluster, dml_cursor,
                                                   base_tbl_name)
                self._validate_table(cluster, test_schema, base_tbl_name,
                                     vector.diststyle)
                self._validate_table(cluster, test_schema, base_tbl_name,
                                     vector.diststyle)

                # Reversed order
                dml_cursor.execute('begin')
                self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._copy_tables_not_bursted(cluster, dml_cursor,
                                              base_tbl_name)
                dml_cursor.execute('commit')
                self._start_and_wait_for_refresh(cluster)
                dml_cursor.execute('begin')
                self._update_tables_2_bursted(cluster, dml_cursor,
                                              base_tbl_name)
                self._update_tables_2_not_bursted(cluster, dml_cursor,
                                                  base_tbl_name)
                dml_cursor.execute('commit')
                self._start_and_wait_for_refresh(cluster)
                dml_cursor.execute('begin')
                self._delete_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._delete_tables_not_bursted(cluster, dml_cursor,
                                                base_tbl_name)
                dml_cursor.execute('abort')
                self._start_and_wait_for_refresh(cluster)
                dml_cursor.execute('begin')
                self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._insert_tables_not_bursted(cluster, dml_cursor,
                                                base_tbl_name)
                dml_cursor.execute('abort')
                self._start_and_wait_for_refresh(cluster)
                # Validate content
                self._validate_content_equivalence(cluster, dml_cursor,
                                                   base_tbl_name)
                self._validate_table(cluster, test_schema, base_tbl_name,
                                     vector.diststyle)
                self._validate_table(cluster, test_schema, base_tbl_name,
                                     vector.diststyle)


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
        'gconf_disk_cache_size': '100'
    })
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteMixedWorkloadSS(TestBurstWriteMixedWorkload):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['diststyle even', 'distkey(c0)'],
                sortkey=['', 'sortkey(c0)'],
                size=['small', 'large']))

    def test_burst_write_mixed_workload_without_txn(self, cluster, vector):
        self.base_test_burst_write_mixed_workload_without_txn(cluster, vector)

    def test_burst_write_mixed_workload_within_txn(self, cluster, vector):
        self.base_test_burst_write_mixed_workload_within_txn(cluster, vector)


@pytest.mark.serial_only
@pytest.mark.custom_burst_gucs(gucs=burst_write_basic_gucs)
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.cluster_only
class TestBurstWriteMixedWorkloadCluster(TestBurstWriteMixedWorkload):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['diststyle even', 'distkey(c0)'],
                sortkey=['', 'sortkey(c0)'],
                size=['small', 'large']))

    def test_burst_write_mixed_workload_without_txn(self, cluster, vector):
        self.base_test_burst_write_mixed_workload_without_txn(cluster, vector)

    def test_burst_write_mixed_workload_within_txn(self, cluster, vector):
        self.base_test_burst_write_mixed_workload_within_txn(cluster, vector)


@pytest.mark.serial_only
@pytest.mark.custom_burst_gucs(
    gucs=dict(
        burst_write_basic_gucs, **{
            'broadcast_cred_mode': 7,
            'burst_propagated_static_gucs':
            '{"padbGucs": ['
            '{"name": "broadcast_cred_mode", "value": "7"}]}'
        }))
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.cluster_only
@pytest.mark.cred_broadcasting
class TestBurstWriteMixedWorkloadClusterCredBroadcasting(
        TestBurstWriteMixedWorkload, CredBroadcasting):
    '''
        Test burst cluster copy queries with the IAM credentials broadcasting
        enabled for copy queries.
        When the guc "broadcast_cred_mode" is set to 7, that means the
        credentials broadcasting functionality from LN to CN is enabled for
        copy queries.
    '''

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['diststyle even'],
                sortkey=['sortkey(c0)'],
                size=['large']))

    def test_burst_write_mixed_workload_without_txn_cred_broadcasting(
            self, cluster, vector):
        self.base_test_burst_write_mixed_workload_without_txn(cluster, vector)

    def test_burst_write_mixed_workload_within_txn_cred_broadcasting(
            self, cluster, vector):
        self.base_test_burst_write_mixed_workload_within_txn(cluster, vector)
