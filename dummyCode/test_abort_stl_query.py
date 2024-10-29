# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import logging

from psycopg2.extensions import QueryCanceledError
from raff.common.db.db_exception import DatabaseError, InternalError
from raff.common.db.driver.jdbc_exception import Error
from raff.common.db.session_context import SessionContext
from raff.monitoring.monitoring_test import MonitoringTestSuite

log = logging.getLogger(__name__)


@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.session_ctx(user_type=SessionContext.BOOTSTRAP)
class TestAbortSTLQuery(MonitoringTestSuite):
    """
    Test that PADB should not crash when STL query aborted. This class
    simulates two scenraios: 1. STL query aborted due to timeout. 2. STL query
    throw exception at scan step.
    """

    # GUC overrides for tests in this class.
    GUCS = {
        'enable_query_sys_table_log_from_S3': 'true',
        's3_stl_bucket': 'padb-monitoring-test',

        # Enable stl_query_heap_memory_usage logging in localhost mode for
        # additional diagnostics should the test fail again due to DP-49224.
        'enable_query_heap_mem_usage_logging': 'true'
    }

    def verify_no_invariant(self, db_session, cluster):
        invariant_query = "select count(*) from stl_invariant  \
            where  message like '%Iterator initialized multiple times%';"

        with db_session.cursor() as cursor:
            # Turn off timeout.
            cursor.execute('SET statement_timeout TO 0;')
            invariant_cnt = self.run_query_first_result(
                db_session, cluster, invariant_query)
            cursor.execute(invariant_query)
            assert 0 == int(invariant_cnt)

    def test_PADB_should_not_crash_when_stl_query_aborted_due_to_timeout(
            self, cluster, cluster_session, db_session):
        """
        Run STL query with very short timeout to trigger STL query canceled,
        and verify that the timeout query won't crash PADB.
        """
        self.cleanup_log_files_by_type('stl_invariant')
        with cluster_session(gucs=self.GUCS):
            with db_session.cursor() as cursor:
                sample_stl_query = "select count(*) from stl_scan;"
                # Warm up.
                cursor.execute(sample_stl_query)
                abort_count = 0
                # Timeout in mill secs.
                for time_out in [5, 10, 50, 100, 200, 500, 1000, 2000, 3000]:
                    try:
                        cursor.execute(
                            'SET statement_timeout TO {};'.format(time_out))
                        cursor.execute(sample_stl_query)
                    # Catch query cancel exception, other exception will
                    # fail this test.
                    except (QueryCanceledError, Error) as e:
                        # Different abort paths throw different message.
                        error_msg1 = "cancelled on user's request"
                        error_msg2 = "canceled on user's request"
                        assert str(e).strip().find(error_msg1) != -1 or \
                            str(e).strip().find(error_msg2) != -1
                        abort_count += 1
                assert abort_count > 0
                # Verify Systbl_Iterator won't be init multiple times.
                self.verify_no_invariant(db_session, cluster)

    def test_PADB_should_not_crash_when_init_STL_iterator_throws_exception(
            self, cluster, cluster_session, db_session):
        """
        Test PADB won't crash when STL query throw exception at STL scan step.
        """
        sample_stl_query = "select count(*) from stl_scan;"
        self.cleanup_log_files_by_type('stl_invariant')
        with cluster_session(gucs=self.GUCS):
            # The query should fail when STL query throws execption at scan
            # step, but PADB should not crash.
            with cluster.event('EtThrowExceptionWhenInitSTLIterator'):
                with db_session.cursor() as cursor:
                    error = None
                    try:
                        cursor.execute(sample_stl_query)
                    except tuple(InternalError + DatabaseError) as e:
                        error = e
                    error_msg = 'Simulate S3 client excpetion'
                    assert str(error).strip().find(error_msg) != -1
            # Run stl query again to verify the PADB is healthy.
            with db_session.cursor() as cursor:
                cursor.execute(sample_stl_query)
                scan_count = cursor.fetch_scalar()
                assert scan_count >= 0
            # Verify Systbl_Iterator won't be init multiple times.
            self.verify_no_invariant(db_session, cluster)
