import pytest
import logging
import os
import time
import threading
from datetime import datetime, timedelta
from raff.util.utils import PropagatingThread

from raff.common.db.db_exception import ProgrammingError
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.monitoring.monitoring_test import MonitoringTestSuite

log = logging.getLogger(__name__)

@pytest.mark.serial_only
class TestCanReissueStatusMetrics(MonitoringTestSuite):
    """ Check whether the SessionState metric are created correctly.
        TODO: @amzxiao adding more tests for different scenarios:
        https://issues.amazon.com/issues/Autopilot-271
    """

    stage_metric_file = "/dev/shm/redshift/monitoring/session/stats"
    metrics_name = "SessionState"

    def get_metric_value(self):
        """
        Aggregate count of 0 of metric value from giving metrics for 2 minutes.
        """
        zero_cnt = 0
        filename = self.stage_metric_file
        total = 0

        while (zero_cnt == 0) and (total < 36):
            if os.path.exists(filename):
                cur_value = self.read_value_for_metric(filename,
                                            "value",
                                            ["name", self.metrics_name])
                if cur_value == 0:
                    zero_cnt = zero_cnt + 1
            time.sleep(5)
            total = total + 1

        log.debug("0 value cnt for metric {}: {}".format(self.metrics_name, zero_cnt))
        return zero_cnt

    def run_query(self, db_session, query):
        """
        Function to run queries in transaction block for 2 minutes.
        """
        try:
            with db_session.cursor() as cursor:
                cursor.execute("BEGIN")
                cursor.execute(query)

                time.sleep(135)

                cursor.execute(query)
                cursor.execute("END")
        # Any error is treated as failure of the test
        except Exception as e:
            log.info("Query failed with the exception {}".format(str(e)))
            raise Exception("Query execution error")

    @pytest.mark.session_ctx(user_type='regular')
    @pytest.mark.localhost_only
    def test_can_reissue_queries_locally(self, cluster, cluster_session):
        '''
        This test case is testing SessionState CW metrics data publish
        correctness. It would test the query running 2 minutes in transacation
        block.
        It creates two thread:
        1. Aggregate metrics value once per 5 seconds for 2 minutes.
        2. Query running in transcation block for 2 minutes.
        Verify
        1. The count of 0 of the metrics value is larger than 0.
        '''
        gucs = {
            "enable_session_status_tracking": "true",
            "enable_nft_query_reissue": "true"
        }
        self.set_helper_file_loc()
        is_bootstrap = False
        with cluster_session(gucs=gucs, clean_db_before=True):
            try:
                normal_query = "select * from stl_query"

                run_query_thread = PropagatingThread(
                    target=self.run_query,
                    args=[
                        DbSession(cluster.get_conn_params()),
                        normal_query
                    ])

                run_query_thread.start()
                value =  self.get_metric_value()
                # Any error is treated as failure of the test
                log.info("0 count for regular user is {}".format(str(value)))
                assert value > 0
            finally:
                run_query_thread.join()


    @pytest.mark.session_ctx(user_type='bootstrap')
    @pytest.mark.no_jdbc
    @pytest.mark.localhost_only
    def test_can_reissue_queries_for_bootstrap_user_locally(self, db_session, cluster,
                                            cluster_session, s3_client):
        """
        This test case is testing SessionState CW metrics data publish
        correctness for bootstrap user.

        Run any single statement readonly query the metrics value 0 count
        should == 0.
        """
        gucs = {
            "enable_session_status_tracking": "true",
            "enable_nft_query_reissue": "true"
        }
        self.set_helper_file_loc()
        is_bootstrap = True
        with cluster_session(gucs=gucs, clean_db_before=True):
            try:
                normal_query = "select * from stl_query"

                run_query_thread = PropagatingThread(
                    target=self.run_query,
                    args=[
                        db_session,
                        normal_query
                    ])
                run_query_thread.start()
                value =  self.get_metric_value()
                # Any error is treated as failure of the test
                log.info("0 count for bootstrap user is {}".format(str(value)))
                assert value == 0
            finally:
                run_query_thread.join()
