import logging
import pytest
import time

from raff.common.cluster.cluster_session import ClusterSession
from raff.common.db.session_context import SessionContext
from raff.common.db.session import DbSession
from raff.monitoring.monitoring_test import MonitoringTestSuite

log = logging.getLogger(__name__)


@pytest.yield_fixture(scope='class')
def test_gucs(request, cluster):
    session = ClusterSession(cluster)
    gucs = {
        "is_arcadia_cluster": "true",
        "enable_arcadia": "true"
    }
    with session(gucs=gucs, clean_db_after=True):
        yield


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.usefixtures("test_gucs")
class TestDatabaseConnectionsCloudWatchMetric(MonitoringTestSuite):
    """
    Check whether the DatabaseConnections CloudWatch metric is emitted
    correctly
    """

    serverless_metric_file = "/dev/shm/redshift/monitoring/db/serverless_stats"

    def verify_metric(self, expected_value):
        # 2 minute timeout, since system poller iterates per 60 sec
        timeout = 60 * 2
        iteration_delay = 15
        counter = 0
        while (counter < timeout):
            try:
                # Metric at database level
                value = self.read_value_for_metric(
                    self.serverless_metric_file,
                    "value",
                    ["name", "DatabaseConnections",
                     "groups", [[
                         "ClusterIdentifier",
                         "Workgroup",
                         "DatabaseName"]]])
                if value != expected_value:
                    pytest.fail(
                        ("DatabaseConnections not correct, expected: {}, "
                         "actual: {}".format(expected_value, value)))
                # Metric at workgroup level
                value = self.read_value_for_metric(
                    self.serverless_metric_file,
                    "value",
                    ["name", "DatabaseConnections",
                     "groups", [[
                         "ClusterIdentifier",
                         "Workgroup"]]])
                if value != expected_value:
                    pytest.fail(
                        ("DatabaseConnections not correct, expected: {}, "
                         "actual: {}".format(expected_value, value)))
            except IOError:
                log.warn("File {} not found".format(
                    self.serverless_metric_file))
                pass
            time.sleep(iteration_delay)
            counter += iteration_delay

    def test_regular_user_database_connections_serverless(
            self, cluster):
        """
        Check to see that DatabaseConnections metric correctly represents the
        number of active sessions, for a regular user.
        """

        conn_params = cluster.get_conn_params()
        # Creating 2 regular sessions.
        ctx1 = SessionContext(user_type=SessionContext.REGULAR)
        ctx2 = SessionContext(user_type=SessionContext.REGULAR)

        with DbSession(conn_params, session_ctx=ctx1) as sess1, \
                sess1.cursor():
            self.remove_local_file(self.serverless_metric_file)
            self.verify_metric(1)
            with DbSession(conn_params, session_ctx=ctx2) as sess2, \
                    sess2.cursor():
                self.remove_local_file(self.serverless_metric_file)
                self.verify_metric(2)
                sess2.close()
                self.remove_local_file(self.serverless_metric_file)
                self.verify_metric(1)
            sess1.close()
            self.remove_local_file(self.serverless_metric_file)
            self.verify_metric(0)

    def test_mixed_db_connections_serverless(
            self, cluster):
        """
        Test that regular user sessions count for the metric, but
        bootstrap user sessions do not.
        """
        conn_params = cluster.get_conn_params()
        # Creating 5 bootstrap sessions and 1 regular session.
        ctx1 = SessionContext(user_type=SessionContext.BOOTSTRAP)
        ctx2 = SessionContext(user_type=SessionContext.BOOTSTRAP)
        ctx3 = SessionContext(user_type=SessionContext.BOOTSTRAP)
        ctx4 = SessionContext(user_type=SessionContext.BOOTSTRAP)
        ctx5 = SessionContext(user_type=SessionContext.BOOTSTRAP)
        ctx6 = SessionContext(user_type=SessionContext.REGULAR)
        with DbSession(conn_params, session_ctx=ctx1) as sess1, \
                DbSession(conn_params, session_ctx=ctx2) as sess2, \
                DbSession(conn_params, session_ctx=ctx3) as sess3, \
                DbSession(conn_params, session_ctx=ctx4) as sess4, \
                DbSession(conn_params, session_ctx=ctx5) as sess5, \
                DbSession(conn_params, session_ctx=ctx6) as sess6:
            with sess1.cursor():
                with sess2.cursor():
                    with sess3.cursor():
                        with sess4.cursor():
                            with sess5.cursor():
                                with sess6.cursor():
                                    self.remove_local_file(
                                            self.serverless_metric_file)
                                    self.verify_metric(1)
