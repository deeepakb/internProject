import pytest
import os
import time
import os.path
import logging

from raff.monitoring.monitoring_test import MonitoringTestSuite
from raff.common.db.session_context import SessionContext
from raff.common.db.session import DbSession

log = logging.getLogger(__name__)


@pytest.mark.serial_only
@pytest.mark.localhost_only
class TestCloudWatchMetrics(MonitoringTestSuite):
    """ Check whether the Cloud watch metrics are emitted correctly."""

    stage_metric_file = "/dev/shm/redshift/monitoring/db/public"

    def get_metric_value(self, metric):
        """ Helper to call the read_matching_json_metadata_value with some
        boilerplate for this test.
        """
        value = self.read_value_for_metric(self.stage_metric_file,
                                                       "value",
                                                       ["name", metric])
        log.debug("value for metric {}: {}".format(metric, value))
        return value

    def test_regular_user_db_connections(self, db_session, cluster, host_type):
        """ Check to see that regular user sessions are correctly represented by
        the metric.
        """
        conn_params = cluster.get_conn_params()
        # creatiing 5 regular user session
        ctx1 = SessionContext(user_type=SessionContext.REGULAR)
        ctx2 = SessionContext(user_type=SessionContext.REGULAR)
        ctx3 = SessionContext(user_type=SessionContext.REGULAR)
        ctx4 = SessionContext(user_type=SessionContext.REGULAR)
        ctx5 = SessionContext(user_type=SessionContext.REGULAR)
        with DbSession(conn_params, session_ctx=ctx1) as sess1, \
                DbSession(conn_params, session_ctx=ctx2) as sess2, \
                DbSession(conn_params, session_ctx=ctx3) as sess3, \
                DbSession(conn_params, session_ctx=ctx4) as sess4, \
                DbSession(conn_params, session_ctx=ctx5) as sess5:
                    sn1 = sess1.session_ctx.schema
                    sn2 = sess2.session_ctx.schema
                    sn3 = sess3.session_ctx.schema
                    sn4 = sess4.session_ctx.schema
                    sn5 = sess5.session_ctx.schema
                    with sess1.cursor() as cursor1:
                        with sess2.cursor() as cursor2:
                            with sess3.cursor() as cursor3:
                                with sess4.cursor() as cursor4:
                                    with sess5.cursor() as cursor5:
                                        self.remove_local_file( \
                                                self.stage_metric_file)
                                        while not os.path.isfile( \
                                                self.stage_metric_file):
                                            time.sleep(2)
        metric = "DatabaseConnections"
        val = self.get_metric_value(metric)
        if val != 5:
            pytest.fail("The metric {} should have a value 5 but instead"
                        "found {}".format(metric, val))


    def test_bootstrap_user_db_connections(self, db_session, cluster, host_type):
        """ Test to see that bootstrap user sessions are not accounted for in
        the metric.
        """
        conn_params = cluster.get_conn_params()
        # creating two bootstrap user session context
        ctx1 = SessionContext(user_type=SessionContext.BOOTSTRAP)
        ctx2 = SessionContext(user_type=SessionContext.BOOTSTRAP)
        with DbSession(conn_params, session_ctx=ctx1) as sess1, \
                DbSession(conn_params, session_ctx=ctx2) as sess2:
                    sn1 = sess1.session_ctx.schema
                    sn2 = sess2.session_ctx.schema
                    with sess1.cursor() as cursor1:
                        with sess2.cursor() as cursor2:
                            self.remove_local_file( \
                                    self.stage_metric_file)
                            while not os.path.isfile( \
                                    self.stage_metric_file):
                                time.sleep(2)
        metric = "DatabaseConnections"
        val = self.get_metric_value(metric)
        if val != 0:
            pytest.fail("The metric {} should have a value 0 but instead"
                        "found {}".format(metric, val))

    def test_mixed_db_connections(self, db_session, cluster, host_type):
        """ Test the situation when there're both bootstrap sessions and regular
        session, the metric should only account for the regular session.
        """
        conn_params = cluster.get_conn_params()
        # creatiing 5 bootstrap sessions and 1 regular session
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
                                            self.stage_metric_file)
                                    while not os.path.isfile(
                                            self.stage_metric_file):
                                        time.sleep(2)
        metric = "DatabaseConnections"
        val = self.get_metric_value(metric)
        if val != 1:
            pytest.fail("The metric {} should have a value 1 but instead "
                        "found {}".format(metric, val))
