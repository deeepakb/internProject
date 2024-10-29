# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest
import time
import logging

from raff.common.db.session_context import SessionContext
from raff.monitoring.monitoring_test import MonitoringTestSuite

log = logging.getLogger(__name__)


@pytest.mark.session_ctx(user_type=SessionContext.BOOTSTRAP)
@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.skip_asan
class TestLogQueryHeapMemoryUsageMetrics(MonitoringTestSuite):
    """
    Test query heap memory usage and time consumption per component metrics.
    """

    PID_SQL = 'SELECT PG_REAL_BACKEND_PID()'
    SELECT_SQL = 'SELECT * FROM stv_slices'
    XPX_ENABLE_BOOTSTRAP_LOGGING = "xpx 'event set EtLogHeapMemUsageForBootstrap'"
    XPX_SET_STUCK = "xpx 'event set EtSimStuckBeforeWlm,pid={},timeout_secs={}'"
    XPX_CONSUME_HEAP_MEM = "xpx 'event set EtSimConsumeHeapMem,pid={},allocate_bytes={}'"

    def read_metric_file(self, component_metric_file, metric_name):
        # Timeout for metrics to populate.
        timeout = 60 * 2
        iteration_delay = 5
        counter = 0
        while counter < timeout:
            metric_value = self.read_value_for_metric(component_metric_file,
                                                      "value",
                                                      ["name", metric_name])
            if metric_value is not None:
                break
            else:
                time.sleep(iteration_delay)
                counter += iteration_delay
        return metric_value

    def test_log_query_heap_memory_by_component_metrics(
            self, cluster, cluster_session, db_session):
        component_metric_file = '/dev/shm/redshift/monitoring/component/stats'
        mem_metric_name = 'HeapMemoryUsage'
        time_metric_name = 'TimeConsumption'
        custom_gucs = {
            'enable_result_cache_for_session': 'false',
            'enable_query_heap_mem_usage_logging': 'true',
            'min_time_consumption_logging': '64',
            'min_heap_mem_usage_logging': '100',
            'heap_oom_guard_mode': '4'
        }
        with cluster_session(gucs=custom_gucs):
            with db_session.cursor() as cursor:
                try:
                    # Let's wait for a minute after the cluster has restarted,
                    # to make sure the metric data from past queries has been
                    # recorded. The current metric reporting time interval is
                    # 60 seconds.
                    time.sleep(60)
                    # Remove metric file left from previous retries or tests.
                    self.remove_local_file(component_metric_file)
                    # Enable logging for bootstrap user.
                    cursor.execute(self.XPX_ENABLE_BOOTSTRAP_LOGGING)
                    # Find the current process id.
                    cursor.execute(self.PID_SQL)
                    pid = cursor.fetch_scalar()
                    # Let the query stuck in the planner component for 90
                    # seconds.
                    stuck_time_sec = 90
                    cursor.execute(
                        self.XPX_SET_STUCK.format(pid, stuck_time_sec))
                    # Allocate 128 MB of heap memory within planner component.
                    consume_heap_mem_mb = 128
                    cursor.execute(
                        self.XPX_CONSUME_HEAP_MEM.format(
                            pid, consume_heap_mem_mb * 1024 * 1024))
                    cursor.execute(self.SELECT_SQL)

                    # Check whether the metric file is available.
                    self.check_if_file_exists(component_metric_file)

                    # Verify that the heap memory consumption is correctly
                    # reflected.
                    heap_mem_value = self.read_value_for_metric(
                        component_metric_file, "value",
                        ["name", mem_metric_name])
                    assert heap_mem_value >= consume_heap_mem_mb, (
                        "Recorded heap memory usage value {}".format(
                            heap_mem_value))

                    # Verify that the time consumption is correctly reflected.
                    time_metric_value = self.read_metric_file(
                        component_metric_file, time_metric_name)
                    assert time_metric_value >= stuck_time_sec, (
                        "Recorded time consumption value {}".format(
                            time_metric_value))
                    if time_metric_value is None:
                        log.error("Could not find metric %s in metrics file." %
                                  time_metric_name)
                        log.info("Metric value for file {} key {}: {}".format(
                            component_metric_file, time_metric_name,
                            time_metric_value))
                finally:
                    # Unset the events.
                    cursor.execute(
                        "xpx 'event unset EtLogHeapMemUsageForBootstrap'")
                    cursor.execute("xpx 'event unset EtSimStuckBeforeWlm'")
                    cursor.execute("xpx 'event unset EtSimConsumeHeapMem'")
