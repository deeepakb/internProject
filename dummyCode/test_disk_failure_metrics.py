import pytest
import os
import logging
import time
from datetime import datetime, timedelta

from raff.monitoring.monitoring_test import MonitoringTestSuite

log = logging.getLogger(__name__)


@pytest.mark.localhost_only
@pytest.mark.serial_only
class TestDiskFailureMetrics(MonitoringTestSuite):
    """
    Test if disk failure reason metrics is updated after xpx command.
    """

    disk_failure_metrics_file = "/dev/shm/redshift1/monitoring/disk/failures"
    metric_name = "DiskFailures"

    @pytest.mark.localhost_only
    def test_disk_failure_metrics_result(self, cluster, cluster_session):
        """
        Fail one disk on one node, verify the number of failed disks
        in the result file is correct.
        """
        gucs = dict(
            disk_mirror_count="1",
            monitoring_metric_blacklist="",
            enable_padb_monitoring_agent="true",
            monitoring_system_poller_wait_secs="5")
        # remove metric file left from previous retries or tests
        self.remove_local_file(self.disk_failure_metrics_file)
        with cluster_session(gucs=gucs,
                             clean_db_before=True, clean_db_after=True):
            cluster.run_xpx("fail_lun 1 0 -f")
            start_time = datetime.now()
            loop_till = start_time + timedelta(seconds=120)
            value_found = False
            while datetime.now() <= loop_till:
                if not os.path.isfile(self.disk_failure_metrics_file):
                    time.sleep(1)
                else:
                    value = self.read_value_for_metric(
                            self.disk_failure_metrics_file,
                            "value",
                            ["name", self.metric_name, "metadata", None])
                    value_found = True
                    break
            if not value_found:
                log.info("We cannot find {0} and got a timeout error".format(
                                 self.disk_failure_metrics_file))
                assert False, "No metrics file found and timed out."
            assert value == 1
