import logging
import os
import pytest

from raff.monitoring.monitoring_test import MonitoringTestSuite
from io import open

log = logging.getLogger(__name__)


@pytest.mark.serial_only
@pytest.mark.localhost_only
class TestMetricFileCleanup(MonitoringTestSuite):
    """
    Metric files are removed when database starts.
    """

    def test_cleanup_metric_file_before_folder_recreate(self, cluster, cluster_session):
        """
        This test case is testing, if metrics files are cleaned up before
        folder recreating, when database starts.
        """
        with cluster_session(clean_db_before=True):
            metrics_file_folder = '/dev/shm/redshift/monitoring/filesystem'
            metrics_file_path = "{}/1.txt".format(metrics_file_folder)
            if not os.path.exists(metrics_file_folder):
                os.makedirs(metrics_file_folder)
            log.info("Creating dummy file to be deleted later on restart.")
            with open(metrics_file_path, "w+") as fd:
                fd.write("Some text strings")
            assert os.path.exists(metrics_file_path)

            cluster.reboot_cluster()
            err_msg = "Metrics file folder is not cleaned."
            assert not os.path.exists(metrics_file_path), err_msg
