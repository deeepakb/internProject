# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import logging
import time
from datetime import datetime, timedelta

from raff.monitoring.monitoring_test import MonitoringTestSuite
from raff.util.utils import run_bootstrap_sql

log = logging.getLogger(__name__)


@pytest.mark.cluster_only
@pytest.mark.serial_only
class TestDiskChecker(MonitoringTestSuite):
    """
    Test if disk checker is able to parse /proc/diskstats correctly.
    """
    def test_disk_checker_parsing(self, cluster, cluster_session, db_session):
        """
        Verify that there is no error message in stl_print like
        'An error occurred while reading /proc/diskstats, disk: %'
        """
        gucs = dict(
            disk_checker_sample_duration_secs="5",
            enable_padb_monitoring_agent="true",
            monitoring_metric_blacklist="",
            monitoring_system_poller_wait_secs="5")
        with cluster_session(gucs=gucs,
                             clean_db_before=True,
                             clean_db_after=True):
            start_time = datetime.now()
            loop_till = start_time + timedelta(seconds=120)
            while datetime.now() <= loop_till:
                test_query = """
                    SELECT *
                    FROM stl_print
                    WHERE message LIKE
                    'An error occurred while reading /proc/diskstats, %'
                    """
                lst = run_bootstrap_sql(cluster, test_query)
                assert len(lst) == 0
                time.sleep(5)
