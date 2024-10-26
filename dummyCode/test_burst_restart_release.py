# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest

from raff.util.utils import run_bootstrap_sql
from raff.burst.burst_test import (BurstTest, setup_teardown_burst,
                                   burst_build_to_acquire)

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst", "burst_build_to_acquire"]

log = logging.getLogger(__name__)

DISABLE_SHUTDOWN_RELEASE_GUCS = {
    'burst_mode': '3',
    'enable_burst_startup_release': 'true',
    'enable_burst_shutdown_release': 'false',
    'enable_burst_async_acquire': 'false'
}

ENABLE_SHUTDOWN_RELEASE_GUCS = {
    'burst_mode': '3',
    'enable_burst_shutdown_release': 'true',
    'enable_burst_async_acquire': 'false'
}

LATEST_RELEASE_REASON_SQL = """
select reason from stl_burst_service_client where action = 'RELEASE'
order by eventtime desc limit 1;
"""


def release_and_verify_reason(cluster, expected_reason,
                              burst_build_to_acquire):
    starttime = cluster.get_cluster_start_time()
    cluster.release_all_burst_clusters()
    cluster.acquire_burst_cluster(burst_build_to_acquire)

    cluster.reboot_cluster()

    assert starttime != cluster.get_cluster_start_time(
    ), "Cluster didn't restart"

    reason = run_bootstrap_sql(cluster, LATEST_RELEASE_REASON_SQL)[0][0]
    assert reason.strip() == expected_reason


@pytest.mark.serial_only
@pytest.mark.usefixtures('setup_teardown_burst')
@pytest.mark.custom_burst_gucs(gucs=DISABLE_SHUTDOWN_RELEASE_GUCS)
@pytest.mark.cluster_only
class TestBurstReleaseOnStartup(BurstTest):
    def test_burst_release_on_startup(self, cluster, burst_build_to_acquire):
        '''
        Test that verifies that clusters are correctly released on startup.
        '''
        release_and_verify_reason(cluster, "Startup", burst_build_to_acquire)


@pytest.mark.serial_only
@pytest.mark.usefixtures('setup_teardown_burst')
@pytest.mark.custom_burst_gucs(gucs=ENABLE_SHUTDOWN_RELEASE_GUCS)
@pytest.mark.cluster_only
class TestBurstReleaseOnShutdown(BurstTest):
    def test_burst_release_on_shutdown(self, cluster, burst_build_to_acquire):
        '''
        Test that verifies that clusters are correctly released on shutdown.
        '''
        release_and_verify_reason(cluster, "Shutdown", burst_build_to_acquire)


@pytest.mark.serial_only
@pytest.mark.usefixtures('setup_teardown_burst')
@pytest.mark.custom_burst_gucs(gucs=ENABLE_SHUTDOWN_RELEASE_GUCS)
@pytest.mark.cluster_only
class TestBurstReleaseOnCrash(BurstTest):
    def test_burst_release_on_crash(self, cluster, burst_build_to_acquire):
        '''
        Test that verifies that clusters are correctly released after crash.
        '''
        starttime = cluster.get_cluster_start_time()
        cluster.run_xpx('burst_release_all')
        cluster.acquire_burst_cluster(burst_build_to_acquire)

        try:
            cluster.run_xpx('cause_error sig11')
        except Exception:
            # expected
            log.debug('Cluster has been crashed on purpose.')

        cluster.wait_for_padb_up()

        assert starttime != cluster.get_cluster_start_time(
        ), "Cluster didn't restart"

        reason = run_bootstrap_sql(cluster, LATEST_RELEASE_REASON_SQL)[0][0]
        assert reason.strip() == 'Startup'
