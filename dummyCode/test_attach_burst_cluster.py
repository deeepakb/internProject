# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
import logging
import pytest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_test import setup_teardown_burst, is_burst_cluster_fresh
from raff.burst.burst_test import get_burst_clusters_arns
from raff.burst.burst_write import burst_write_basic_gucs
from raff.storage.storage_test import disable_all_autoworkers
from test_burst_mv_refresh import TestBurstWriteMVBase
log = logging.getLogger(__name__)
__all__ = [disable_all_autoworkers, super_simulated_mode, setup_teardown_burst]


@pytest.mark.cluster_only
@pytest.mark.custom_burst_gucs(
    gucs=dict(list(burst_write_basic_gucs.items()) + [(
        'burst_percent_threshold_to_trigger_backup',
        '100'), ('burst_cumulative_time_since_stale_backup_threshold_s',
                 '86400'), ('enable_burst_async_acquire', 'false')]))
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.serial_only
@pytest.mark.skip_load_data
@pytest.mark.keep_burst
@pytest.mark.usefixtures("disable_all_autoworkers")
@pytest.mark.no_jdbc
@pytest.mark.no_burst_nightly
class TestAttachBurstCluster(TestBurstWriteMVBase):
    def test_attach_burst_cluster(self, cluster):
        cluster_arns = get_burst_clusters_arns(cluster)
        assert len(cluster_arns) == 1
        assert is_burst_cluster_fresh(cluster)

    def test_backup_main_cluster(self, cluster):
        cluster_arns = get_burst_clusters_arns(cluster)
        assert len(cluster_arns) == 1
        cluster.run_xpx('backup backupMainCluster')
        assert not is_burst_cluster_fresh(cluster)
        cluster_arns = get_burst_clusters_arns(cluster)
        assert len(cluster_arns) == 1

    def test_detach_burst_cluster(self, cluster):
        cluster_arns = get_burst_clusters_arns(cluster)
        assert len(cluster_arns) == 1
        cluster.release_all_burst_clusters()
        cluster_arns = get_burst_clusters_arns(cluster)
        assert not cluster_arns
        assert not is_burst_cluster_fresh(cluster)
        cluster_arns = get_burst_clusters_arns(cluster)
        assert not cluster_arns
