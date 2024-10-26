# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
import time
import uuid

from datetime import datetime, timedelta

from raff.burst.burst_test import (
    BurstTest,
    setup_teardown_burst
)
from raff.common.cluster.cluster_helper import RedshiftClusterHelper
from raff.util.utils import run_bootstrap_sql

log = logging.getLogger(__name__)


# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst]


CUSTOM_GUCS = dict(
    burst_mode='3',
    enable_burst_refresh='true',
    burst_refresh_expiration_seconds='300',
    burst_refresh_start_seconds='-1',
    burst_refresh_check_seconds='-1',
    burst_commit_refresh_check_frequency_seconds='-1',
    burst_max_idle_time_seconds='3600'
)
TIMEOUT = 180
CHECK_REFRESH_QUERY = (
    "select count(*) from stl_burst_manager_refresh "
    "where action = 'RefreshEnd' and cluster_arn = '{}'"
)


class RefreshTimeOut(Exception):
    pass


@pytest.mark.load_tpcds_data
@pytest.mark.cluster_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS)
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstRefreshResetSorted(BurstTest):

    def test_burst_refresh_reset_sorted(self, cluster):
        '''
        Test Burst Refresh correctness after table sorted region has been
        changed.

        Args:
            cluster (RedshiftCluster): Redshift cluster instance
        '''

        # personalize burst cluster
        burst_cluster_arn = cluster.list_acquired_burst_clusters()[0]
        cluster.personalize_burst_cluster(burst_cluster_arn)

        # change sortness of table catalog_sales
        cluster.run_xpx(
            'reset_sortedrows {} allunsorted 0'.format('catalog_sales'))

        # backup cluster
        snapshot_id = ("{}-{}".format(
            cluster.cluster_identifier, str(uuid.uuid4().hex)))
        cluster.backup_cluster(snapshot_id)

        # issue Burst Refresh
        cluster.run_xpx('burst_start_refresh')
        start_time = datetime.now()
        while True:
            if datetime.now() > start_time + timedelta(seconds=TIMEOUT):
                raise RefreshTimeOut(
                    "Refresh didn't finish within {} seconds".format(TIMEOUT))
            else:
                cluster.run_xpx('burst_check_refresh')
                rows = run_bootstrap_sql(
                    cluster, CHECK_REFRESH_QUERY.format(burst_cluster_arn))
                result = int(rows[0][0])
                if result > 0:
                    break
                else:
                    time.sleep(1)

        RedshiftClusterHelper.delete_snapshot(snapshot_id)
