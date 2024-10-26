# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest

from datetime import datetime
from raff.burst.burst_test import (BurstTest, setup_teardown_burst,
                                   burst_build_to_acquire)


# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst", "burst_build_to_acquire"]


CUSTOM_GUCS = {
    'burst_mode': '3',
    'is_burst_cluster': 'false'
}


@pytest.mark.skip(reason=('Disabling until burst mode 3 '
                          'can run in functional tests'))
@pytest.mark.cluster_only
@pytest.mark.usefixtures('setup_teardown_burst')
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS)
@pytest.mark.serial_only
class TestBurstSimSlowAcquire(BurstTest):
    SLOW_ACQUIRE_SLEEP_SECONDS = 10

    def test_burst_sim_slow_acquire(self, cluster, burst_build_to_acquire):
        cluster.run_xpx('burst_release_all')

        with cluster.event('EtBurstSimSlowAcquire',
                           'sleep={}'.format(self.SLOW_ACQUIRE_SLEEP_SECONDS)):
            start_time = datetime.now()
            cluster.acquire_burst_cluster(burst_build_to_acquire)
            elapsed = datetime.now() - start_time
            assert(elapsed.total_seconds() > self.SLOW_ACQUIRE_SLEEP_SECONDS)
