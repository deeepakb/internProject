# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest

from raff.burst.burst_refresh_test import BurstRefreshTest, restore_padb

log = logging.getLogger(__name__)

__all__ = [restore_padb]

# global slice number mapping for distall tables from Burst to Main
DISTALL_SLICE_MAP = {
    0: 0,
    2: 2,
    4: 4
}
# global slice number mapping for non-distall tables from Burst to Main
NON_DISTALL_SLICE_MAP = {
    0: 0,
    1: 1,
    2: 2,
    3: 3,
    4: 4,
    5: 5
}
MAIN_NODE_SLICE_INFO = dict(
    main_size=3,
    main_slices_per_node=2,
    main_node_slice_map=None
)
BURST_NODE_SLICE_INFO = dict(
    burst_size=3,
    burst_slices_per_node=2
)
MAIN_BURST_SLICE_MAPPING = dict(
    distall=DISTALL_SLICE_MAP,
    non_distall=NON_DISTALL_SLICE_MAP
)


@pytest.mark.usefixtures("restore_padb")
@pytest.mark.serial_only
class TestBurstRefreshSameTopology(BurstRefreshTest):
    '''
    Test Burst refresh if Burst cluster has the same topology as main
    cluster.

    Both Main and Burst cluster are:
        3 nodes x 2 slices/node
    '''

    def test_burst_refresh_same_topology_encrypted_main(self, cluster):
        self.execute(cluster, MAIN_NODE_SLICE_INFO, BURST_NODE_SLICE_INFO,
                     MAIN_BURST_SLICE_MAPPING, encrypted_main=True,
                     remote_access=False)

    def test_burst_refresh_same_topology_unencrypted_main(self, cluster):
        self.execute(cluster, MAIN_NODE_SLICE_INFO, BURST_NODE_SLICE_INFO,
                     MAIN_BURST_SLICE_MAPPING, encrypted_main=False,
                     remote_access=False)
