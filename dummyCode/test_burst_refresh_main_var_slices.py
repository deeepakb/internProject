# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest

from raff.burst.burst_refresh_test import BurstRefreshTest, restore_padb

log = logging.getLogger(__name__)

__all__ = [restore_padb]

# src/tools/slice_mappings/node_to_slice_map_2
NODE_TO_SLICE_MAP = {
    0: [0, 3, 2],
    1: [1],
    2: [5, 4]
}
MAIN_NODE_SLICE_INFO = dict(
    main_size=None,
    main_slices_per_node=None,
    main_node_slice_map=NODE_TO_SLICE_MAP
)
BURST_NODE_SLICE_INFO = dict(
    burst_size=4,
    burst_slices_per_node=2
)
# global slice number mapping for distall tables from Burst to Main
DISTALL_SLICE_MAP = {
    0: 0,
    2: 0,
    4: 5,
    6: 5
}
# global slice number mapping for non-distall tables from Burst to Main
NON_DISTALL_SLICE_MAP = {
    0: 0,
    1: 3,
    2: 2,
    3: 1,
    4: 5,
    6: 4
}
MAIN_BURST_SLICE_MAPPING = dict(
    distall=DISTALL_SLICE_MAP,
    non_distall=NON_DISTALL_SLICE_MAP
)


@pytest.mark.usefixtures("restore_padb")
@pytest.mark.serial_only
class TestBurstRefreshMainVarSlices(BurstRefreshTest):
    '''
    Test Burst refresh if slices in main cluster vary.

    Main:  src/tools/slice_mappings/node_to_slice_map_2
    Burst: 4 nodes x 2 slices/node
    '''
    def test_burst_refresh_main_var_slices_encrypted_main(self,
                                                          cluster,
                                                          db_session):
        self.execute(cluster, MAIN_NODE_SLICE_INFO, BURST_NODE_SLICE_INFO,
                     MAIN_BURST_SLICE_MAPPING, encrypted_main=True,
                     remote_access=False)

    def test_burst_refresh_main_var_slices_unencrypted_main(self,
                                                            cluster,
                                                            db_session):
        self.execute(cluster, MAIN_NODE_SLICE_INFO, BURST_NODE_SLICE_INFO,
                     MAIN_BURST_SLICE_MAPPING, encrypted_main=False,
                     remote_access=False)
