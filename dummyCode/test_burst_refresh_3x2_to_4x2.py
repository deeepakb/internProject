# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest

from raff.burst.burst_refresh_test import BurstRefreshTest, restore_padb
from raff.s3commit.defaults import S3_MIRROR_GUC, S3_MIRROR_GUC_VALUES

log = logging.getLogger(__name__)

__all__ = [restore_padb]

# global slice number mapping for distall tables from Burst to Main
DISTALL_SLICE_MAP = {
    0: 0,
    2: 2,
    4: 4,
    6: 4
}
# global slice number mapping for non-distall tables from Burst to Main
NON_DISTALL_SLICE_MAP = {
    0: 0,
    1: 1,
    2: 2,
    3: 3,
    4: 4,
    6: 5
}
MAIN_NODE_SLICE_INFO = dict(
    main_size=3,
    main_slices_per_node=2,
    main_node_slice_map=None
)
BURST_NODE_SLICE_INFO = dict(
    burst_size=4,
    burst_slices_per_node=2
)
MAIN_BURST_SLICE_MAPPING = dict(
    distall=DISTALL_SLICE_MAP,
    non_distall=NON_DISTALL_SLICE_MAP
)


@pytest.mark.usefixtures("restore_padb")
@pytest.mark.serial_only
class TestBurstRefresh3x2To4x2(BurstRefreshTest):
    '''
    Test Burst refresh if Burst cluster has different topology with main
    cluster.

    Scenarios:
        Main:  3 nodes x 2 slices/node
        Burst: 4 nodes x 2 slices/node
    '''

    def test_burst_refresh_3x2_to_4x2_encrypted_main(self, cluster):
        self.execute(cluster, MAIN_NODE_SLICE_INFO, BURST_NODE_SLICE_INFO,
                     MAIN_BURST_SLICE_MAPPING, encrypted_main=True,
                     remote_access=False)

    def test_burst_refresh_3x2_to_4x2_unencrypted_main(self, cluster):
        self.execute(cluster, MAIN_NODE_SLICE_INFO, BURST_NODE_SLICE_INFO,
                     MAIN_BURST_SLICE_MAPPING, encrypted_main=False,
                     remote_access=False)

    @pytest.mark.skip(reason="https://sim.amazon.com/issues/RedshiftDP-25450")
    def test_remote_access_3x2_to_4x2_encrypted_main(self, cluster):
        if cluster.get_padb_conf_value(S3_MIRROR_GUC) == \
                str(S3_MIRROR_GUC_VALUES['enabled']):
            pytest.skip('Tests for Data Sharing are not compatible with S3 '
                        'Commit at this point')

        self.execute(cluster, MAIN_NODE_SLICE_INFO, BURST_NODE_SLICE_INFO,
                     MAIN_BURST_SLICE_MAPPING, encrypted_main=True,
                     remote_access=True)

    @pytest.mark.skip(reason="https://sim.amazon.com/issues/RedshiftDP-25450")
    def test_remote_access_3x2_to_4x2_unencrypted_main(self, cluster):
        if cluster.get_padb_conf_value(S3_MIRROR_GUC) == \
                str(S3_MIRROR_GUC_VALUES['enabled']):
            pytest.skip('Tests for Data Sharing are not compatible with S3 '
                        'Commit at this point')

        self.execute(cluster, MAIN_NODE_SLICE_INFO, BURST_NODE_SLICE_INFO,
                     MAIN_BURST_SLICE_MAPPING, encrypted_main=False,
                     remote_access=True)
