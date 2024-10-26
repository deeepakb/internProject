# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import burst_write_mv_gucs
from raff.burst.burst_write import burst_write_basic_gucs
from raff.burst.burst_test import setup_teardown_burst
from test_burst_mv_ddl_tx import TestBurstWriteMVDDLBase
from test_burst_mv_ddl_tx import dimensions_txn_part2

__all__ = ["super_simulated_mode", "setup_teardown_burst"]


@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(
    gucs=list(burst_write_basic_gucs.items()) + [('slices_per_node', '3')])
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.serial_only
@pytest.mark.skip_load_data
class TestBurstWriteMVDDLSSPart2(TestBurstWriteMVDDLBase):
    @classmethod
    def modify_test_dimensions(cls):
        return dimensions_txn_part2

    def test_burst_mv_ddl_with_txn_ss(self, cluster, vector):
        self.base_test_burst_mv_ddl(cluster, vector)
