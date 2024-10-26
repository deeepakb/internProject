# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest

import raff.burst.remote_exec_helpers as helpers
from raff.burst.burst_write import burst_write_mv_gucs
from test_burst_mv_refresh import TestBurstWriteMVBase


@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=list(
    helpers.burst_unified_remote_exec_gucs_burst(slices_per_node=3).items())
                               + list(burst_write_mv_gucs.items()))
@pytest.mark.custom_local_gucs(
    gucs=list(helpers.burst_unified_remote_exec_gucs_main().items()) +
    [('burst_enable_write_copy', 'true')])
@pytest.mark.usefixtures("super_simulated_mode")
class TestUnifiedBurstMVRefresh(TestBurstWriteMVBase):
    def test_unified_burst_mv_refresh_ss(self, cluster):
        self.use_unified_burst = True
        self.base_test_burst_mv_refresh(cluster)
