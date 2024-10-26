# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_test import setup_teardown_burst
from raff.burst.burst_write import burst_write_basic_gucs
from raff.storage.storage_test import disable_all_autoworkers
from test_burst_mv_refresh import TestBurstWriteMVBase
log = logging.getLogger(__name__)
__all__ = [disable_all_autoworkers, super_simulated_mode, setup_teardown_burst]


@pytest.mark.no_burst_nightly
@pytest.mark.serial_only
@pytest.mark.skip_load_data
@pytest.mark.keep_burst
@pytest.mark.usefixtures("disable_all_autoworkers")
@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(
    gucs=list(burst_write_basic_gucs.items()) + [('slices_per_node', '3')])
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstStartDocker(TestBurstWriteMVBase):
    def test_burst_start_docker(self, cluster):
        log.info("Starting test_burst_start_docker")
        for _ in range(5):
            self.run_directly_on_burst(cluster, "xpx 'hello'")
        log.info("Ending test_burst_start_docker")
        return
