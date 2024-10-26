# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
import raff.burst.remote_exec_helpers as helpers
from raff.burst.test_burst_idu_ss import BurstInsertDeleteUpdateSSBase
from raff.common.dimensions import Dimensions

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.custom_burst_gucs(
    gucs=dict(helpers.burst_unified_remote_exec_gucs_burst(slices_per_node=3)))
@pytest.mark.custom_local_gucs(
    gucs=dict(helpers.burst_unified_remote_exec_gucs_main()))
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstInsertDeleteUpdateUnifiedExecSS(BurstInsertDeleteUpdateSSBase):
    '''
    This test validates executing Insert/Update/Delete statements on a burst
    cluster where unified remote execution is enabled.
    '''
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['distkey'], sortkey=['sortkey']))

    def test_burst_insert_ss_mode(self, cluster, vector):
        self._test_burst_insert_ss_mode(
            cluster, vector.diststyle, vector.sortkey)

    def test_burst_delete_ss_mode(self, cluster, vector):
        self._test_burst_delete_ss_mode(
            cluster, vector.diststyle, vector.sortkey)

    def test_burst_update_ss_mode(self, cluster, vector):
        self._test_burst_update_ss_mode(
            cluster, vector.diststyle, vector.sortkey)
