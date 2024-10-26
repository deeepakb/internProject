# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
import raff.burst.remote_exec_helpers as helpers
from raff.burst.test_burst_write_mixed_iduc import TestBurstWriteMixedIDUCBase
from raff.common.dimensions import Dimensions

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]


@pytest.mark.encrypted_only
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
class TestBurstWriteMixedIDUCUnifiedExec(TestBurstWriteMixedIDUCBase):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['diststyle even', 'distkey(c0)'],
                sortkey=['', 'sortkey(c0)'],
                size=['pristine', 'small', 'large']))

    def test_burst_write_mixed_iduc_without_txn(self, cluster, vector):
        """
        Test: test mixed insert, delete, update and copy on
        the same table without txn.
        """
        self.is_unified_remote_exec = True
        self._test_burst_write_mixed_iduc_without_txn(
            cluster, vector.diststyle, vector.sortkey, vector.size,
            large_tbl_load_cnt=3, is_temp=False, skip_copy_stmt=True)

    def test_burst_write_mixed_iduc_within_txn(self, cluster, vector):
        """
        Test: test mixed insert, delete, update and copy on
        the same table with txn, commit and abort.
        """
        self.is_unified_remote_exec = True
        # We currently skip burst of copy in unified remote exec as the
        # path is unsupported.
        self._test_burst_write_mixed_iduc_within_txn(
            cluster, vector.diststyle, vector.sortkey, vector.size,
            large_tbl_load_cnt=3, is_temp=False, skip_copy_stmt=True)
