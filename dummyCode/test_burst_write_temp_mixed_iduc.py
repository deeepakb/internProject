# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_temp_write import burst_user_temp_support_gucs
from raff.burst.test_burst_write_mixed_iduc import TestBurstWriteMixedIDUCBase
from raff.common.dimensions import Dimensions

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.ssm_perm_or_temp_config
@pytest.mark.custom_burst_gucs(
    gucs=dict(list(burst_user_temp_support_gucs.items()) +
              [('slices_per_node', '3'), ('burst_enable_write', 'true'),
               ('vacuum_auto_worker_enable', 'false')]))
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteTempMixedIDUC(TestBurstWriteMixedIDUCBase):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['diststyle even', 'distkey(c0)'],
                sortkey=['', 'sortkey(c0)'],
                size=['pristine', 'small', 'large']))

    def test_burst_write_temp_mixed_iduc_without_txn(
            self, cluster, vector, is_temp):
        """
        Test: test mixed insert, delete, update and copy on
        the same table without txn.
        """
        self._test_burst_write_mixed_iduc_without_txn(
            cluster, vector.diststyle, vector.sortkey, vector.size,
            large_tbl_load_cnt=3, is_temp=is_temp)

    def test_burst_write_temp_mixed_iduc_within_txn(
            self, cluster, vector, is_temp):
        """
        Test: test mixed insert, delete, update and copy on
        the same table with txn, commit and abort.
        """
        self._test_burst_write_mixed_iduc_within_txn(
            cluster, vector.diststyle, vector.sortkey, vector.size,
            large_tbl_load_cnt=3, is_temp=is_temp)
