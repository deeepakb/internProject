# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.remote_exec_helpers import \
  burst_unified_remote_exec_gucs_main, burst_unified_remote_exec_gucs_burst
from test_burst_cluster_error_in_txn_no_retry import super_simulated_mode_method
from raff.burst.test_burst_copy_default_values_base import \
    TestBurstCopyWithDefaultValuesBase

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, super_simulated_mode_method]


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(
    gucs=dict(
        list(burst_unified_remote_exec_gucs_burst().items()) +
        [('burst_enable_write', 'true'), ('burst_enable_write_copy', 'true'),
         ('burst_enable_write_copy_default_value_column', 'true')]))
@pytest.mark.custom_local_gucs(
    gucs=dict(
        list(
            burst_unified_remote_exec_gucs_main(
                burst_use_ds_localization='true')
            .items()) + [('burst_enable_write', 'true'), (
                'burst_enable_write_copy', 'true'), (
                    'burst_enable_write_copy_default_value_column', 'true')]))
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.skip(reason="DP-65126")
class TestBurstCopyWithDefaultValuesUnifiedRemoteExec(
        TestBurstCopyWithDefaultValuesBase):
    def test_burst_write_copy_with_default_values_unified_remote_exec(
            self, cluster, db_session):
        self.base_test_burst_write_copy_with_default_values(
            cluster, db_session, unified_remote_exec=True)
