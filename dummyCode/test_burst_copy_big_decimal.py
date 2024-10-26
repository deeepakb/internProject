# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import os
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.ingestion.ingestion_test import S3CopySuite
from raff.burst.burst_temp_write import BurstTempWrite
from raff.burst.remote_exec_helpers import \
  burst_unified_remote_exec_gucs_main, burst_unified_remote_exec_gucs_burst
from raff.burst.burst_temp_write import burst_user_temp_support_gucs

__all__ = [super_simulated_mode]


class BaseBurstCopyBigDecimal(BurstTempWrite, S3CopySuite):
    @property
    def testfiles_dir(self):
        return os.path.join(self.TEST_ROOT_DIR, 'burst', 'testfiles')

    def _test_burst_copy_big_decimal(self, cluster, db_session, is_temp,
                                     unified_remote_exec):
        """
        This test is for an issue for copying data on big decimal column.
        https://issues.amazon.com/Burst-3419
        """
        setup_file = "test_burst_copy_big_decimal_setup" if not is_temp \
            else "test_burst_temp_copy_big_decimal_setup"

        self._run_and_compare_yaml_test(
            cluster,
            db_session,
            setup_file,
            "test_burst_copy_big_decimal_exec",
            2,
            vector=[],
            unified_remote_exec=unified_remote_exec)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.ssm_perm_or_temp_config
@pytest.mark.custom_burst_gucs(gucs={
    'slices_per_node': '3',
    'burst_enable_write': 'true'
})
@pytest.mark.custom_local_gucs(
    gucs={**burst_user_temp_support_gucs, 'burst_use_ds_localization': 'false'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstCopyBigDecimal(BaseBurstCopyBigDecimal):
    def test_burst_copy_big_decimal(self, cluster, db_session, is_temp):
        self._test_burst_copy_big_decimal(cluster, db_session, is_temp, False)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=burst_unified_remote_exec_gucs_burst())
@pytest.mark.custom_local_gucs(
    gucs=burst_unified_remote_exec_gucs_main(burst_use_ds_localization='true'))
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstCopyBigDecimalUnifiedRemoteExec(BaseBurstCopyBigDecimal):
    def test_burst_copy_big_decimal_unified_remote_exec(
            self, cluster, db_session):
        self._test_burst_copy_big_decimal(
            cluster, db_session, is_temp=False, unified_remote_exec=True)
