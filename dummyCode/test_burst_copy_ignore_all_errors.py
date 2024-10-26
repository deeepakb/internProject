# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import os

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.ingestion.ingestion_test import S3CopySuite
from raff.burst.burst_temp_write import (BurstTempWrite, burst_user_temp_support_gucs)
from raff.burst.remote_exec_helpers import \
  burst_unified_remote_exec_gucs_main, burst_unified_remote_exec_gucs_burst

__all__ = [super_simulated_mode]


class BaseBurstCopyIgnoreAllErrors(BurstTempWrite, S3CopySuite):
    @property
    def testfiles_dir(self):
        return os.path.join(self.TEST_ROOT_DIR, 'burst', 'testfiles')

    def _test_burst_copy_ignore_all_errors(self, cluster, db_session, is_temp,
                                           unified_remote_exec):
        """
        This test validates IGNOREALLERRORS keyword usage in Burst Copy
        codepath.
        """
        setup_file = "test_burst_copy_ignore_all_errors_setup" if not is_temp \
            else "test_burst_temp_copy_ignore_all_errors_setup"
        # The exec file contains queries attempting to run copy queries expected
        # to error out, which is burstable on temp but not perm. Therefore, the
        # expected number of burstable queries is higher (6) on temp as compared
        # to perm (2)
        if is_temp or unified_remote_exec:
            expected_burst = 6
        else:
            expected_burst = 2

        self._run_and_compare_yaml_test(
            cluster,
            db_session,
            setup_file,
            "test_burst_copy_ignore_all_errors_exec",
            expected_burst,
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
class TestBurstCopyIgnoreAllErrors(BaseBurstCopyIgnoreAllErrors):
    def test_burst_copy_ignore_all_errors(self, cluster, db_session, is_temp):
        self._test_burst_copy_ignore_all_errors(cluster, db_session, is_temp,
                                                False)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=burst_unified_remote_exec_gucs_burst())
@pytest.mark.custom_local_gucs(
    gucs=burst_unified_remote_exec_gucs_main(burst_use_ds_localization='true'))
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstCopyIgnoreAllErrorsUnifiedRemoteExec(
        BaseBurstCopyIgnoreAllErrors):
    def test_burst_copy_ignore_all_errors_unified_remote_exec(
            self, cluster, db_session):
        self._test_burst_copy_ignore_all_errors(
            cluster, db_session, is_temp=False, unified_remote_exec=True)
