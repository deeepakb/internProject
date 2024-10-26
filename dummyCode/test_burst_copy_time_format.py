# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import os
from copy import deepcopy

from raff.common.dimensions import Dimensions
from raff.burst.burst_super_simulated_mode_helper \
  import super_simulated_mode, super_simulated_mode_method
from raff.ingestion.ingestion_test import S3CopySuite
from raff.burst.burst_temp_write import (BurstTempWrite, burst_user_temp_support_gucs)
from raff.burst.remote_exec_helpers import \
  burst_unified_remote_exec_gucs_main, burst_unified_remote_exec_gucs_burst

__all__ = [super_simulated_mode, super_simulated_mode_method]


class BaseBurstCopyTimeFormat(BurstTempWrite, S3CopySuite):
    @property
    def testfiles_dir(self):
        return os.path.join(self.TEST_ROOT_DIR, 'burst', 'testfiles')

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['distkey(start_timestamp)', 'diststyle even'],
                sortkey=['', 'sortkey(start_timestamp)']))

    def _transform_test(self, test, vector):
        result = deepcopy(test)
        result.query.sql = test.query.sql.format(
            diststyle=vector.diststyle, sortkey=vector.sortkey)
        return result

    def _test_burst_copy_timeformat(self, cluster, db_session, vector, is_temp,
                                    unified_remote_exec):
        """
        Test-0: Burst copy with different TIMEFORMAT options.
        """
        setup_file = "test_burst_copy_timeformat_setup" if not is_temp \
            else "test_burst_temp_copy_timeformat_setup"
        # The exec file contains queries attempting to run on burst after an
        # errored out copy without refresh, which is burstable on temp or
        # running under unified remote execution but not perm. That's why the
        # expected number of bursted queries are more for temp table or running
        # under unified remote execution.
        expected_burst = 81 if is_temp or unified_remote_exec else 66
        self._run_and_compare_yaml_test(
            cluster,
            db_session,
            setup_file,
            "test_burst_copy_timeformat_exec",
            expected_burst,
            vector=vector,
            unified_remote_exec=unified_remote_exec)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.ssm_perm_or_temp_config
@pytest.mark.custom_local_gucs(
    gucs={**burst_user_temp_support_gucs, 'burst_use_ds_localization': 'false'})
@pytest.mark.custom_burst_gucs(gucs={'burst_enable_write': 'true'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstCopyTimeFormat(BaseBurstCopyTimeFormat):
    def test_burst_copy_timeformat(self, cluster, db_session, vector, is_temp):
        self._test_burst_copy_timeformat(cluster, db_session, vector, is_temp,
                                         False)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=burst_unified_remote_exec_gucs_burst())
@pytest.mark.custom_local_gucs(
    gucs=burst_unified_remote_exec_gucs_main(burst_use_ds_localization='true'))
class TestBurstCopyTimeFormatUnifiedRemoteExec(BaseBurstCopyTimeFormat):
    @pytest.mark.usefixtures("super_simulated_mode_method")
    def test_burst_copy_timeformat_unified_remote_exec(self, cluster,
                                                       db_session, vector):
        self._test_burst_copy_timeformat(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)
