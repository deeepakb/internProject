import os
import pytest
from copy import deepcopy

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.dimensions import Dimensions
from raff.ingestion.ingestion_test import S3CopySuite
from raff.burst.burst_temp_write import BurstTempWrite
from raff.burst.burst_temp_write import burst_user_temp_support_gucs
from raff.burst.remote_exec_helpers import \
  burst_unified_remote_exec_gucs_main, burst_unified_remote_exec_gucs_burst

__all__ = [super_simulated_mode]


class BaseBurstCopyCsvOptions(BurstTempWrite, S3CopySuite):
    @property
    def testfiles_dir(self):
        return os.path.join(self.TEST_ROOT_DIR, 'burst', 'testfiles')

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['distkey(c0)', 'diststyle even'],
                sortkey=['', 'sortkey(c0)']))

    def _transform_test(self, test, vector):
        result = deepcopy(test)
        result.query.sql = test.query.sql.format(
            diststyle=vector.diststyle,
            sortkey=vector.sortkey)
        return result

    def _test_burst_copy_option_one(self, cluster, db_session, vector, is_temp,
                                    unified_remote_exec):
        """
        Test-0: Burst copy with CSV, IGNOREHEADER, REMOVEQUOTES, GZIP,
                DELIMITER AS, and REMOVEQUOTES.
        """
        setup_file = "test_burst_copy_option_one_setup" if not is_temp \
            else "test_burst_temp_copy_option_one_setup"
        self._run_and_compare_yaml_test(
            cluster,
            db_session,
            setup_file,
            "test_burst_copy_option_one_exec",
            20,
            vector=vector,
            unified_remote_exec=unified_remote_exec)

    def _test_burst_copy_csv_quote_options(self, cluster, db_session, vector,
                                           is_temp, unified_remote_exec):
        """
        Test-1: Burst copy with CSV with QUOTE, NULL, IGNOREBLANKLINES,
                TRUNCATECOLUMNS, FILLRECORD, TRIMBLANKS, MAXERROR, REMOVEQUOTES.
        """
        setup_file = "test_burst_copy_csv_quote_options_setup" \
            if not is_temp \
            else "test_burst_temp_copy_csv_quote_options_setup"
        self._run_and_compare_yaml_test(
            cluster,
            db_session,
            setup_file,
            "test_burst_copy_csv_quote_options_exec",
            22,
            vector=vector,
            unified_remote_exec=unified_remote_exec)

    def _test_burst_copy_csv_quote_options_two(
            self, cluster, db_session, vector, is_temp, unified_remote_exec):
        """
        Test-2: Burst copy with CSV with EMPTYASNULL, ESCAPE, FILLRECORD,
                NULL AS, DELIMITER AS and CSV containing ERROR.
        """
        setup_file = "test_burst_copy_csv_quote_options_two_setup" \
            if not is_temp \
            else "test_burst_temp_copy_csv_quote_options_two_setup"
        self._run_and_compare_yaml_test(
            cluster,
            db_session,
            setup_file,
            "test_burst_copy_csv_quote_options_two_exec",
            11,
            vector=vector,
            unified_remote_exec=unified_remote_exec)

    def _test_burst_copy_csv_options_failure(self, cluster, db_session, vector,
                                             is_temp, unified_remote_exec):
        """
        Test-3: Burst copy with CSV that contains invalid options. Verify copy
                should fail.
        """
        setup_file = "test_burst_copy_csv_options_failure_setup" if not \
            is_temp else \
            "test_burst_temp_copy_csv_options_failure_setup"
        # The exec file contains queries attempting to run on burst after an
        # errored out copy without refresh, which is burstable on temp or
        # running under unified remote execution but not perm. That's why the
        # expected number of bursted queries are more for temp table or running
        # under unified remote execution.
        expected_burst = 30 if is_temp or unified_remote_exec else 20
        self._run_and_compare_yaml_test(
            cluster,
            db_session,
            setup_file,
            "test_burst_copy_csv_options_failure_exec",
            expected_burst,
            vector=vector,
            unified_remote_exec=unified_remote_exec)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.ssm_perm_or_temp_config
@pytest.mark.custom_burst_gucs(gucs={
    'slices_per_node': '4',
    'burst_enable_write': 'true'
})
@pytest.mark.custom_local_gucs(
    gucs={**burst_user_temp_support_gucs, 'burst_use_ds_localization': 'false'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstCopyCsvOptions(BaseBurstCopyCsvOptions):
    def test_burst_copy_option_one(self, cluster, db_session, vector, is_temp):
        self._test_burst_copy_option_one(cluster, db_session, vector, is_temp,
                                         False)

    def test_burst_copy_csv_quote_options(self, cluster, db_session, vector,
                                          is_temp):
        self._test_burst_copy_csv_quote_options(cluster, db_session, vector,
                                                is_temp, False)

    def test_burst_copy_csv_quote_options_two(self, cluster, db_session,
                                              is_temp, vector):
        self._test_burst_copy_csv_quote_options_two(cluster, db_session,
                                                    vector, is_temp, False)

    def test_burst_copy_csv_options_failure(self, cluster, db_session, vector,
                                            is_temp):
        self._test_burst_copy_csv_options_failure(cluster, db_session, vector,
                                                  is_temp, False)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(gucs=burst_unified_remote_exec_gucs_burst())
@pytest.mark.custom_local_gucs(
    gucs=burst_unified_remote_exec_gucs_main(burst_use_ds_localization='true'))
class TestBurstCopyCsvOptionsUnifiedRemoteExec(BaseBurstCopyCsvOptions):
    def test_burst_copy_option_one_unified_remote_exec(self, cluster,
                                                       db_session, vector):
        self._test_burst_copy_option_one(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)

    def test_burst_copy_csv_quote_options_unified_remote_exec(
            self, cluster, db_session, vector):
        self._test_burst_copy_csv_quote_options(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)

    def test_burst_copy_csv_quote_options_two_unified_remote_exec(
            self, cluster, db_session, vector):
        self._test_burst_copy_csv_quote_options_two(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)

    def test_burst_copy_csv_options_failure_unified_remote_exec(
            self, cluster, db_session, vector):
        self._test_burst_copy_csv_options_failure(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)
