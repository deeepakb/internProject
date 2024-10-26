# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import os
import pytest
from copy import deepcopy
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.dimensions import Dimensions
from raff.ingestion.ingestion_test import S3CopySuite
from raff.burst.burst_temp_write import (BurstTempWrite, burst_user_temp_support_gucs)
from raff.burst.remote_exec_helpers import \
  burst_unified_remote_exec_gucs_main, burst_unified_remote_exec_gucs_burst

__all__ = [super_simulated_mode]


class BaseBurstCopyUtfEncode(BurstTempWrite, S3CopySuite):
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
        secret_name = "ingestion-master-symmetric-key"
        symmetric_key = S3CopySuite.get_secret(secret_name)
        result = deepcopy(test)
        result.query.sql = test.query.sql.format(
            diststyle=vector.diststyle, sortkey=vector.sortkey,
            symmetric_key=symmetric_key)
        return result

    def _test_burst_copy_csv_encode(self, cluster, db_session, vector, is_temp,
                                    unified_remote_exec):
        """
        Test-0: Burst copy from CSV file with UTF ENCODING option.
        """
        setup_file = "test_burst_copy_csv_encode_setup" if not is_temp \
            else "test_burst_temp_copy_csv_encode_setup"
        # The exec file contains queries attempting to run on burst after an
        # errored out copy without refresh, which is burstable on temp or
        # running under unified remote execution but not perm. That's why the
        # expected number of bursted queries are more for temp table or running
        # under unified remote execution.
        expected_burst = 50 if is_temp or unified_remote_exec else 33
        self._run_and_compare_yaml_test(
            cluster,
            db_session,
            setup_file,
            "test_burst_copy_csv_encode_exec",
            expected_burst,
            vector=vector,
            unified_remote_exec=unified_remote_exec)

    def _test_burst_copy_json_encode(self, cluster, db_session, vector,
                                     is_temp, unified_remote_exec):
        """
        Test-1: Burst copy from JSON file with UTF ENCODING option.
        """
        setup_file = "test_burst_copy_json_encode_setup" if not is_temp \
            else "test_burst_temp_copy_json_encode_setup"
        # The exec file contains queries attempting to run on burst after an
        # errored out copy without refresh, which is burstable on temp or
        # running under unified remote execution but not perm. That's why the
        # expected number of bursted queries are more for temp table or running
        # under unified remote execution.
        expected_burst = 44 if is_temp or unified_remote_exec else 31
        self._run_and_compare_yaml_test(
            cluster,
            db_session,
            setup_file,
            "test_burst_copy_json_encode_exec",
            expected_burst,
            vector=vector,
            unified_remote_exec=unified_remote_exec)

    def _test_burst_copy_accept_invchars(self, cluster, db_session, vector,
                                         is_temp, unified_remote_exec):
        """
        Test-2: Burst copy with ACCEPTINVCHARS option.
        """
        setup_file = "test_burst_copy_accept_invchars_setup" if not is_temp \
            else "test_burst_temp_copy_accept_invchars_setup"

        self._run_and_compare_yaml_test(
            cluster,
            db_session,
            setup_file,
            "test_burst_copy_accept_invchars_exec",
            10,
            vector=vector,
            unified_remote_exec=unified_remote_exec)

    def _test_burst_copy_blank_as_null(self, cluster, db_session, vector,
                                       is_temp, unified_remote_exec):
        """
        Test-3: Burst copy with BLANKSASNULL option.
        """
        setup_file = "test_burst_copy_blank_as_null_setup" if not is_temp \
            else "test_burst_temp_copy_blank_as_null_setup"

        self._run_and_compare_yaml_test(
            cluster,
            db_session,
            setup_file,
            "test_burst_copy_blank_as_null_exec",
            8,
            vector=vector,
            unified_remote_exec=unified_remote_exec)

    def _test_burst_copy_accept_any_date(self, cluster, db_session, vector,
                                         is_temp, unified_remote_exec):
        """
        Test-4: Burst copy with ACCEPTANYDATE option.
        """
        setup_file = "test_burst_copy_accept_any_date_setup" if not is_temp \
            else "test_burst_temp_copy_accept_any_date_setup"
        # The exec file contains queries attempting to run on burst after an
        # errored out copy without refresh, which is burstable on temp or
        # running under unified remote execution but not perm. That's why the
        # expected number of bursted queries are more for temp table or running
        # under unified remote execution.
        expected_burst = 6 if is_temp or unified_remote_exec else 5
        self._run_and_compare_yaml_test(
            cluster,
            db_session,
            setup_file,
            "test_burst_copy_accept_any_date_exec",
            expected_burst,
            vector=vector,
            unified_remote_exec=unified_remote_exec)

    @pytest.mark.skip(reason="rsqa-9907")
    def _test_burst_copy_compression(self, cluster, db_session, vector,
                                     is_temp, unified_remote_exec):
        """
        Test-5: Burst copy with raw, gzip, lzop, bzip2 compressed data and
                ROUNDEC option.
        """
        setup_file = "test_burst_copy_compression_setup" if not is_temp \
            else "test_burst_temp_copy_compression_setup"

        self._run_and_compare_yaml_test(
            cluster,
            db_session,
            setup_file,
            "test_burst_copy_compression_exec",
            36,
            vector=vector,
            unified_remote_exec=unified_remote_exec)

    def _test_burst_copy_zstd(self, cluster, db_session, vector, is_temp,
                              unified_remote_exec):
        """
        Test-6: Burst copy with zstd compressed data
        """
        setup_file = "test_burst_copy_zstd_setup" if not is_temp \
            else "test_burst_temp_copy_zstd_setup"
        # The exec file contains queries attempting to run on burst after an
        # errored out copy without refresh, which is burstable on temp or
        # running under unified remote execution but not perm. That's why the
        # expected number of bursted queries are more for temp table or running
        # under unified remote execution.
        if is_temp:
            expected_burst = 24
        elif unified_remote_exec:
            expected_burst = 23
        else:
            expected_burst = 11
        self._run_and_compare_yaml_test(
            cluster,
            db_session,
            setup_file,
            "test_burst_copy_zstd_exec",
            expected_burst,
            vector=vector,
            unified_remote_exec=unified_remote_exec)

    def _test_burst_copy_zstd_encrypt(self, cluster, db_session, vector,
                                      is_temp, unified_remote_exec):
        """
        Test-7: Burst COPY encrypted ZSTD data.
        1) SSE-KMS encrypted zstd file.
        2) SSE-S3 (AES256) encrypted zstd file.
        2) CSE using a client-side symmetric master key encrypted zstd file.
        3) Try to decrypt an un-encrypted file using CSE -- fail.
        4) Invalid SYMMETRIC_MASTER_KEY -- fail.
        5) Invalid cse encrypted object metadata on s3 -- fail.
        6) Truncated cse encrypted object -- fail.
        """

        setup_file = "test_burst_copy_zstd_encrypt_setup" if not is_temp \
            else "test_burst_temp_copy_zstd_encrypt_setup"
        if is_temp:
            expected_burst = 11
        elif unified_remote_exec:
            expected_burst = 13
        else:
            expected_burst = 7
        self._run_and_compare_yaml_test(
            cluster,
            db_session,
            setup_file,
            "test_burst_copy_zstd_encrypt_exec",
            expected_burst,
            vector=vector,
            unified_remote_exec=unified_remote_exec)

    def _test_burst_copy_all_column_types(self, cluster, db_session, vector,
                                          is_temp, unified_remote_exec):
        """
        Test-8: Burst COPY on all data type FIXEDWIDTH, REMOVEQUOTES,
                TIMEFORMAT, and DATEFORMAT.
        """
        setup_file = "test_burst_copy_all_column_types_setup" if not is_temp \
            else "test_burst_temp_copy_all_column_types_setup"

        self._run_and_compare_yaml_test(
            cluster,
            db_session,
            setup_file,
            "test_burst_copy_all_column_types_exec",
            28,
            vector=vector,
            unified_remote_exec=unified_remote_exec)

    def _test_burst_copy_with_column_mapping(self, cluster, db_session, vector,
                                             is_temp, unified_remote_exec):
        """
        Test copy with all column types on column mapping options. Verify the
        result is the same while running on main.
        """
        setup_file = "test_burst_column_mapping_setup" if not is_temp \
            else "test_burst_temp_column_mapping_setup"

        self._run_and_compare_yaml_test(
            cluster,
            db_session,
            setup_file,
            "test_burst_column_mapping_exec",
            num_bursts=22,
            vector=vector,
            unified_remote_exec=unified_remote_exec)
        conn_params = cluster.get_conn_params()
        session_ctx = SessionContext(user_type="bootstrap")
        bootstrap_db_session = DbSession(conn_params, session_ctx=session_ctx)
        self.execute_test_file(
            setup_file, session=bootstrap_db_session, vector=vector)
        self.execute_test_file(
            "test_burst_column_mapping_exec",
            session=bootstrap_db_session,
            vector=vector)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.ssm_perm_or_temp_config
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '3',
        'burst_enable_write': 'true',
        'copy_max_record_size_mb': '4'
    })
@pytest.mark.custom_local_gucs(
    gucs={**burst_user_temp_support_gucs, 'burst_use_ds_localization': 'false'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstCopyUtfEncode(BaseBurstCopyUtfEncode):
    def test_burst_copy_csv_encode(self, cluster, db_session, vector, is_temp):
        self._test_burst_copy_csv_encode(cluster, db_session, vector, is_temp,
                                         False)

    def test_burst_copy_json_encode(self, cluster, db_session, vector,
                                    is_temp):
        self._test_burst_copy_json_encode(cluster, db_session, vector, is_temp,
                                          False)

    def test_burst_copy_accept_invchars(self, cluster, db_session, vector,
                                        is_temp):
        self._test_burst_copy_accept_invchars(cluster, db_session, vector,
                                              is_temp, False)

    def test_burst_copy_blank_as_null(self, cluster, db_session, vector,
                                      is_temp):
        self._test_burst_copy_blank_as_null(cluster, db_session, vector,
                                            is_temp, False)

    def test_burst_copy_accept_any_date(self, cluster, db_session, vector,
                                        is_temp):
        self._test_burst_copy_accept_any_date(cluster, db_session, vector,
                                              is_temp, False)

    @pytest.mark.skip(reason="rsqa-9907")
    def test_burst_copy_compression(self, cluster, db_session, vector,
                                    is_temp):
        self._test_burst_copy_compression(cluster, db_session, vector, is_temp,
                                          False)

    def test_burst_copy_zstd(self, cluster, db_session, vector, is_temp):
        self._test_burst_copy_zstd(cluster, db_session, vector, is_temp, False)

    def test_burst_copy_zstd_encrypt(self, cluster, db_session, vector,
                                     is_temp):
        self._test_burst_copy_zstd_encrypt(cluster, db_session, vector,
                                           is_temp, False)

    def test_burst_copy_all_column_types(self, cluster, db_session, vector,
                                         is_temp):
        self._test_burst_copy_all_column_types(cluster, db_session, vector,
                                               is_temp, False)

    def test_burst_copy_with_column_mapping(self, cluster, db_session, vector,
                                            is_temp):
        self._test_burst_copy_with_column_mapping(cluster, db_session, vector,
                                                  is_temp, False)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(
    gucs={**burst_unified_remote_exec_gucs_burst(),
          'copy_max_record_size_mb': '4'})
@pytest.mark.custom_local_gucs(
    gucs=burst_unified_remote_exec_gucs_main(burst_use_ds_localization='true'))
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstCopyUtfEncodeUnifiedRemoteExec(BaseBurstCopyUtfEncode):
    def test_burst_copy_csv_encode_unified_remote_exec(self, cluster,
                                                       db_session, vector):
        self._test_burst_copy_csv_encode(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)

    def test_burst_copy_json_encode_unified_remote_exec(
            self, cluster, db_session, vector):
        self._test_burst_copy_json_encode(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)

    def test_burst_copy_accept_invchars_unified_remote_exec(
            self, cluster, db_session, vector):
        self._test_burst_copy_accept_invchars(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)

    def test_burst_copy_blank_as_null_unified_remote_exec(
            self, cluster, db_session, vector):
        self._test_burst_copy_blank_as_null(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)

    def test_burst_copy_accept_any_date_unified_remote_exec(
            self, cluster, db_session, vector):
        self._test_burst_copy_accept_any_date(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)

    @pytest.mark.skip(reason="rsqa-9907")
    def test_burst_copy_compression_unified_remote_exec(
            self, cluster, db_session, vector):
        self._test_burst_copy_compression(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)

    def test_burst_copy_zstd_unified_remote_exec(self, cluster, db_session,
                                                 vector):
        self._test_burst_copy_zstd(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)

    def test_burst_copy_zstd_encrypt_unified_remote_exec(
            self, cluster, db_session, vector):
        self._test_burst_copy_zstd_encrypt(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)

    def test_burst_copy_all_column_types_unified_remote_exec(
            self, cluster, db_session, vector):
        self._test_burst_copy_all_column_types(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)

    def test_burst_copy_with_column_mapping_unified_remote_exec(
            self, cluster, db_session, vector):
        self._test_burst_copy_with_column_mapping(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)
