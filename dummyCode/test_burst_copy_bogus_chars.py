import logging
import pytest
import re

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.cred_helper import get_key_auth_str
from raff.common.db.db_exception import Error
from raff.common.profile import Profiles
from raff.ingestion.ingestion_test import S3CopySuite
from raff.burst.burst_temp_write import BurstTempWrite
from raff.burst.remote_exec_helpers import \
  burst_unified_remote_exec_gucs_main, burst_unified_remote_exec_gucs_burst

__all__ = [super_simulated_mode]
log = logging.getLogger(__name__)
DEFAULT_S3_PATH = "smoketests"
S3_BUCKET = "awscm-users-use1"


class BaseBurstCopyS3BogusChars(BurstTempWrite, S3CopySuite):
    cred_mapping = get_key_auth_str(Profiles.get_by_name('cookie-core'))
    tbl_name = "customer_table_to_fail_copy"

    def _init_table(self, cluster, cursor, table_type):
        columns = ("id int, age int, name varchar(64) , "
                   "creation_date varchar(64), tv_show varchar(32), "
                   "hobbies varchar(32)")
        attr = "diststyle even"
        cursor.execute(
            self.create_table_cmd(self.tbl_name, columns, attr, table_type))
        self._start_and_wait_for_refresh(cluster)

    def _test_burst_copy_bogus_chars_in_json(self, cluster, db_session,
                                             is_temp, unified_remote_exec):
        """
        Test-0: Burst copy with json that has bogus characters.
        """
        with db_session.cursor() as cursor:
            table_type = 'temp' if is_temp else ''
            self._init_table(cluster, cursor, table_type)
            error_msg = r".*Manifest file is not in correct json format"
            with pytest.raises(Error) as excep:
                source = self.data_source_str(
                    "general.csv", s3bucket=S3_BUCKET, s3path=DEFAULT_S3_PATH)
                cursor.execute("select now();")
                starttime = cursor.fetch_scalar()
                cursor.run_copy(
                    self.tbl_name,
                    source,
                    self.cred_mapping,
                    JSON='s3://awscm-users-use1/smoketests'
                    '/something%s%p%x%d%n.txt')
                self._check_last_copy_bursted(cluster, cursor)
                if unified_remote_exec:
                    assert self.get_ds_localization_q_count(
                        cluster, starttime) == 1
                assert re.match(error_msg, str(excep.value))
            self.assert_db_is_up(cursor)

    def _test_burst_copy_bogus_chars_in_manifest(self, cluster, db_session,
                                                 is_temp, unified_remote_exec):
        """
        Test-1: Burst copy with manifest that has bogus characters.
        """
        with db_session.cursor() as cursor:
            table_type = 'temp' if is_temp else ''
            self._init_table(cluster, cursor, table_type)
            error_msg = r".*Manifest file is not in correct json format"
            with pytest.raises(Error) as excep:
                source = self.data_source_str(
                    "something%s%p%x%d%n.txt",
                    s3bucket=S3_BUCKET,
                    s3path=DEFAULT_S3_PATH)
                cursor.execute("select now();")
                starttime = cursor.fetch_scalar()
                cursor.run_copy(
                    self.tbl_name,
                    source,
                    self.cred_mapping,
                    MANIFEST=True,
                    CSV=True)
                self._check_last_copy_bursted(cluster, cursor)
                if unified_remote_exec:
                    assert self.get_ds_localization_q_count(
                        cluster, starttime) == 1
                assert re.match(error_msg, str(excep.value))
            self.assert_db_is_up(cursor)

    def _test_burst_copy_bogus_chars_in_filename(self, cluster, db_session,
                                                 is_temp, unified_remote_exec):
        """
        Test-2: Burst copy with file that has bogus characters.
        """
        with db_session.cursor() as cursor:
            table_type = 'temp' if is_temp else ''
            self._init_table(cluster, cursor, table_type)
            error_msg = (r".*Problem reading manifest file - "
                         "S3ServiceException:The specified key does not "
                         "exist.*")
            with pytest.raises(Error) as excep:
                source = self.data_source_str(
                    "nonexisting-something%s%p%x%d%n.txt",
                    s3bucket=S3_BUCKET,
                    s3path=DEFAULT_S3_PATH)
                cursor.execute("select now();")
                starttime = cursor.fetch_scalar()
                cursor.run_copy(
                    self.tbl_name,
                    source,
                    self.cred_mapping,
                    MANIFEST=True,
                    CSV=True)
                self._check_last_copy_bursted(cluster, cursor)
                if unified_remote_exec:
                    assert self.get_ds_localization_q_count(
                        cluster, starttime) == 1
                assert re.match(error_msg, str(excep.value))
            self.assert_db_is_up(cursor)

    def _test_burst_copy_bogus_jsonobject_in_manifest(
            self, cluster, db_session, is_temp, unified_remote_exec):
        """
        Test-3: Burst copy with manifest that has bogus json object.
        """
        with db_session.cursor() as cursor:
            table_type = 'temp' if is_temp else ''
            self._init_table(cluster, cursor, table_type)
            error_msg = r".*Root must be a json object in the manifest file"
            with pytest.raises(Error) as excep:
                source = self.data_source_str(
                    "something%s%p%x%d%n.txt2",
                    s3bucket=S3_BUCKET,
                    s3path=DEFAULT_S3_PATH)
                cursor.execute("select now();")
                starttime = cursor.fetch_scalar()
                cursor.run_copy(
                    self.tbl_name,
                    source,
                    self.cred_mapping,
                    MANIFEST=True,
                    CSV=True)
                self._check_last_copy_bursted(cluster, cursor)
                if unified_remote_exec:
                    assert self.get_ds_localization_q_count(
                        cluster, starttime) == 1
                assert re.match(error_msg, str(excep.value))
            self.assert_db_is_up(cursor)

    def _test_burst_copy_bogus_jsonobject_in_missing_url(
            self, cluster, db_session, is_temp, unified_remote_exec):
        """
        Test-4: Burst copy with manifest file that contains an entry with a
                missing url file.
        """
        with db_session.cursor() as cursor:
            table_type = 'temp' if is_temp else ''
            self._init_table(cluster, cursor, table_type)
            error_msg = (r".*Manifest file is not in expected format - 'url' "
                         "field missing. Please check the user documentation "
                         "for the correct format of manifest file during copy "
                         "command.")
            with pytest.raises(Error) as excep:
                source = self.data_source_str(
                    "something%s%p%x%d%n.txt3",
                    s3bucket=S3_BUCKET,
                    s3path=DEFAULT_S3_PATH)
                cursor.execute("select now();")
                starttime = cursor.fetch_scalar()
                cursor.run_copy(
                    self.tbl_name,
                    source,
                    self.cred_mapping,
                    MANIFEST=True,
                    CSV=True)
                self._check_last_copy_bursted(cluster, cursor)
                if unified_remote_exec:
                    assert self.get_ds_localization_q_count(
                        cluster, starttime) == 1
                assert re.match(error_msg, str(excep.value))
            self.assert_db_is_up(cursor)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.ssm_perm_or_temp_config
@pytest.mark.custom_burst_gucs(gucs={'burst_enable_write': 'true'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstCopyS3BogusChars(BaseBurstCopyS3BogusChars):
    def test_burst_copy_bogus_chars_in_json(
            self, cluster, db_session, is_temp):
        self._test_burst_copy_bogus_chars_in_json(
            cluster, db_session, is_temp, unified_remote_exec=False)

    def test_burst_copy_bogus_chars_in_manifest(
            self, cluster, db_session, is_temp):
        self._test_burst_copy_bogus_chars_in_manifest(
            cluster, db_session, is_temp, unified_remote_exec=False)

    def test_burst_copy_bogus_chars_in_filename(
            self, cluster, db_session, is_temp):
        self._test_burst_copy_bogus_chars_in_filename(
            cluster, db_session, is_temp, unified_remote_exec=False)

    def test_burst_copy_bogus_jsonobject_in_manifest(
            self, cluster, db_session, is_temp):
        self._test_burst_copy_bogus_jsonobject_in_manifest(
            cluster, db_session, is_temp, unified_remote_exec=False)

    def test_burst_copy_bogus_jsonobject_in_missing_url(
            self, cluster, db_session, is_temp):
        self._test_burst_copy_bogus_jsonobject_in_missing_url(
            cluster, db_session, is_temp, unified_remote_exec=False)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(gucs=burst_unified_remote_exec_gucs_burst())
@pytest.mark.custom_local_gucs(
    gucs=burst_unified_remote_exec_gucs_main(burst_use_ds_localization='true'))
class TestBurstCopyS3BogusCharsUnifiedRemoteExec(BaseBurstCopyS3BogusChars):
    def test_burst_copy_bogus_chars_in_json_unified_remote_exec(
            self, cluster, db_session):
        self._test_burst_copy_bogus_chars_in_json(
            cluster, db_session, is_temp=False, unified_remote_exec=True)

    def test_burst_copy_bogus_chars_in_manifest_unified_remote_exec(
            self, cluster, db_session):
        self._test_burst_copy_bogus_chars_in_manifest(
            cluster, db_session, is_temp=False, unified_remote_exec=True)

    def test_burst_copy_bogus_chars_in_filename_unified_remote_exec(
            self, cluster, db_session):
        self._test_burst_copy_bogus_chars_in_filename(
            cluster, db_session, is_temp=False, unified_remote_exec=True)

    def test_burst_copy_bogus_jsonobject_in_manifest_unified_remote_exec(
            self, cluster, db_session):
        self._test_burst_copy_bogus_jsonobject_in_manifest(
            cluster, db_session, is_temp=False, unified_remote_exec=True)

    def test_burst_copy_bogus_jsonobject_in_missing_url_unified_remote_exec(
            self, cluster, db_session):
        self._test_burst_copy_bogus_jsonobject_in_missing_url(
            cluster, db_session, is_temp=False, unified_remote_exec=True)
