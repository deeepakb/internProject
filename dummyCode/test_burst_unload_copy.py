# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import os
import pytest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_temp_write import BurstTempWrite
from raff.common.dimensions import Dimensions
from raff.common.aws_clients.s3_client import S3Client
from raff.common.cred_helper import (
    get_key_auth_str,
    get_role_auth_str
    )
from raff.common.profile import Profiles
from raff.ingestion.ingestion_test import S3CopySuite
from raff.burst.burst_temp_write import burst_user_temp_support_gucs
from raff.burst.remote_exec_helpers import \
  burst_unified_remote_exec_gucs_main, burst_unified_remote_exec_gucs_burst

__all__ = [super_simulated_mode]
S3_BUCKET = "cookie-monster-test-s3-ingestion"


class BaseBurstCopyUnloadedData(BurstTempWrite, S3CopySuite):
    profile = Profiles.get_by_name('cookie-core')
    cred_mapping = {
        'default': get_key_auth_str(profile),
        "iamrole": get_role_auth_str()
    }

    @property
    def testfiles_dir(self):
        return os.path.join(self.TEST_ROOT_DIR, 'burst', 'testfiles')

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(dict(diststyle=['diststyle even', 'distkey(c0)']))

    def _init_empty_folder(self, cluster, cursor, name, unload_table,
                           diststyle, is_temp):
        columns = ("c0 int , name varchar(64) , email varchar(64),"
                   "creation_date date")
        table_type = 'temp' if is_temp else ''
        cursor.execute(
            self.create_table_cmd(name, columns, diststyle, temp=table_type))
        if not unload_table:
            cursor.execute(
                self.create_table_cmd(unload_table, columns, diststyle))
        self._start_and_wait_for_refresh(cluster)
        cursor.execute("set query_group to burst;")

    def _test_burst_load_empty_folder(self, cluster, db_session, vector,
                                      is_temp, unified_remote_exec):
        """
        Test-1: Burst copy from empty folder.
        """
        with db_session.cursor() as cursor:
            table_name = "empty_folder"
            self._init_empty_folder(cluster, cursor, table_name, None,
                                    vector.diststyle, is_temp)
            source = self.data_source_str(
                "", s3bucket=S3_BUCKET, s3path="empty-folder")
            cursor.execute("select now();")
            starttime = cursor.fetch_scalar()
            cursor.run_copy(
                table_name,
                source,
                self.cred_mapping['iamrole'],
                DELIMITER=",",
                COMPUPDATE='OFF')
            self._check_last_copy_bursted(cluster, cursor)
            if unified_remote_exec:
                assert self.get_ds_localization_q_count(cluster,
                                                        starttime) == 1
            cursor.execute("select count(*) from {};".format(table_name))
            assert cursor.fetch_scalar() == 0
            self._check_last_copy_bursted(cluster, cursor)

    def _test_burst_load_empty_gzip_folder(self, cluster, db_session, vector,
                                           is_temp, unified_remote_exec):
        """
        Test-2: Burst copy from empty folder with gzip option.
        """
        with db_session.cursor() as cursor:
            table_name = "empty_folder"
            self._init_empty_folder(cluster, cursor, table_name, None,
                                    vector.diststyle, is_temp)
            source = self.data_source_str(
                "", s3bucket=S3_BUCKET, s3path="empty-folder")
            cursor.execute("select now();")
            starttime = cursor.fetch_scalar()
            cursor.run_copy(
                table_name,
                source,
                self.cred_mapping['iamrole'],
                DELIMITER=",",
                gzip=True,
                COMPUPDATE='OFF')
            self._check_last_copy_bursted(cluster, cursor)
            if unified_remote_exec:
                assert self.get_ds_localization_q_count(cluster,
                                                        starttime) == 1
            cursor.execute("select count(*) from {};".format(table_name))
            assert cursor.fetch_scalar() == 0
            self._check_last_copy_bursted(cluster, cursor)

    def _test_burst_copy_zero_length_folder(self, cluster, db_session, vector,
                                            is_temp, unified_remote_exec):
        """
        Test-3: Burst copy from file with no data.
        """
        with db_session.cursor() as cursor:
            table_name = "customers_zero_length"
            self._init_empty_folder(cluster, cursor, table_name, None,
                                    vector.diststyle, is_temp)
            source = self.data_source_str(
                "", s3bucket=S3_BUCKET, s3path="zero-length-customer")
            cursor.execute("select now();")
            starttime = cursor.fetch_scalar()
            cursor.run_copy(
                table_name,
                source,
                self.cred_mapping['iamrole'],
                DELIMITER=",",
                COMPUPDATE='OFF')
            self._check_last_copy_bursted(cluster, cursor)
            if unified_remote_exec:
                assert self.get_ds_localization_q_count(cluster,
                                                        starttime) == 1
            cursor.execute("select count(*) from {};".format(table_name))
            assert cursor.fetch_scalar() == 0
            self._check_last_copy_bursted(cluster, cursor)

    def _test_burst_copy_zero_length_ignoreheader(
            self, cluster, db_session, vector, is_temp, unified_remote_exec):
        """
        Test-4: Burst copy from file with no data when ignoreheader option
                is on.
        """
        with db_session.cursor() as cursor:
            table_name = "customers_zero_length"
            self._init_empty_folder(cluster, cursor, table_name, None,
                                    vector.diststyle, is_temp)
            source = self.data_source_str(
                "", s3bucket=S3_BUCKET, s3path="zero-length-customer")
            cursor.execute("select now();")
            starttime = cursor.fetch_scalar()
            cursor.run_copy(
                table_name,
                source,
                self.cred_mapping['iamrole'],
                DELIMITER=",",
                ignoreheader=1,
                COMPUPDATE='OFF')
            self._check_last_copy_bursted(cluster, cursor)
            if unified_remote_exec:
                assert self.get_ds_localization_q_count(cluster,
                                                        starttime) == 1
            cursor.execute("select count(*) from {};".format(table_name))
            assert cursor.fetch_scalar() == 0
            self._check_last_copy_bursted(cluster, cursor)

    def _test_burst_copy_two_header_files(self, cluster, db_session, vector,
                                          is_temp, unified_remote_exec):
        """
        Test-5: Burst copy to load data from file containing 2 header lines
                with ignoreheader value as 10.
        """
        with db_session.cursor() as cursor:
            table_name = "customers_zero_length"
            self._init_empty_folder(cluster, cursor, table_name, None,
                                    vector.diststyle, is_temp)
            source = self.data_source_str(
                "",
                s3bucket=S3_BUCKET,
                s3path="test_data_with_2_header_lines")
            cursor.execute("select now();")
            starttime = cursor.fetch_scalar()
            cursor.run_copy(
                table_name,
                source,
                self.cred_mapping['iamrole'],
                DELIMITER=",",
                ignoreheader=10,
                COMPUPDATE='OFF')
            self._check_last_copy_bursted(cluster, cursor)
            if unified_remote_exec:
                assert self.get_ds_localization_q_count(cluster,
                                                        starttime) == 1
            cursor.execute("select count(*) from {};".format(table_name))
            assert cursor.fetch_scalar() == 0
            self._check_last_copy_bursted(cluster, cursor)

    def _test_burst_copy_two_header_with_value_two(
            self, cluster, db_session, vector, is_temp, unified_remote_exec):
        """
        Test-6: Burst copy to load data from file containing 2 header lines
                with ignoreheader value as 2.
        """
        with db_session.cursor() as cursor:
            table_name = "customers_zero_length"
            self._init_empty_folder(cluster, cursor, table_name, None,
                                    vector.diststyle, is_temp)
            source = self.data_source_str(
                "",
                s3bucket=S3_BUCKET,
                s3path="test_data_with_2_header_lines")
            cursor.execute("select now();")
            starttime = cursor.fetch_scalar()
            cursor.run_copy(
                table_name,
                source,
                self.cred_mapping['iamrole'],
                DELIMITER=",",
                ignoreheader=2,
                COMPUPDATE='OFF')
            self._check_last_copy_bursted(cluster, cursor)
            if unified_remote_exec:
                assert self.get_ds_localization_q_count(cluster,
                                                        starttime) == 1
            cursor.execute("select count(*) from {};".format(table_name))
            assert cursor.fetch_scalar() == 0
            self._check_last_copy_bursted(cluster, cursor)

    def _test_burst_copy_no_data_with_gzip_format(
            self, cluster, db_session, vector, is_temp, unified_remote_exec):
        """
        Test-7: Burst copy to load data from file containing 2 header line
                with GZIP file format..
        """
        with db_session.cursor() as cursor:
            table_name = "customers_zero_length"
            self._init_empty_folder(cluster, cursor, table_name, None,
                                    vector.diststyle, is_temp)
            source = self.data_source_str(
                "", s3bucket=S3_BUCKET, s3path="zero-length-customer")
            cursor.execute("select now();")
            starttime = cursor.fetch_scalar()
            cursor.run_copy(
                table_name,
                source,
                self.cred_mapping['iamrole'],
                DELIMITER=",",
                gzip=True,
                COMPUPDATE='OFF')
            self._check_last_copy_bursted(cluster, cursor)
            if unified_remote_exec:
                assert self.get_ds_localization_q_count(cluster,
                                                        starttime) == 1
            cursor.execute("select count(*) from {};".format(table_name))
            assert cursor.fetch_scalar() == 0
            self._check_last_copy_bursted(cluster, cursor)

    def _test_burst_copy_folder_with_zero_length_files(
            self, cluster, db_session, vector, is_temp, unified_remote_exec):
        """
        Test-8: Burst copy to load data from folder with empty file.
        """
        with db_session.cursor() as cursor:
            table_name = "customers"
            self._init_empty_folder(cluster, cursor, table_name, None,
                                    vector.diststyle, is_temp)
            source = self.data_source_str(
                "",
                s3bucket=S3_BUCKET,
                s3path="folder-with-zero-length-file")
            cursor.execute("select now();")
            starttime = cursor.fetch_scalar()
            cursor.run_copy(
                table_name,
                source,
                self.cred_mapping['iamrole'],
                COMPUPDATE="OFF",
                DELIMITER=",")
            self._check_last_copy_bursted(cluster, cursor)
            if unified_remote_exec:
                assert self.get_ds_localization_q_count(cluster,
                                                        starttime) == 1
            cursor.execute("select count(*) from {};".format(table_name))
            assert cursor.fetch_scalar() == 6
            self._check_last_copy_bursted(cluster, cursor)

    def _test_burst_copy_default_delimiter(self, cluster, db_session, vector,
                                           is_temp):
        """
        Test-9: Burst copy verifies that '|' is the default delimiter.
        """
        with db_session.cursor() as cursor:
            table_name = "customers_delimiter"
            self._init_empty_folder(cluster, cursor, table_name, None,
                                    vector.diststyle, is_temp)
            source = self.data_source_str(
                "", s3bucket=S3_BUCKET, s3path="default-delimiter")
            cursor.run_copy(
                table_name,
                source,
                self.cred_mapping['iamrole'],
                COMPUPDATE="OFF")
            self._check_last_copy_bursted(cluster, cursor)
            cursor.execute("select count(*) from {};".format(table_name))
            assert cursor.fetch_scalar() == 6

    def _test_burst_unloaded_empty_file(self, cluster, db_session, vector,
                                        is_temp):
        """
        Test-9: Burst unloads empty table into S3 and loads it back again.
        Verifies that the loaded table has no rows
        """
        with db_session.cursor() as cursor:
            table_name = "empty_table__"
            load_table = "load_empty_table"
            self._init_empty_folder(cluster, cursor, table_name, load_table,
                                    vector.diststyle, is_temp)
            unload_sql = (
                "unload ('select * from {0}') to 's3://{1}/{0}/' {2} "
                "ALLOWOVERWRITE delimiter ',';").format(
                    table_name, S3_BUCKET, self.cred_mapping['iamrole'])
            cursor.execute(unload_sql)
            source = self.data_source_str(
                "", s3bucket=S3_BUCKET, s3path=table_name)
            cursor.run_copy(
                table_name,
                source,
                self.cred_mapping['iamrole'],
                COMPUPDATE="OFF",
                DELIMITER=",")
            self._check_last_copy_bursted(cluster, cursor)
            validate_sql = "select count(*) from {};".format(table_name)
            cursor.execute(validate_sql)
            assert cursor.fetch_scalar() == 0
            self._check_last_copy_bursted(cluster, cursor)

    def _test_burst_unloaded_empty_gzip_file(self, cluster, db_session, vector,
                                             is_temp):
        """
        Test-10: Burst unloads empty table into S3 and loads it back again.
        Verifies that the loaded table has no rows
        """
        with db_session.cursor() as cursor:
            table_name = "empty_table_gz__"
            load_table = "load_empty_table"
            self._init_empty_folder(cluster, cursor, table_name, load_table,
                                    vector.diststyle, is_temp)
            unload_sql = (
                "unload ('select * from {0}') to 's3://{1}/{0}/' {2} "
                "ALLOWOVERWRITE delimiter ',' gzip;").format(
                    table_name, S3_BUCKET, self.cred_mapping['iamrole'])
            cursor.execute(unload_sql)
            source = self.data_source_str(
                "", s3bucket=S3_BUCKET, s3path=table_name)
            cursor.run_copy(
                table_name,
                source,
                self.cred_mapping['iamrole'],
                COMPUPDATE="OFF",
                DELIMITER=",",
                gzip=True)
            self._check_last_copy_bursted(cluster, cursor)

            validate_sql = "select count(*) from {};".format(table_name)
            cursor.execute(validate_sql)
            assert cursor.fetch_scalar() == 0
            self._check_last_copy_bursted(cluster, cursor)

    def _init_unload_table(self,
                           cluster,
                           cursor,
                           tbl_name,
                           unload_tbl,
                           diststyle,
                           is_temp,
                           columns="c0 int, c1 int"):
        table_type = 'temp' if is_temp else ''
        cursor.execute(
            self.create_table_cmd(
                tbl_name, columns, diststyle, temp=table_type))
        cursor.execute(
            self.create_table_cmd(
                unload_tbl, columns, diststyle, temp=table_type))
        self._start_and_wait_for_refresh(cluster)
        cursor.execute("set query_group to burst;")

    def _test_burst_copy_unloaded_table_with_diff_options(
            self, cluster, db_session, vector, is_temp):
        """
        Test-11: Unload to bucket with SSE header gzip compression,
                 burst copy unloaded data back and compare result.
        """
        with db_session.cursor() as cursor:
            table_name = "dp9224_tbl"
            unload_table = "dp9224_gzip_tbl"
            s3client = S3Client(profile=Profiles.get_by_name("cookie-core"))
            s3_path = 's3://cookie-monster-test-s3-bucket-policy/unload/{}' \
                      '/dp9224_gzip'
            unload_path = s3_path.format(self._generate_random_string())
            self._init_unload_table(cluster, cursor, table_name, unload_table,
                                    vector.diststyle, is_temp)
            select_unload = "select * from {}".format(table_name)
            select_stmt = "select * from {} order by 1,2;".format(unload_table)
            with self.unload_session(unload_path, s3client):
                source = self.data_source_str(
                    "small.txt", s3bucket=S3_BUCKET, s3path="stsclient")
                cursor.run_copy(
                    table_name,
                    source,
                    self.cred_mapping['default'],
                    DELIMITER=",",
                    COMPUPDATE="OFF")
                self._check_last_copy_bursted(cluster, cursor)
                cursor.run_unload(
                    select_unload,
                    unload_path,
                    self.cred_mapping["default"],
                    ALLOWOVERWRITE=True,
                    delimiter=',',
                    gzip=True)
                cursor.run_copy(
                    unload_table,
                    unload_path,
                    self.cred_mapping["default"],
                    DELIMITER=",",
                    gzip=True,
                    COMPUPDATE="OFF")
                self._check_last_copy_bursted(cluster, cursor)
                cursor.execute(select_stmt)
                assert cursor.fetchall() == [(1, 1), (2, 2), (3, 3)]

    def _test_burst_copy_unloaded_bzip2_table_with_diff_options(
            self, cluster, db_session, vector, is_temp):
        """
        Test-12: Unload to bucket with forces SSE header bzip2 compression,
                 burst copy unloaded data back and compare result.
        """
        with db_session.cursor() as cursor:
            s3client = S3Client(profile=Profiles.get_by_name("cookie-core"))
            s3_path = 's3://cookie-monster-test-s3-bucket-policy/unload/{}' \
                      '/dp9224_bzip2'
            unload_path = s3_path.format(self._generate_random_string())
            table_name = "dp9224_tbl"
            unload_table = "dp9224_bzip2_tbl"
            select_unload = "select * from {}".format(table_name)
            select_stmt = "select * from {} order by 1,2;".format(unload_table)
            self._init_unload_table(cluster, cursor, table_name, unload_table,
                                    vector.diststyle, is_temp)
            with self.unload_session(unload_path, s3client):
                source = self.data_source_str(
                    "small.txt", s3bucket=S3_BUCKET, s3path="stsclient")
                cursor.run_copy(
                    table_name,
                    source,
                    self.cred_mapping["default"],
                    DELIMITER=",",
                    COMPUPDATE="OFF")
                self._check_last_copy_bursted(cluster, cursor)
                cursor.run_unload(
                    select_unload,
                    unload_path,
                    self.cred_mapping["default"],
                    ALLOWOVERWRITE=True,
                    delimiter=',',
                    bzip2=True)
                cursor.run_copy(
                    unload_table,
                    unload_path,
                    self.cred_mapping["default"],
                    DELIMITER=",",
                    bzip2=True,
                    COMPUPDATE="OFF")
                self._check_last_copy_bursted(cluster, cursor)
                cursor.execute(select_stmt)
                assert cursor.fetchall() == [(1, 1), (2, 2), (3, 3)]

    def _test_burst_copy_unloaded_csv_table_with_diff_options(
            self, cluster, db_session, vector, is_temp):
        """
        Test-13: Unload to bucket with forces SSE header in csv format,
                 burst copy unloaded data back and compare result.
        """
        with db_session.cursor() as cursor:
            s3client = S3Client(profile=Profiles.get_by_name("cookie-core"))
            s3_path = 's3://cookie-monster-test-s3-bucket-policy/unload/{}' \
                      '/dp9224_comma'
            unload_path = s3_path.format(self._generate_random_string())
            table_name = "dp9224_tbl"
            unload_table = "dp9224_comma_tbl"
            select_unload = "select * from {}".format(table_name)
            select_stmt = "select * from {} order by 1,2;".format(unload_table)
            self._init_unload_table(cluster, cursor, table_name, unload_table,
                                    vector.diststyle, is_temp)
            with self.unload_session(unload_path, s3client):
                source = self.data_source_str(
                    "small.txt", s3bucket=S3_BUCKET, s3path="stsclient")
                cursor.run_copy(
                    table_name,
                    source,
                    self.cred_mapping["default"],
                    DELIMITER=",",
                    COMPUPDATE="OFF")
                self._check_last_copy_bursted(cluster, cursor)
                cursor.run_unload(
                    select_unload,
                    unload_path,
                    self.cred_mapping["default"],
                    ALLOWOVERWRITE=True,
                    delimiter=',')
                cursor.run_copy(
                    unload_table,
                    unload_path,
                    self.cred_mapping["default"],
                    DELIMITER=",",
                    COMPUPDATE="OFF")
                self._check_last_copy_bursted(cluster, cursor)
                cursor.execute(select_stmt)
                assert cursor.fetchall() == [(1, 1), (2, 2), (3, 3)]

    def _test_burst_copy_unloaded_manifest_table_with_diff_options(
            self, cluster, db_session, vector, is_temp):
        """
        Test-14: Unload to bucket with forces SSE header and manifest,
                 burst copy unloaded data back and compare result.
        """
        with db_session.cursor() as cursor:
            s3client = S3Client(profile=Profiles.get_by_name("cookie-core"))
            s3_path = 's3://cookie-monster-test-s3-bucket-policy/unload/{0}' \
                      '/dp9224_'
            unload_path = s3_path.format(self._generate_random_string())
            copy_path = unload_path + "manifest"
            table_name = "dp9224_tbl"
            unload_table = "dp9224_manifest_tbl"
            select_unload = "select * from {}".format(table_name)
            select_stmt = "select * from {} order by 1,2;".format(unload_table)
            self._init_unload_table(cluster, cursor, table_name, unload_table,
                                    vector.diststyle, is_temp)
            with self.unload_session(unload_path, s3client):
                source = self.data_source_str(
                    "small.txt", s3bucket=S3_BUCKET, s3path="stsclient")
                cursor.run_copy(
                    table_name,
                    source,
                    self.cred_mapping["default"],
                    DELIMITER=",",
                    COMPUPDATE="OFF")
                self._check_last_copy_bursted(cluster, cursor)
                cursor.run_unload(
                    select_unload,
                    unload_path,
                    self.cred_mapping["default"],
                    ALLOWOVERWRITE=True,
                    delimiter=',',
                    manifest=True)
                cursor.run_copy(
                    unload_table,
                    copy_path,
                    self.cred_mapping["default"],
                    DELIMITER=",",
                    manifest=True,
                    COMPUPDATE='OFF')
                self._check_last_copy_bursted(cluster, cursor)
                cursor.execute(select_stmt)
                assert cursor.fetchall() == [(1, 1), (2, 2), (3, 3)]

    def _test_burst_copy_unloaded_default_delimiter(self, cluster, db_session,
                                                    vector, is_temp):
        """
        Test-15: burst copy unloaded data back, verifies that '|' is the
                 default delimiter, and compare result.
        """
        with db_session.cursor() as cursor:
            s3client = S3Client(profile=Profiles.get_by_name("cookie-core"))
            s3_path = 's3://{}/unload_default_delim/{}/'
            unload_path = s3_path.format(S3_BUCKET,
                                         self._generate_random_string())
            table_name = "unload_default_delim"
            unload_table = "load_default_delim"
            column_list = "c0 int, c1 int, c2 int"
            self._init_unload_table(cluster, cursor, table_name, unload_table,
                                    vector.diststyle, is_temp, column_list)
            select_unload = "select * from {}".format(table_name)
            select_stmt = "select * from {} order by 1,2;".format(unload_table)
            insert_cmd = "insert into unload_default_delim values(1,2,3);"
            with self.unload_session(unload_path, s3client):
                cursor.execute(insert_cmd)
                self._check_last_query_bursted(cluster, cursor)
                cursor.run_unload(
                    select_unload,
                    unload_path,
                    self.cred_mapping["default"],
                    ALLOWOVERWRITE=True)
                cursor.run_copy(
                    unload_table,
                    unload_path,
                    self.cred_mapping["default"],
                    COMPUPDATE='OFF')
                self._check_last_copy_bursted(cluster, cursor)
                cursor.execute(select_stmt)
                assert cursor.fetchall() == [(1, 2, 3)]
                self._check_last_query_bursted(cluster, cursor)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.ssm_perm_or_temp_config
@pytest.mark.custom_burst_gucs(gucs={'burst_enable_write': 'true'})
@pytest.mark.custom_local_gucs(
    gucs={**burst_user_temp_support_gucs, 'burst_use_ds_localization': 'false'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstCopyUnloadedData(BaseBurstCopyUnloadedData):
    def test_burst_load_empty_folder(
            self, cluster, db_session, vector, is_temp):
        self._test_burst_load_empty_folder(
            cluster, db_session, vector, is_temp, False)

    def test_burst_load_empty_gzip_folder(
            self, cluster, db_session, vector, is_temp):
        self._test_burst_load_empty_gzip_folder(
            cluster, db_session, vector, is_temp, False)

    def test_burst_copy_zero_length_folder(
            self, cluster, db_session, vector, is_temp):
        self._test_burst_copy_zero_length_folder(
            cluster, db_session, vector, is_temp, False)

    def test_burst_copy_zero_length_ignoreheader(
            self, cluster, db_session, vector, is_temp):
        self._test_burst_copy_zero_length_ignoreheader(
            cluster, db_session, vector, is_temp, False)

    def test_burst_copy_two_header_files(
            self, cluster, db_session, vector, is_temp):
        self._test_burst_copy_two_header_files(
            cluster, db_session, vector, is_temp, False)

    def test_burst_copy_two_header_with_value_two(
            self, cluster, db_session, vector, is_temp):
        self._test_burst_copy_two_header_with_value_two(
            cluster, db_session, vector, is_temp, False)

    def test_burst_copy_no_data_with_gzip_format(
            self, cluster, db_session, vector, is_temp):
        self._test_burst_copy_no_data_with_gzip_format(
            cluster, db_session, vector, is_temp, False)

    def test_burst_copy_folder_with_zero_length_files(
            self, cluster, db_session, vector, is_temp):
        self._test_burst_copy_folder_with_zero_length_files(
            cluster, db_session, vector, is_temp, False)

    def test_burst_copy_default_delimiter(
            self, cluster, db_session, vector, is_temp):
        self._test_burst_copy_default_delimiter(
            cluster, db_session, vector, is_temp)

    def test_burst_unloaded_empty_file(
            self, cluster, db_session, vector, is_temp):
        self._test_burst_unloaded_empty_file(
            cluster, db_session, vector, is_temp)

    def test_burst_unloaded_empty_gzip_file(
            self, cluster, db_session, vector, is_temp):
        self._test_burst_unloaded_empty_gzip_file(
            cluster, db_session, vector, is_temp)

    def test_burst_copy_unloaded_table_with_diff_options(
            self, cluster, db_session, vector, is_temp):
        self._test_burst_copy_unloaded_table_with_diff_options(
            cluster, db_session, vector, is_temp)

    def test_burst_copy_unloaded_bzip2_table_with_diff_options(
            self, cluster, db_session, vector, is_temp):
        self._test_burst_copy_unloaded_bzip2_table_with_diff_options(
            cluster, db_session, vector, is_temp)

    def test_burst_copy_unloaded_csv_table_with_diff_options(
            self, cluster, db_session, vector, is_temp):
        self._test_burst_copy_unloaded_csv_table_with_diff_options(
            cluster, db_session, vector, is_temp)

    def test_burst_copy_unloaded_manifest_table_with_diff_options(
            self, cluster, db_session, vector, is_temp):
        self._test_burst_copy_unloaded_manifest_table_with_diff_options(
            cluster, db_session, vector, is_temp)

    def test_burst_copy_unloaded_default_delimiter(self, cluster, db_session,
                                                   vector, is_temp):
        self._test_burst_copy_unloaded_default_delimiter(
            cluster, db_session, vector, is_temp)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=burst_unified_remote_exec_gucs_burst())
@pytest.mark.custom_local_gucs(
    gucs=burst_unified_remote_exec_gucs_main(burst_use_ds_localization='true'))
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstCopyUnloadedDataUnifiedRemoteExec(
        BaseBurstCopyUnloadedData):
    def test_burst_load_empty_folder_unified_remote_exec(
            self, cluster, db_session, vector):
        self._test_burst_load_empty_folder(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)

    def test_burst_load_empty_gzip_folder_unified_remote_exec(
            self, cluster, db_session, vector):
        self._test_burst_load_empty_gzip_folder(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)

    def test_burst_copy_zero_length_folder_unified_remote_exec(
            self, cluster, db_session, vector):
        self._test_burst_copy_zero_length_folder(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)

    def test_burst_copy_zero_length_ignoreheader_unified_remote_exec(
            self, cluster, db_session, vector):
        self._test_burst_copy_zero_length_ignoreheader(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)

    def test_burst_copy_two_header_files_unified_remote_exec(
            self, cluster, db_session, vector):
        self._test_burst_copy_two_header_files(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)

    def test_burst_copy_two_header_with_value_two_unified_remote_exec(
            self, cluster, db_session, vector):
        self._test_burst_copy_two_header_with_value_two(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)

    def test_burst_copy_no_data_with_gzip_format_unified_remote_exec(
            self, cluster, db_session, vector):
        self._test_burst_copy_no_data_with_gzip_format(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)

    def test_burst_copy_folder_with_zero_length_files_unified_remote_exec(
            self, cluster, db_session, vector):
        self._test_burst_copy_folder_with_zero_length_files(
            cluster,
            db_session,
            vector,
            is_temp=False,
            unified_remote_exec=True)
