#!/usr/bin/env python3
# -*- coding: iso-8859-1 -*-

# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import logging
from random import randrange

from raff.common.aws_clients.s3_client import S3Client
from raff.common.base_test import run_priviledged_query
from raff.common.cred_helper import get_key_auth_str
from raff.common.db.db_exception import ProgrammingError
from raff.common.db.session_context import SessionContext
from raff.common.profile import Profiles
from raff.common.region import Regions
from raff.monitoring.monitoring_test import MonitoringTestSuite
from raff.common.profile import AwsAccounts

log = logging.getLogger(__name__)

TEST_TABLE = "test_load_error_detail_{}".format(randrange(100))

CREATE_TABLE_WITH_WRONG_SCHEMA = """
create table if not exists {}(id int, v1 int, v2 int);
""".format(TEST_TABLE)

CLEAN_TABLE = "drop table if exists {};".format(TEST_TABLE)

GET_COL_NAMES_OF_SYS_LOAD_ERROR_DETAIL = """
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'sys_load_error_detail';
"""

GET_LAST_LOAD_ERROR = """
select query_id, trim(database_name), trim(file_name),
line_number, error_code, trim(error_message)
from sys_load_error_detail
where start_time > '{}'::timestamp
order by start_time desc
limit 1;
"""

GET_QUERY_ID_OF_FAILED_COPY = """
select query_id from sys_query_history
where start_time > '{}'::timestamp and
      status = 'failed' and
      query_text ilike '%copy {}%'
limit 1;
"""

GET_LAST_LOAD_ERROR_BY_QUERY_ID = """
select query_id, trim(database_name), trim(file_name),
line_number, error_code, trim(error_message)
from sys_load_error_detail
where query_id = {}
order by start_time desc
limit {};
"""

GET_LOAD_ERROR_COUNT = """
select count(*)
from sys_load_error_detail
where query_id = {};
"""

COPY_DATA_SOURCE = "s3://tpc-h/10M/lineitem.tbl"
ERROR_MESSAGE = "Check 'sys_load_error_detail' system table for details"

S3_ARN = AwsAccounts.DP.iam_roles.Redshift_S3_Write.arn
S3_BUCKET = "cookie-monster-s3-ingestion"


@pytest.mark.localhost_only
@pytest.mark.serial_only
class TestSysLoadErrorDetail(MonitoringTestSuite):
    """
    This test is for vlidating system view sys_load_error_detail should have
    load error details
    """

    def test_sys_load_error_detail_should_record_error(self, cluster_session,
                                                       db_session):
        """
        1. Run load cmd to load S3 file to test table with errors
        2. Very sys_load_error_detail should have recorded the load errors
        """
        guc = {'is_arcadia_cluster': 'true', 'enable_arcadia': 'true'}
        with cluster_session(gucs=guc):
            test_start_time = self.current_db_time(db_session)
            with db_session.cursor() as cursor:
                # Create test table
                cursor.execute(CREATE_TABLE_WITH_WRONG_SCHEMA)
                copy_failed = False
                try:
                    # Run copy cmd
                    auth_profile = get_key_auth_str()
                    cursor.run_copy(
                        TEST_TABLE,
                        COPY_DATA_SOURCE,
                        auth_profile,
                        delimiter='|')
                except Exception as e:
                    log.info('exeption: {}'.format(e))
                    assert ERROR_MESSAGE in str(e)
                    copy_failed = True

                    # Sanity check for sys_load_error_detail to make sure
                    # it doesn't contain raw data column which may contain
                    # sensitive info about customer business
                    cursor.execute(GET_COL_NAMES_OF_SYS_LOAD_ERROR_DETAIL)
                    col_names = cursor.fetchall()
                    assert not ('raw_line',) in col_names and \
                        not ('raw_field_value',) in col_names

                    # Get copy status from sys_load_error_detail
                    cursor.execute(GET_LAST_LOAD_ERROR.format(test_start_time))
                    load_query_result = cursor.fetchall()

                    # Get query id from sys_query_history.
                    cursor.execute("select max(query_id) from \
                            sys_query_history where query_text like \
                            'COPY {}%';".format(TEST_TABLE))
                    last_used_query_id = cursor.fetch_scalar()
                    EXPECTED_RESULT = (last_used_query_id, 'dev',
                                       COPY_DATA_SOURCE, 1, 1202,
                                       'Extra column(s) found')
                    for expected, res in zip(EXPECTED_RESULT,
                                             load_query_result[0]):
                        if isinstance(expected, str):
                            assert expected in res,\
                                'string result does not match'
                        else:
                            assert expected == res,\
                                'number result does not match'

                    cursor.execute(
                        GET_LOAD_ERROR_COUNT.format(last_used_query_id))
                    error_count = int(cursor.fetchall()[0][0])
                    assert error_count == 1, 'Expect only one error record'

                assert copy_failed, 'Copy should fail'
                cursor.execute(CLEAN_TABLE)

    @pytest.mark.session_ctx(user_type=SessionContext.SUPER)
    def test_super_user_can_not_access_stl_user_load_error_detail(
            self, cluster, db_session):
        """
        Test that super user cannot access stl_user_load_error_detail.
        """
        with db_session.cursor() as cursor:
            permission_error = False
            try:
                cursor.execute('select * from stl_user_load_error_detail;')
            except ProgrammingError as e:
                assert ("permission denied for relation "
                        "stl_user_load_error_detail") in str(e)
                permission_error = True
            assert permission_error

    def verify_spectrum_copy_error_helper(self, db_session, cursor,
                                          source_file, expected_error_msg):
        test_start_time = self.current_db_time(db_session)
        copy_cmd = """
            COPY {table}
            FROM '{file}'
            IAM_ROLE '{arn}'
            {option};
            """
        file_format = source_file.split('.')[-1]
        prefix = "test_sys_load_history/"
        test_file_path = \
            ("s3://{}/{}{}").format(S3_BUCKET, prefix, source_file)
        try:
            cursor.execute(
                copy_cmd.format(
                    table=TEST_TABLE,
                    file=test_file_path,
                    arn=S3_ARN,
                    option="format as {}".format(file_format)))
        except Exception:
            cursor.execute(
                GET_QUERY_ID_OF_FAILED_COPY.format(
                    test_start_time, TEST_TABLE))
            query_id = int(cursor.fetchall()[0][0])
            cursor.execute(
                GET_LAST_LOAD_ERROR_BY_QUERY_ID.format(query_id, 1))
            load_error_result = cursor.fetchall()[0]
            log.info('Query result: {}'.format(load_error_result))
            assert expected_error_msg in load_error_result[-1]  # error message
            # The error code should be 15007 which refers to
            # S3SubqueryUserError
            assert load_error_result[-2] == 15007               # error code
            assert load_error_result[2] == test_file_path       # filename
            assert load_error_result[1] == "dev"                # database name

    def test_sys_load_error_detail_should_surface_spectrum_copy_error_info(
            self, cluster_session, db_session):
        guc = {'is_arcadia_cluster': 'true', 'enable_arcadia': 'true'}
        with cluster_session(gucs=guc):
            with db_session.cursor() as cursor:
                try:
                    cursor.execute(
                        "drop table if exists {}".format(TEST_TABLE))
                    cursor.execute(
                        "create table {} (c1 int, c2 varchar(10))".format(
                            TEST_TABLE))

                    # Test case 1:
                    # The target table has 2 columns: c1 int, c2 varchar
                    # while the column c1 of source s3 file is varchar, so
                    # the copy command will fail due to data type mismatch

                    # Test copy Parquet file from s3
                    test_parquet_file = \
                        ("s3://{}/test_sys_load_history/"
                            "parquet_test_2.parquet").format(S3_BUCKET)
                    expected_error_msg = \
                        ("has an incompatible Parquet schema for column '{}.c1"
                            "'").format(test_parquet_file)
                    self.verify_spectrum_copy_error_helper(
                        db_session, cursor, "parquet_test_2.parquet",
                        expected_error_msg)

                    # Test copy ORC file from s3
                    expected_error_msg =\
                        ("declared column type INT for column c1 incompatible"
                            " with ORC file column type string")
                    self.verify_spectrum_copy_error_helper(
                        db_session, cursor, "orc_test_2.orc",
                        expected_error_msg)

                    # Test case 2:
                    # The target table has 2 columns: c1 int, c2 varchar
                    # and the same with source s3 file, but the string length
                    # of column c2 in source file is larger than the length of
                    # c2 in target table. So, the copy command will fail due
                    # to string length exceeding DDL length
                    expected_error_msg = (
                        "Spectrum Scan Error. The length of the data column c"
                        "2 is longer than the length defined in the table")

                    # Test copy Parquet file from s3
                    self.verify_spectrum_copy_error_helper(
                        db_session, cursor, "parquet_test_3.parquet",
                        expected_error_msg)

                    # Test copy ORC file from s3
                    self.verify_spectrum_copy_error_helper(
                        db_session, cursor, "orc_test_3.orc",
                        expected_error_msg)
                finally:
                    cursor.execute(
                        "drop table if exists {}".format(TEST_TABLE))

    def test_should_log_spectrum_copy_error_in_multi_thread_scenario(
            self, cluster_session, db_session):
        # This test is supposed to verify PADB will not crash due to SIG11
        # caused by failed multi-thread spectrum copy.
        # SIM: https://sim.amazon.com/issues/RedshiftDP-39665
        guc = {'is_arcadia_cluster': 'true', 'enable_arcadia': 'true'}
        with cluster_session(gucs=guc):
            test_start_time = self.current_db_time(db_session)
            with db_session.cursor() as cursor:
                # This copy cmd will try to load 1000+ parquet files in the s3
                # path below in parallel; because the column type in parquet
                # file is different with date type in target test table, the
                # copy cmd would fail and then trigger exception handling for
                # Spectrum copy.
                prefix = "test_sys_load_history/test_concurrent_failed_copy/"
                test_file_path = \
                    ("s3://{}/{}").format(S3_BUCKET, prefix)
                copy_files_in_parallel = """
                    COPY {table}
                    FROM '{s3_path}'
                    IAM_ROLE '{arn}'
                    FORMAT AS PARQUET;
                    """.format(
                        table=TEST_TABLE,
                        s3_path=test_file_path,
                        arn=S3_ARN)
                s3_client = S3Client(Profiles.DP_BASIC, Regions.IAD)
                objs = list(s3_client.list_all_objects(S3_BUCKET, prefix))
                assert len(objs) > 1000,\
                    'There should be more than 1000 files in the s3 path'
                try:
                    cursor.execute(
                        "drop table if exists {}".format(TEST_TABLE))
                    cursor.execute("create table {} (c1 int, c2 varchar(10))"
                                   .format(TEST_TABLE))
                    cursor.execute(copy_files_in_parallel)
                except Exception:
                    cursor.execute(
                        GET_QUERY_ID_OF_FAILED_COPY.format(
                            test_start_time, TEST_TABLE))
                    query_id = int(cursor.fetchall()[0][0])
                    cursor.execute(
                        GET_LAST_LOAD_ERROR_BY_QUERY_ID.format(query_id, 2000))
                    results = cursor.fetchall()
                    # Make sure failed spectrum copy error info has been logged
                    # into system table
                    assert results  # Make sure the results is not None
                    assert len(results) > 0
                    expected_error_msg = \
                        ("has an incompatible Parquet schema for column "
                         "'{}").format(test_file_path)
                    for res in results:
                        # error message
                        assert expected_error_msg in res[-1]
                        # The error code should be 15007 which refers to
                        # S3SubqueryUserError
                        assert res[-2] == 15007
                        # filename
                        assert test_file_path in res[2]
                        # database name
                        assert res[1] == "dev"

    def test_failed_spectrum_copy_should_not_hit_sig11_when_cleanup_error_msg(
            self, cluster, cluster_session, db_session):
        """
        This test is verifying if we will NOT do XCHECK and restart PADB if
        there is invalid UTF-8 string in spectrum error message, instead it
        just skip all invalid characters.
        """
        test_parquet_file = ("s3://{}/test_sys_load_history/"
                             "parquet_test_2.parquet").format(S3_BUCKET)
        guc = {'is_arcadia_cluster': 'true', 'enable_arcadia': 'true'}
        # To query the debug log written by monitoring_trace_debug
        guc['gconf_event'] = '{}|{}'.format(
            str(cluster.get_padb_conf_value("gconf_event")),
            'EtMonitoringTracing,level=ElDebug5')
        with cluster_session(gucs=guc):
            test_start_time = self.current_db_time(db_session)
            # Append test invalid UTF-8 string to the end of error message
            with cluster.event('EtSimulateInvalidUTF8String'):
                with db_session.cursor() as cursor:
                    try:
                        cursor.execute(
                            "drop table if exists {}".format(TEST_TABLE))
                        cursor.execute(
                            "create table {} (c1 int, c2 varchar(10))".format(
                                TEST_TABLE))
                        # The target table has 2 columns: c1 int, c2 varchar
                        # while the column c1 of source s3 file is varchar, so
                        # the copy command will fail due to data type mismatch
                        copy_cmd = "COPY {} FROM '{}' IAM_ROLE '{}' {};"
                        try:
                            cursor.execute(
                                copy_cmd.format(
                                    TEST_TABLE, test_parquet_file, S3_ARN,
                                    "format as parquet"))
                        except Exception:
                            cursor.execute(
                                GET_QUERY_ID_OF_FAILED_COPY.format(
                                    test_start_time, TEST_TABLE))
                            query_id = int(cursor.fetchall()[0][0])
                            cursor.execute(
                                GET_LAST_LOAD_ERROR_BY_QUERY_ID.format(
                                    query_id, 2000))
                            results = cursor.fetchall()
                            # Make sure failed spectrum copy error info has been logged
                            # into system table
                            assert results  # Make sure the results is not None
                            assert len(results) > 0
                            escaped_invalid_utf8_string = 'test1test2'
                            for res in results:
                                # We will skip all invalid characters
                                assert res[-1].endswith(
                                        escaped_invalid_utf8_string)
                            expected_warning_message = \
                                "Invalid multi-bytes character in"
                            count = int(run_priviledged_query(
                                cluster, self.db.cursor(),
                                ("select count(*) from stl_event_trace "
                                 "where message like '%{}%' and "
                                 "eventtime > '{}'::timestamp").format(
                                    expected_warning_message,
                                    test_start_time))[0][0])
                            assert count > 0, \
                                ("There should be warning message for invalid"
                                 " UTF-8 string in stl_event_trace.")
                    finally:
                        cursor.execute(
                            "drop table if exists {}".format(TEST_TABLE))
