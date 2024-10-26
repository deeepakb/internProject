# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
import logging
import pytest
import uuid

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions
from raff.common.db.external_schema import ExternalSchemaSession
from raff.burst.burst_temp_write import (burst_user_temp_support_gucs,
                                         BurstTempWrite)

from raff.burst.burst_unload.burst_unload_test_helper import (
    IAM_CREDENTIAL, TEST_S3_PATH, IAM_CREDENTIAL_ARN)

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

INSERT_SELECT_CMD = ("INSERT INTO {} select * from {}")
SELECT_CMD = ("select count(*) from {}")
SPECTRUM_COPY = (
    "copy call_center_burst from "
    "'s3://cookie-monster-s3-ingestion/test_run_unload_example/call_center_parquet/' "
    " credentials 'aws_iam_role=arn:aws:iam::467896856988:role/Redshift-S3' PARQUET ;"
)
CETAS_CMD = ("create external table {} "
             "stored as parquet location '{}' "
             "as select * from {}")
DUPLICATE_AS_TEMP = "CREATE TEMP TABLE {} AS SELECT * FROM {}"
DROP_STMT = "DROP TABLE IF EXISTS {};"


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.ssm_perm_or_temp_config
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(
    gucs=dict(list(burst_user_temp_support_gucs.items()) +
              [('burst_enable_write', 'true'),
               ('burst_enable_write_copy', 'true'),
               ('burst_enable_write_insert', 'true'),
               ('small_table_row_threshold', 100)]))
class TestBurstSpectrumCases(BurstTempWrite):
    '''
    1. Test spectrum copy is not bursted.
    2. Test insert select from spectrum copy is bursted.
    '''

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['even', 'distkey'],
                sortkey=['', 'sortkey'],
                pristine=['empty', 'non-empty']))

    def _setup_tables(self, db_session, schema, vector, is_temp, temp_call_name=None,
                      temp_catalog_name=None):
        diststyle_one = diststyle_two = vector.diststyle
        sortkey_one = sortkey_two = vector.sortkey
        if vector.diststyle == 'distkey':
            diststyle_one = 'key distkey cr_returned_date_sk'
            diststyle_two = 'key distkey cc_call_center_id'
        if vector.sortkey == 'sortkey':
            sortkey_one = 'cr_item_sk'
            sortkey_two = 'cc_call_center_id'
        self._setup_table(db_session, schema, "catalog_returns", 'tpcds', '1',
                          diststyle_one, sortkey_one, '_burst', is_temp=is_temp)
        self._setup_table(db_session, schema, "call_center", 'tpcds', '1',
                          diststyle_two, sortkey_two, '_burst', is_temp=is_temp)
        with db_session.cursor() as cursor:
            table_type = 'temp' if is_temp else ''
            """
            If testing on temp tables, we need to set up the reference tables with
            unique names in order to avoid naming collisions which will interfere with
            fetching their schema
            """
            if temp_call_name is not None:
                log.info("creating table {}".format(temp_call_name + "_burst"))
                cursor.execute(
                    "create temp table {}(like call_center_burst);".
                    format(temp_call_name + "_burst"))
            if temp_catalog_name is not None:
                log.info("creating table {}".format(temp_catalog_name + "_burst"))
                cursor.execute(
                    "create temp table {}(like catalog_returns_burst);".
                    format(temp_catalog_name + "_burst"))

            # Finalize the table names based on whether they're temp or perm
            call_tbl_name = temp_call_name + "_burst" if temp_call_name is not None \
                else "call_center_burst"
            catalog_tbl_name = temp_catalog_name + "_burst" if temp_catalog_name is \
                not None else "catalog_returns_burst"

            # Drop the local tables if they exist in order to avoid collisions
            cursor.execute(DROP_STMT.format("call_center_local"))
            cursor.execute(
                "create {} table call_center_local(like {});".format(
                    table_type, call_tbl_name))
            cursor.execute(DROP_STMT.format("catalog_returns_local"))
            cursor.execute(
                "create {} table catalog_returns_local(like {});".
                format(table_type, catalog_tbl_name))
            if vector.pristine == 'empty':
                cursor.execute('truncate {}'.format(call_tbl_name))
                cursor.execute('truncate {}'.format(catalog_tbl_name))

    def test_burst_write_spectrum_copy(self, cluster, vector, is_temp):
        """
        Test-1: test spectrum copy should not be bursted.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, schema, vector, is_temp)
            if is_temp:
                schema = self._get_temp_table_schema(cursor, "call_center_burst")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(SPECTRUM_COPY)
            copy_rows = cursor.last_copy_row_count()
            assert copy_rows == 48
            self.check_last_copy_not_bursted_status(cluster, cursor, 50)
            self._validate_table(cluster, schema, 'call_center_burst',
                                 vector.diststyle)

    def _compare_table(self, cursor, tbl1, tbl2):
        cursor.execute("set query_group to metrics;")
        cursor.execute("select * from {} minus select * from {};".format(
            tbl1, tbl2))
        result = cursor.fetchall()
        assert result == []
        cursor.execute("select * from {} minus select * from {};".format(
            tbl2, tbl1))
        result = cursor.fetchall()
        assert result == []
        cursor.execute("set query_group to burst;")
        cursor.execute("select * from {} minus select * from {};".format(
            tbl1, tbl2))
        result = cursor.fetchall()
        assert result == []
        cursor.execute("select * from {} minus select * from {};".format(
            tbl2, tbl1))
        result = cursor.fetchall()
        assert result == []

    def _setup_external_tables(self, cluster):
        db_session = DbSession(cluster.get_conn_params(user='master'))
        rand_str1 = self._generate_random_string()
        rand_str2 = self._generate_random_string()
        unload_path1 = TEST_S3_PATH.format('insert_from_spectrum_table',
                                           rand_str1)
        unload_path2 = TEST_S3_PATH.format('insert_from_spectrum_table',
                                           rand_str2)
        ext_schema = "spectr_{}".format(rand_str1)
        ext_tbl1 = "{}.call_center_{}".format(ext_schema, rand_str1)
        ext_tbl2 = "{}.catalog_returns_{}".format(ext_schema, rand_str2)
        external_schema_options = {
            'schema': ext_schema,
            'database': 'data_lake_export_raff_test_db',
            'arn': IAM_CREDENTIAL_ARN
        }
        regular_user_schema = ExternalSchemaSession(db_session)

        return db_session, ext_tbl1, ext_tbl2, unload_path1, unload_path2, \
                external_schema_options, regular_user_schema

    def test_burst_insert_from_spectrum_table(self, cluster, vector, is_temp):
        """
        Test-2: test insert select from spectrum table should be bursted.
        """

        db_session, ext_tbl1_full, ext_tbl2_full, unload_path1, unload_path2, \
                external_schema_options, regular_user_schema = self._setup_external_tables(cluster)
        with regular_user_schema(**external_schema_options), \
                db_session.cursor() as cursor:
            schema_call = db_session.session_ctx.schema
            schema_catalog = schema_call
            # Setup local table and external table.
            catalog_tbl_name = "catalog_returns_" + str(uuid.uuid4().hex) if is_temp \
                else "catalog_returns"
            call_tbl_name = "call_center_" + str(uuid.uuid4().hex) if is_temp else \
                "call_center"
            self._setup_tables(db_session, schema_catalog, vector, is_temp,
                               call_tbl_name, catalog_tbl_name)
            catalog_tbl_name += "_burst"
            call_tbl_name += "_burst"
            if is_temp:
                schema_call = self._get_temp_table_schema(cursor, call_tbl_name)
                schema_catalog = self._get_temp_table_schema(cursor, catalog_tbl_name)
            cursor.execute(
                CETAS_CMD.format(ext_tbl1_full, unload_path1,
                                 call_tbl_name))
            cursor.execute(
                CETAS_CMD.format(ext_tbl2_full, unload_path2,
                                 catalog_tbl_name))
            cursor.execute("select count(*) from {};".format(call_tbl_name))
            call_center_size1 = cursor.fetch_scalar()
            # Get baseline row count of test tables.
            cursor.execute("select count(*) from {};".format(catalog_tbl_name))
            catalog_returns_size1 = cursor.fetch_scalar()

            if vector.pristine == 'empty':
                assert call_center_size1 == 0
                assert catalog_returns_size1 == 0
                cursor.execute(
                    "select count(*) from {};".format(ext_tbl1_full))
                call_center_size1 = cursor.fetch_scalar()
                cursor.execute(
                    "select count(*) from {};".format(ext_tbl2_full))
                catalog_returns_size1 = cursor.fetch_scalar()

            base = 1 if vector.pristine == 'empty' else 2
            for i in range(3):
                cursor.execute("set query_group to burst;")
                self._start_and_wait_for_refresh(cluster)
                # Test insert into local table select from
                # external table on small table.
                cursor.execute(
                    INSERT_SELECT_CMD.format(call_tbl_name,
                                             ext_tbl1_full))
                self._check_last_query_bursted(cluster, cursor)
                self._validate_table(cluster, schema_call, call_tbl_name,
                                     vector.diststyle)
                cursor.execute("select count(*) from {};".format(call_tbl_name))
                call_center_size2 = cursor.fetch_scalar()
                assert call_center_size2 == (i + base) * call_center_size1

                # Test insert into local table select from
                # external table on large table.
                cursor.execute(
                    INSERT_SELECT_CMD.format(catalog_tbl_name,
                                             ext_tbl2_full))
                self._check_last_query_bursted(cluster, cursor)
                self._validate_table(cluster, schema_catalog, catalog_tbl_name,
                                     vector.diststyle)
                cursor.execute("select count(*) from {};".format(catalog_tbl_name))
                catalog_returns_size2 = cursor.fetch_scalar()
                assert catalog_returns_size2 == (
                    i + base) * catalog_returns_size1

                # Compare with insertion to local tables.
                cursor.execute("set query_group to metrics;")
                cursor.execute(
                    INSERT_SELECT_CMD.format("call_center_local",
                                             ext_tbl1_full))
                cursor.execute(
                    INSERT_SELECT_CMD.format("catalog_returns_local",
                                             ext_tbl2_full))
                self._start_and_wait_for_refresh(cluster)
                self._compare_table(cursor, "call_center_local",
                                    call_tbl_name)
                self._compare_table(cursor, "catalog_returns_local",
                                    "catalog_returns_local")
            # Cleanup external table.
            cursor.execute("DROP TABLE IF EXISTS {};".format(ext_tbl1_full))
            cursor.execute("DROP TABLE IF EXISTS {};".format(ext_tbl2_full))
