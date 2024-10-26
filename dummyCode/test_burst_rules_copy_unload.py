# Copyright18 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import uuid

import pytest
from time import sleep
from raff.common.result import SelectResult
from raff.qp.qp_test import QPTest
from raff.burst.burst_test import (
        BurstTest,
        setup_teardown_burst,
        verify_query_didnt_burst
    )
from raff.common.db.session import DbSession
from raff.common.profile import Profiles
from raff.common.cred_helper import get_key_auth_str, get_role_auth_str
from raff.ingestion.ingestion_test import UnloadTestSuite


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstRulesCopyUnload(BurstTest):
    def test_copy_burst(self, db_session, verify_query_didnt_burst):
        """
        This test runs copies and make sure it doesn't burst. Adapted from
        test_create_table.py#test_dimensions_create_table
        """
        with db_session.cursor() as cursor:
            # Create table. Each table will be created in its own schema.
            sql_create = """CREATE TABLE IF NOT EXISTS region (
                            r_regionkey int4 NOT NULL,
                            r_name CHAR(25) NOT NULL,
                            r_comment VARCHAR(152) NOT NULL)"""
            cursor.execute(sql_create)

            # Run COPY and validate all 5 rows has been inserted
            cursor.run_copy("region", "s3://tpc-h/1/region.tbl",
                             gzip=True, delimiter="|")
            assert cursor.last_copy_row_count() == 5

    def test_unload_burst(self, db_session, s3_client, verify_query_didnt_burst):
        """
        This test runs unload and makes sure it doesn't burst. Adapted from
        test_run_unload.py#test_run_unload_positive.
        """
        SELECT_STMT = 'SELECT * FROM call_center'
        S3_PATH = 's3://cookie-monster-s3-ingestion/test_run_unload_example/{}/'
        unload_path = S3_PATH.format(str(uuid.uuid4()).replace("-", ""))

        with self.unload_session(unload_path, s3_client):
            with db_session.cursor() as cursor:
                # Test: run UNLOAD with specified profile (AWS Keys in string).
                dp_keys_str = get_key_auth_str(
                        profile=Profiles.get_by_name("dp-basic"))
                cursor.run_unload(SELECT_STMT, unload_path, dp_keys_str,
                                  allowoverwrite=True)
                assert cursor.last_unload_row_count() == 6
