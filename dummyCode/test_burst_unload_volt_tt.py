# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest

from raff.burst.burst_test import (BurstTest, setup_teardown_burst,
                                   verify_query_didnt_burst,
                                   verify_all_queries_bursted)
from raff.common.cred_helper import get_role_auth_str
from raff.common.profile import AwsAccounts

__all__ = [
    "setup_teardown_burst", "verify_query_didnt_burst",
    "verify_all_queries_bursted"
]

CUSTOM_GUCS = {
    'burst_enable_volt_tts': 'true',
    'enable_burst_unload': 'false'
}

TPCDS_Q1_SQL = """
      {}
      WITH /* TPC-DS query1.tpl 0.12 */ customer_total_return AS
          (SELECT sr_customer_sk AS ctr_customer_sk,
                  sr_store_sk AS ctr_store_sk,
                  sum(SR_STORE_CREDIT) AS ctr_total_return
           FROM store_returns,
                date_dim
           WHERE sr_returned_date_sk = d_date_sk
             AND d_year =2000
           GROUP BY sr_customer_sk,
                    sr_store_sk)
        SELECT /* TPC-DS query1.tpl 0.12 */ top 100 c_customer_id
        FROM customer_total_return ctr1,
             store,
             customer
        WHERE ctr1.ctr_total_return >
            (SELECT avg(ctr_total_return)*1.2
             FROM customer_total_return ctr2
             WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
          AND s_store_sk = ctr1.ctr_store_sk
          AND s_state = 'MI'
          AND ctr1.ctr_customer_sk = c_customer_sk
        ORDER BY c_customer_id;
"""


@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS)
@pytest.mark.load_tpcds_data
@pytest.mark.usefixtures("setup_teardown_burst")
class TestVoltTTUnloadBurstQualification(BurstTest):
    """
    Tests that when unload is disabled, the main query
    does not burst.
    """

    def test_burst_volt_tt_unload_disabled(self, db_session, s3_client):
        IAM_CREDENTIAL = get_role_auth_str(
            AwsAccounts.DP.iam_roles.Redshift_S3_Write)
        TEST_S3_PATH = ("s3://cookie-monster-s3-ingestion/"
                        "raff_test_burst_unload/{}/{}/")
        rand_str = self._generate_random_string()
        unload_path = TEST_S3_PATH.format('volt_tt_quals', rand_str)
        error_msg = 'false - BurstVoltTTQualification - Unexpected no-burst reason: 21'
        with self.unload_session(unload_path, s3_client):
            with self.burst_db_cursor(db_session) as cursor:
                self.execute_failing_unload(
                    select_stmt=TPCDS_Q1_SQL.format("").replace(
                        "\'", "\\'").replace(" top 100", ""),
                    data_dest=unload_path,
                    error_regex=error_msg,
                    auth=IAM_CREDENTIAL,
                    session=db_session)
                assert cursor.last_unload_row_count() == 0
