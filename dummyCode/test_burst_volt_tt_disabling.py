# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest

from raff.burst.burst_test import (BurstTest, setup_teardown_burst)
from raff.common.aws_clients.redshift_client import RedshiftClient
from raff.common.cred_helper import get_role_auth_str
from raff.common.profile import AwsAccounts
from raff.common.profile import Profiles
from raff.common.region import Regions

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst"]

CUSTOM_GUCS = {
    'burst_disable_volt_tts_on_failure': 'true',
    'burst_enable_volt_tts': 'true',
    'burst_mode': '3',
    'burst_volt_tts_require_replay': 'false',
    'enable_burst_failure_handling': 'true'
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
QUERY_ID = "select pg_last_query_id();"
VALIDATE = """
    select concurrency_scaling_status from stl_query where query = {};
"""


@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.no_jdbc
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS)
@pytest.mark.load_tpcds_data
@pytest.mark.usefixtures("setup_teardown_burst")
class TestVoltTTFailureDisabling(BurstTest):
    """
    Tests that failures when Bursting Volt TTs are not retried but lead to
    disabling the feature until the next cluster restart. Note that this test
    relies on Burst tests requiring cluster restarts before and after running
    all tests for a class.
    """

    def get_burst_cluster(self, burst_cluster_arn):
        profile_obj = Profiles.get_by_name(Profiles.QA_BURST_TEST.name)
        burst_client = RedshiftClient(profile=profile_obj, region=Regions.QA)
        burst_cluster_name = burst_cluster_arn.split(':cluster:')[-1].strip()
        burst_cluster = burst_client.describe_cluster(burst_cluster_name)
        return burst_cluster

    def test_burst_volt_tt_failure_disabling(self, cluster, db_session,
                                             s3_client):
        # Setup
        burst_cluster_arns = cluster.list_acquired_burst_clusters()
        # Using [0] below is ok because 'setup_teardown_burst' fixture
        # makes sure that only one Burst cluster is prepared for Burst tests.
        burst_cluster = self.get_burst_cluster(burst_cluster_arns[0])
        IAM_CREDENTIAL = get_role_auth_str(
            AwsAccounts.DP.iam_roles.Redshift_S3_Write)
        TEST_S3_PATH = ("s3://cookie-monster-s3-ingestion/"
                        "raff_test_burst_unload/{}/{}/")

        # Ensure that the first query fails if we fail the second and final query.
        burst_cluster.set_event('EtFailNthBurstQueries,frequency={}'.format(2))
        error_msg = '.*Simulated Burst query failure.*'
        rand_str = self._generate_random_string()
        unload_path = TEST_S3_PATH.format('volt_tt_failure_no_replay',
                                          rand_str)
        with self.unload_session(unload_path, s3_client):
            with self.burst_db_cursor(db_session) as cursor:
                # Note: UNLOAD does not support LIMIT.
                self.execute_failing_unload(
                    select_stmt=TPCDS_Q1_SQL.format("").replace(
                        "\'", "\\'").replace(" top 100", ""),
                    data_dest=unload_path,
                    error_regex=error_msg,
                    auth=IAM_CREDENTIAL,
                    session=db_session)

        # Ensure that following executions of that same query do not fail.
        rand_str = self._generate_random_string()
        unload_path = TEST_S3_PATH.format('volt_tt_failure_no_replay',
                                          rand_str)
        with self.unload_session(unload_path, s3_client):
            with self.burst_db_cursor(db_session) as cursor:
                # Note: UNLOAD does not support LIMIT.
                cursor.run_unload(
                    TPCDS_Q1_SQL.format("").replace("\'", "\\'").replace(
                        " top 100", ""), unload_path, IAM_CREDENTIAL)
                assert cursor.last_unload_row_count() == 0

        rand_str = self._generate_random_string()
        unload_path = TEST_S3_PATH.format('volt_tt_failure_no_replay',
                                          rand_str)
        with self.unload_session(unload_path, s3_client):
            with self.burst_db_cursor(db_session) as cursor:
                # Note: UNLOAD does not support LIMIT.
                cursor.run_unload(
                    TPCDS_Q1_SQL.format("").replace("\'", "\\'").replace(
                        " top 100", ""), unload_path, IAM_CREDENTIAL)
                assert cursor.last_unload_row_count() == 0

        # Unset the event to fail on Burst cluster.
        burst_cluster.unset_event('EtFailNthBurstQueries')

        # Ensure that following executions of Volt queries that can be
        # replayed do not fail and do burst.
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(TPCDS_Q1_SQL.format(""))
            assert cursor.fetchall() == []
            cursor.execute(QUERY_ID)
            QID = cursor.fetchall()[0][0]
            cursor.execute(VALIDATE.format(QID))
            assert cursor.fetchall() == [(1, )]
