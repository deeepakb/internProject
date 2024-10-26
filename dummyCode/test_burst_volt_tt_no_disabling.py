# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest
import datetime
from contextlib import contextmanager

from raff.burst.burst_test import (BurstTest, get_burst_cluster_name,
                                   setup_teardown_burst)
from raff.common.aws_clients.redshift_client import RedshiftClient
from raff.common.cred_helper import get_role_auth_str
from raff.common.profile import AwsAccounts
from raff.common.profile import Profiles
from raff.common.region import Regions
from raff.common.base_test import run_priviledged_query_scalar_int

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

UNLOAD_BUCKET = "cookie-monster-s3-ingestion"
TEST_S3_PATH = ("s3://{}/" "raff_test_burst_unload/{}/{}/")


@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.no_jdbc
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS)
@pytest.mark.load_tpcds_data
@pytest.mark.usefixtures("setup_teardown_burst")
class TestVoltTTFailureNotDisabling(BurstTest):
    """
    Tests that failures when Bursting Volt TTs together with access denied
    exceptions don't result in disabling burst volt TT.
    """

    @contextmanager
    def whitelist_context(self, cluster, resource):
        cluster.run_xpx('burst_whitelist_resource {}'.format(resource))
        yield
        cluster.run_xpx('burst_whitelist_resource {}'.format(resource))

    def get_burst_cluster(self, cluster):
        profile_obj = Profiles.get_by_name(Profiles.QA_BURST_TEST.name)
        burst_client = RedshiftClient(profile=profile_obj, region=Regions.QA)
        burst_cluster_name = get_burst_cluster_name(cluster)
        burst_cluster = burst_client.describe_cluster(burst_cluster_name)
        return burst_cluster

    def execute_volt_unload(self,
                            s3_client,
                            db_session,
                            cluster,
                            error_msg=None):
        IAM_CREDENTIAL = get_role_auth_str(
            AwsAccounts.DP.iam_roles.Redshift_S3_Write)

        rand_str = self._generate_random_string()
        unload_path = TEST_S3_PATH.format(
            UNLOAD_BUCKET, 'volt_tt_failure_no_replay', rand_str)

        SELECT_STMT = TPCDS_Q1_SQL.replace("\'", "\\'").replace(" top 100", "")

        with self.unload_session(unload_path, s3_client):
            with self.burst_db_cursor(db_session) as cursor:
                if error_msg is None:
                    # Note: UNLOAD does not support LIMIT.
                    cursor.run_unload(SELECT_STMT, unload_path, IAM_CREDENTIAL)
                    assert cursor.last_unload_row_count() == 0
                else:
                    self.execute_failing_unload(
                        select_stmt=SELECT_STMT,
                        data_dest=unload_path,
                        error_regex=error_msg,
                        auth=IAM_CREDENTIAL,
                        session=db_session)
                return cursor.last_unload_query_id()

    def execute_simple_unload(self, s3_client, db_session, cluster):
        IAM_CREDENTIAL = get_role_auth_str(
            AwsAccounts.DP.iam_roles.Redshift_S3_Write)

        rand_str = self._generate_random_string()
        unload_path = TEST_S3_PATH.format(
            UNLOAD_BUCKET, 'volt_tt_failure_no_replay', rand_str)
        SELECT_STMT = 'SELECT * FROM call_center'
        with self.unload_session(unload_path, s3_client):
            with self.burst_db_cursor(db_session) as cursor:
                cursor.run_unload(SELECT_STMT, unload_path, IAM_CREDENTIAL)
                return cursor.last_unload_query_id()

    def verify_volt_not_disabled(self, start_str, cluster):
        query = """ SELECT count(*) from stl_event_trace
                    WHERE message like '%Disabling VoltTT Support%'
                    AND eventtime >= '{}'
                """.format(start_str)
        count = run_priviledged_query_scalar_int(cluster, None, query)
        assert count == 0, "Volt was disabled but that was not expected"

    def verify_bucket_blacklisted(self, start_str, cluster):
        query = """ SELECT count(*) from stl_event_trace
                    WHERE message like '%Blacklisting {}%'
                    AND eventtime >= '{}'
                """.format(UNLOAD_BUCKET, start_str)
        count = run_priviledged_query_scalar_int(cluster, None, query)
        assert count == 1, "Bucket was not blacklisted"

    def get_burst_query_id(self, cluster, qid):
        '''
        Get the burst query id given the main query id.
        When a query fails on burst its WLM state is set to 'Evicted' and
        is rerun on main as a new query with the same xid.
        '''
        query = """select query from stl_wlm_query where xid =
                   (select xid from stl_wlm_query where query = {})
                   and query <> {} and final_state = 'Evicted'
                """.format(qid, qid)
        return run_priviledged_query_scalar_int(cluster, None, query)

    def test_burst_volt_tt_no_failure_disabling_pre(self, cluster, db_session,
                                                    s3_client):
        """
        Tests that volt TT can be executed without problems when the bucket is
        already blacklisted before the first volt query.
        """
        burst_cluster = self.get_burst_cluster(cluster)
        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')
        with self.whitelist_context(cluster, UNLOAD_BUCKET):
            # Run a first query (without volt) to blacklist the bucket.
            with burst_cluster.event('EtSimulateRestAgentFetchError',
                                     'code=8001'):
                # Query fail on burst (29) and retry on main when the event
                # is set. Main status should be 35 (Resource blacklisted)
                main_qid = self.execute_simple_unload(s3_client, db_session,
                                                      cluster)
                burst_qid = self.get_burst_query_id(cluster, main_qid)
                self.verify_query_status(cluster, burst_qid, 29)
                self.verify_query_status(cluster, main_qid, 35)
                self.verify_volt_not_disabled(start_str, cluster)
                self.verify_bucket_blacklisted(start_str, cluster)

            # Verifies that subsequent volt queries run on main because the
            # bucket is already blacklisted.
            main_qid = self.execute_volt_unload(s3_client, db_session, cluster)
            self.verify_query_status(cluster, main_qid, 33)
            self.verify_volt_not_disabled(start_str, cluster)

            # Ensure that following executions of Volt queries that can be
            # replayed do not fail and do burst since they don't access the
            # blacklisted bucket.
            with self.burst_db_cursor(db_session) as cursor:
                cursor.execute(TPCDS_Q1_SQL)
                assert cursor.fetchall() == [], "Volt query failed"
                cursor.execute(QUERY_ID)
                QID = cursor.fetchall()[0][0]
                self.verify_query_bursted(cluster, QID)

            self.verify_volt_not_disabled(start_str, cluster)

    def test_burst_volt_tt_no_failure_disabling_first_query(
            self, cluster, db_session, s3_client):
        """
        Tests that volt TT can be executed without problems when the bucket is
        blacklisted during the first volt query.
        """
        burst_cluster = self.get_burst_cluster(cluster)
        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')

        error_msg = '.*Access Denied.*'

        with self.whitelist_context(cluster, UNLOAD_BUCKET):
            # Run an unload that will fail with access denied.
            with burst_cluster.event('EtFailNthBurstQueries',
                                     'frequency=2,code=8001'):
                # Query fail on burst but can't retry because the volt state
                # is on burst.
                burst_qid = self.execute_volt_unload(s3_client, db_session,
                                                     cluster, error_msg)
                self.verify_query_status(cluster, burst_qid, 1)
                self.verify_volt_not_disabled(start_str, cluster)
                self.verify_bucket_blacklisted(start_str, cluster)

            # Verifies that subsequent volt queries run on main because the
            # bucket is already blacklisted.
            main_qid = self.execute_volt_unload(s3_client, db_session, cluster)
            self.verify_query_status(cluster, main_qid, 33)
            self.verify_volt_not_disabled(start_str, cluster)

            # Ensure that following executions of Volt queries that can be
            # replayed do not fail and do burst since they don't access the
            # blacklisted bucket.
            with self.burst_db_cursor(db_session) as cursor:
                cursor.execute(TPCDS_Q1_SQL)
                assert cursor.fetchall() == [], "Volt query failed"
                cursor.execute(QUERY_ID)
                QID = cursor.fetchall()[0][0]
                self.verify_query_bursted(cluster, QID)

            self.verify_volt_not_disabled(start_str, cluster)
