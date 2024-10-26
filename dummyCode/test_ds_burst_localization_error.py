# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest
import logging
from raff.burst.burst_test import (BurstTest, setup_teardown_burst,
                                   verify_query_didnt_burst,
                                   verify_all_queries_bursted)
from raff.common.cred_helper import get_role_auth_str
from raff.common.profile import AwsAccounts
from raff.common.aws_clients.redshift_client import RedshiftClient
from raff.common.profile import Profiles
from raff.common.region import Regions
from raff.datasharing.datasharing_test import (
    DatasharingBurstTest, consumer_cluster, consumer_session_ext_only,
    datashare_context, datashare_setup, setup_teardown_burst_datasharing,
    customise_burst_cluster_datasharing, verify_query_burst_datasharing,
    verify_query_didnt_burst_datasharing)
log = logging.getLogger(__name__)

# Point to existing consumer cluster.
# Remove if we have a preferered runnable.
# pytestmark = pytest.mark.consumer_cluster_existing(
#     existing_identifier='chunbin-rc-consumer')

CONSUMER_CLUSTER_GUCS = dict(
    enable_burst_datasharing='true',
    enable_burst_datasharing_volt_tt='true',
    enable_burst_async_acquire='false',
    enable_burst_failure_handling='false')
BURST_CLUSTER_GUCS = dict(
    enable_burst_datasharing='true',
    enable_burst_datasharing_volt_tt='true',
    enable_data_sharing_result_cache='false',
    diff_topologies_mode='2',
    enable_redcat_table_integration='true',
    enable_redshift_federation='true',
    use_s3commit_in_rslocal_federation='true')

TPCDS_Q1_SQL = """
      WITH /* TPC-DS query1.tpl 0.12 */ customer_total_return AS
          (SELECT sr_customer_sk AS ctr_customer_sk,
                  sr_store_sk AS ctr_store_sk,
                  sum(SR_STORE_CREDIT) AS ctr_total_return
           FROM devpublic.store_returns,
                devpublic.date_dim
           WHERE sr_returned_date_sk = d_date_sk
             AND d_year =2000
           GROUP BY sr_customer_sk,
                    sr_store_sk)
        SELECT /* TPC-DS query1.tpl 0.12 */ top 100 c_customer_id
        FROM customer_total_return ctr1,
             devpublic.store,
             devpublic.customer
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
@pytest.mark.custom_burst_gucs(gucs=CONSUMER_CLUSTER_GUCS)
@pytest.mark.usefixtures("customise_burst_cluster_datasharing")
@pytest.mark.customise_burst_cluster_args(BURST_CLUSTER_GUCS)
class TestDsBurstLocalization(DatasharingBurstTest):
    """
    Tests that Burst query fails and the expected error message is received
    in the consumer cluster for both pg_error and cpp exception in localization.
    """

    def get_burst_cluster(self, burst_cluster_arn):
        profile_obj = Profiles.get_by_name(Profiles.QA_BURST_TEST.name)
        burst_client = RedshiftClient(profile=profile_obj, region=Regions.QA)
        burst_cluster_name = burst_cluster_arn.split(':cluster:')[-1].strip()
        burst_cluster = burst_client.describe_cluster(burst_cluster_name)
        return burst_cluster

    @pytest.mark.no_jdbc
    def test_ds_burst_localization_error(self, cluster, consumer_cluster,
                                         consumer_session_ext_only, s3_client):
        # Setup
        burst_cluster_arns = consumer_cluster.list_acquired_burst_clusters()
        burst_cluster = self.get_burst_cluster(burst_cluster_arns[0])

        # Case 1: Ensure that the query fails if localization failed with
        # pg_error and the expceted error message is received in the consumer
        # cluster.
        error_msg = 'Simulated Burst localization failure with pg_error'
        burst_cluster.set_event(
            'EtFailBurstQueriesOnLocalization, pg_error={}'.format(1))
        with self.burst_db_cursor(consumer_session_ext_only) as cursor:
            self.execute_failing_query(TPCDS_Q1_SQL, error_msg,
                                       consumer_session_ext_only)

        # Case 2: Ensure that the query fails if localization failed with
        # cpp exception and the expceted error message is received in the
        # consumer cluster.
        error_msg = 'Simulated Burst localization failure with cpp exception'
        burst_cluster.set_event(
            'EtFailBurstQueriesOnLocalization, pg_error={}'.format(0))
        with self.burst_db_cursor(consumer_session_ext_only) as cursor:
            self.execute_failing_query(TPCDS_Q1_SQL, error_msg,
                                       consumer_session_ext_only)
