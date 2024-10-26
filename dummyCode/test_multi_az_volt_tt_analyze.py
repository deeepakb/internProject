# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest

from raff.common.node_type import NodeType
from raff.burst.burst_test import BurstTest
pytestmark = pytest.mark.node_info(
    launch_node_type=NodeType.MULTI_AZ_I3EN_XLPLUS,
    unsupported_node_types=NodeType.non_ra3_types)
# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = []

CUSTOM_GUCS = {
    'burst_enable_volt_tts': 'true',
    'burst_volt_tts_require_replay': 'false',
    'use_stairows_as_reltuples': 'false',
    'try_multi_az_first': 'true',
    "selective_dispatch_level": "0",
    'multi_az_enabled': 'true',
    'legacy_stl_disablement_mode': '0'
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
      ORDER BY c_customer_id
"""
EMPTY_TEMP_TABLE_SQL = """
      WITH /* TPC-DS query1.tpl 0.12 */ customer_total_return AS
        (SELECT sr_customer_sk AS ctr_customer_sk,
                sr_store_sk AS ctr_store_sk,
                sum(SR_STORE_CREDIT) AS ctr_total_return
         FROM store_returns,
              date_dim
         WHERE sr_returned_date_sk = d_date_sk
           AND d_year =999999
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
      ORDER BY c_customer_id
"""
QUERY_ID = "select pg_last_query_id()"
EXPLAIN = """
    select btrim(plannode) from stl_explain where query = {}
    and plannode like '%Scan on% volt_tt_%' order by nodeid
"""
"""
Expected row count and row width of the 2 scan steps of TPC-DS query 1.
They are ordered by node id in svcs_explain.
"""
VOLT_TT_EXPECTED_STATS = ["rows=50441 width=24", "rows=50441 width=20"]
EMPTY_VOLT_TT_EXPECTED_STATS = ["rows=910 width=35", "rows=910 width=31"]


@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.load_tpcds_data
@pytest.mark.no_burst_nightly
@pytest.mark.encrypted_only
@pytest.mark.ra3_only
class TestMultiAzVoltTTAnalyze(BurstTest):
    def test_multi_az_volt_tt_analyze_tpcds1(self, cluster, db_session,
                                             cluster_session):
        """
        Test that we have the correct plan stats (num of rows and width) of
        scan step on Volt tt when the Volt tt is not empty. We verify the plan
        stats by comparing it with that of the Redshift query.
        """
        QID = None
        with cluster_session(gucs=CUSTOM_GUCS), \
            cluster.event('EtBurstFindClusterTracing', "level=ElDebug5"), \
            self.db.cursor() as mcursor:
            # Wait for secondary to be reacquired after the restart to
            # change the gucs
            self.wait_for_multi_az_reacquired(cluster)
            # Trigger a backup and a refresh of the burst cluster
            # after loading the data
            self.trigger_backup_and_refresh(cluster)
            mcursor.execute(TPCDS_Q1_SQL)
            mcursor.execute(QUERY_ID)
            QID = mcursor.fetchall()[0][0]
            self.check_last_query_ran_on_multi_az(cluster, mcursor)

            mcursor.execute(EXPLAIN.format(QID))
            scans = mcursor.fetchall()
            assert len(scans) == 2
            volt_tt_actual_stats = []
            for i in range(0, len(scans)):
                scan = scans[i][0]
                idx = scan.find("rows=")
                idx_end = scan.find(")")
                volt_tt_actual_stats.append(scan[idx:idx_end])
            assert volt_tt_actual_stats == VOLT_TT_EXPECTED_STATS

    def test_multi_az_volt_tt_analyze_no_col_stats(self, cluster, db_session,
                                                   cluster_session):
        """
        Test that we have the correct plan stats (num of rows and width) of
        scan step on Volt tt when the Volt tt is empty. We verify the plan
        stats by comparing it with that of the Redshift query.
        """
        QID = None
        with cluster_session(gucs=CUSTOM_GUCS), \
            cluster.event('EtBurstFindClusterTracing', "level=ElDebug5"), \
            self.db.cursor() as mcursor:
            # Wait for secondary to be reacquired after the restart to
            # change the gucs
            self.wait_for_multi_az_reacquired(cluster)
            # Trigger a backup and a refresh of the burst cluster
            # after loading the data
            self.trigger_backup_and_refresh(cluster)
            mcursor.execute(EMPTY_TEMP_TABLE_SQL)
            mcursor.execute(QUERY_ID)
            QID = mcursor.fetchall()[0][0]
            self.check_last_query_ran_on_multi_az(cluster, mcursor)

            mcursor.execute(EXPLAIN.format(QID))
            scans = mcursor.fetchall()
            assert len(scans) == 2
            volt_tt_actual_stats = []
            for i in range(0, len(scans)):
                scan = scans[i][0]
                idx = scan.find("rows=")
                idx_end = scan.find(")")
                volt_tt_actual_stats.append(scan[idx:idx_end])
            assert volt_tt_actual_stats == EMPTY_VOLT_TT_EXPECTED_STATS
