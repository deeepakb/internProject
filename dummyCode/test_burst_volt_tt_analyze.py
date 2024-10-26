# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest

from raff.burst.burst_test import (BurstTest, setup_teardown_burst,
                                   verify_query_bursted)

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst", "verify_query_bursted"]

CUSTOM_GUCS = {
    'burst_mode': '3',
    'burst_enable_volt_tts': 'true',
    'burst_volt_tts_require_replay': 'false',
    'use_stairows_as_reltuples': 'false'
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
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS)
@pytest.mark.load_tpcds_data
@pytest.mark.usefixtures("setup_teardown_burst")
class TestBurstVoltTTAnalyze(BurstTest):
    @pytest.mark.localhost_only
    def test_burst_volt_tt_analyze_tpcds1(self, db_session,
                                          verify_query_bursted):
        """
        Test that we have the correct plan stats (num of rows and width) of
        scan step on Volt tt when the Volt tt is not empty. We verify the plan
        stats by comparing it with that of the Redshift query.
        """
        QID = None
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(TPCDS_Q1_SQL)
            cursor.execute(QUERY_ID)
            QID = cursor.fetchall()[0][0]

        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(EXPLAIN.format(QID))
            scans = bootstrap_cursor.fetchall()
            assert len(scans) == 2
            volt_tt_actual_stats = []
            for i in range(0, len(scans)):
                scan = scans[i][0]
                idx = scan.find("rows=")
                idx_end = scan.find(")")
                volt_tt_actual_stats.append(scan[idx:idx_end])
            assert volt_tt_actual_stats == VOLT_TT_EXPECTED_STATS

    def test_burst_volt_tt_analyze_no_col_stats(self, db_session,
                                                verify_query_bursted):
        """
        Test that we have the correct plan stats (num of rows and width) of
        scan step on Volt tt when the Volt tt is empty. We verify the plan
        stats by comparing it with that of the Redshift query.
        """
        QID = None
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(EMPTY_TEMP_TABLE_SQL)
            cursor.execute(QUERY_ID)
            QID = cursor.fetchall()[0][0]

        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(EXPLAIN.format(QID))
            scans = bootstrap_cursor.fetchall()
            assert len(scans) == 2
            volt_tt_actual_stats = []
            for i in range(0, len(scans)):
                scan = scans[i][0]
                idx = scan.find("rows=")
                idx_end = scan.find(")")
                volt_tt_actual_stats.append(scan[idx:idx_end])
            assert volt_tt_actual_stats == EMPTY_VOLT_TT_EXPECTED_STATS
