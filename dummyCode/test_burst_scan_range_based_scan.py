# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
import pytest

from contextlib import contextmanager
from raff.burst.burst_test import (BurstTest, setup_teardown_burst,
                                   verify_query_bursted)

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst, verify_query_bursted]


TPCDS_QUERY_6 = """
    SELECT /* TPC-DS query6.tpl 0.58 */ top 100 a.ca_state state,
                                                           count(*) cnt
    FROM customer_address a ,
         customer c ,
         store_sales s ,
         date_dim d ,
         item i
    WHERE a.ca_address_sk = c.c_current_addr_sk
      AND c.c_customer_sk = s.ss_customer_sk
      AND s.ss_sold_date_sk = d.d_date_sk
      AND s.ss_item_sk = i.i_item_sk
      AND d.d_month_seq =
        (SELECT DISTINCT (d_month_seq)
         FROM date_dim
         WHERE d_year = 1998
           AND d_moy = 1 )
      AND i.i_current_price > 1.2 *
        (SELECT avg(j.i_current_price)
         FROM item j
         WHERE j.i_category = i.i_category)
    GROUP BY a.ca_state
    HAVING count(*) >= 10
    ORDER BY cnt
"""

WORK_STEALING_QUERY = """
    SELECT count(*) FROM store_sales WHERE ss_sold_time_sk > 0;
"""

LAST_QUERY_ID = """
    select pg_last_query_id();
"""

STL_SCAN_QUERY = """
    select row_fetcher_state, consumed_scan_ranges from stl_scan
    where query in
        (select concurrency_scaling_query from
         stl_concurrency_scaling_query_mapping
         WHERE primary_query = {})
    and segment = {} and step = {}
"""

NON_RRSCAN_QUERY = """
    select count(*) from store_sales;
"""

GUCS_ROW_FETCHER_ON = {
    "burst_enable_redshift_row_fetcher": "true",
    "burst_redshift_row_fetcher_enable_workstealing": "true",
    "burst_use_local_scans": "true",
    "enable_result_cache_for_session": "false",
    "enable_row_level_filtering": "false",
    "enable_redshift_row_fetcher_for_write": 0b1001, # Insert and Ctas
}

GUCS_ROW_FETCHER_OFF = {
    "burst_enable_redshift_row_fetcher": "false",
    "burst_redshift_row_fetcher_enable_workstealing": "true",
    "burst_use_local_scans": "true",
    "enable_result_cache_for_session": "false",
    "enable_row_level_filtering": "false",
    "enable_redshift_row_fetcher_for_write": 0b1001, # Insert and Ctas
}

GUCS_BURST_SCAN_RANGES_FOR_RRSCAN_ON = {
    "burst_enable_redshift_row_fetcher": "true",
    "burst_redshift_row_fetcher_enable_workstealing": "true",
    "burst_use_local_scans": "true",
    "enable_result_cache_for_session": "false",
    "enable_row_level_filtering": "false",
    "enable_redshift_row_fetcher_non_rrscan": "true",
    "enable_redshift_row_fetcher_for_write": 0b1001, # Insert and Ctas
}

GUCS_BURST_SCAN_RANGES_FOR_RRSCAN_OFF = {
    "burst_enable_redshift_row_fetcher": "true",
    "burst_redshift_row_fetcher_enable_workstealing": "true",
    "burst_use_local_scans": "true",
    "enable_result_cache_for_session": "false",
    "enable_row_level_filtering": "false",
    "enable_redshift_row_fetcher_non_rrscan": "off",
    "enable_redshift_row_fetcher_for_write": 0b1001, # Insert and Ctas
}

NO_ROW_FETCHER = 0
ROW_FETCHER_NO_WORK_STEALING = 1
ROW_FETCHER_WORK_STEALING = 2


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.usefixtures("setup_teardown_burst")
class TestBurstScanRangeBasedScanBase(BurstTest):
    """
    Base class for scan-range-based scan on Burst.
    """
    @contextmanager
    def burst_db_session(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst")
            yield cursor
            cursor.execute("reset query_group")

    def _check_row_fetcher_state(self, scan_step, row_fetcher_state):
        """
        Helper function to check row fetcher state in stl_scan for a given
        scan step.
        """
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(
                STL_SCAN_QUERY.format(scan_step['query'], scan_step['segment'],
                                      scan_step['step']))
            result = bootstrap_cursor.fetchall()
            # Check if all slices use row fetcher.
            assert len(result) > 0
            for row in result:
                assert int(row[0]) == row_fetcher_state

    def _test_routine(self, db_session, test_query, segment_to_check,
                      row_fetcher_state):
        """
        Helper function to run a Burst query and check the row fetcher state.
        """
        with self.burst_db_session(db_session) as cursor:
            cursor.execute(test_query)
            cursor.execute(LAST_QUERY_ID)
            scan_step = {
                'query': cursor.fetch_scalar(),
                'segment': segment_to_check,
                'step': 0
            }
            self._check_row_fetcher_state(scan_step, row_fetcher_state)


@pytest.mark.custom_burst_gucs(gucs=GUCS_ROW_FETCHER_OFF)
class TestBurstScanRangeBasedScanStl(TestBurstScanRangeBasedScanBase):
    """
    Tests for scan-range-based scan on Burst cluster.
    """
    def test_row_fetcher_stl_columns(self, db_session, verify_query_bursted):
        """
        Simple test to check if we can correctly query the new columns in
        stl_scan. Test with disabled row fetcher.
        """
        # Simple query for testing these two columns.
        test_query = ("select count(*) from store_sales "
                      "where ss_sold_time_sk > 0;")

        qid = None

        # Execute a Burst query.
        with self.burst_db_session(db_session) as cursor:
            cursor.execute(test_query)
            cursor.execute(LAST_QUERY_ID)
            qid = cursor.fetch_scalar()

        # Examine stl_scan.
        with self.db.cursor() as bootstrap_cursor:
            # Get stl_scan entry of the previous burst query. Both segment
            # and step are 0 because this is the first scan on perm table.
            bootstrap_cursor.execute(STL_SCAN_QUERY.format(qid, 0, 0))
            rows = bootstrap_cursor.fetchall()
            assert len(rows) > 0
            for row in rows:
                assert int(row[0]) == NO_ROW_FETCHER
                assert int(row[1]) == 0


@pytest.mark.custom_burst_gucs(gucs=GUCS_ROW_FETCHER_ON)
class TestBurstScanRangeBasedScanEnable(TestBurstScanRangeBasedScanBase):
    def test_burst_row_fetcher_guc_on(self, db_session, cluster,
                                      verify_query_bursted):
        """
        Verify we use row fetcher on Burst if burst_enable_redshift_row_fetcher
        is on.
        """
        self._test_routine(db_session, TPCDS_QUERY_6, 0,
                           ROW_FETCHER_NO_WORK_STEALING)


@pytest.mark.custom_burst_gucs(gucs=GUCS_BURST_SCAN_RANGES_FOR_RRSCAN_ON)
class TestBurstRowFetcherNonRRscanOn(TestBurstScanRangeBasedScanBase):
    def test_burst_row_fetcher_non_rrscan_on(self, db_session, cluster,
                                             verify_query_bursted):
        """
        Verify we can use work-stealing of row fetcher on Burst local scan.
        """
        self._test_routine(db_session, NON_RRSCAN_QUERY, 0,
                           ROW_FETCHER_WORK_STEALING)
