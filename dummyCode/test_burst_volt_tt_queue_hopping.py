# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest

from raff.burst.burst_test import (BurstTest, setup_teardown_burst)
from raff.util.utils import run_bootstrap_sql

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst"]

WLM_CONFIGS = ('[{"query_group":["burst"], "user_group":["burst"], '
               '"concurrency_scaling":"auto", "query_concurrency": 2}, '
               '{"query_group":["noburst"], "user_group":["noburst"], '
               '"query_concurrency": 5}]')

CUSTOM_GUCS = {
    'wlm_json_configuration': WLM_CONFIGS,
    'burst_mode': '3',
    'burst_enable_volt_tts': 'true',
    'burst_volt_tts_require_replay': 'false'
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
VALIDATION_QUERY = """
SELECT substring(q.querytxt, 1, 20) as query_text,
       q.concurrency_scaling_status,
       w.service_class
FROM stl_wlm_query w,
     stl_query q
WHERE w.query = q.query
AND   (q.pid, q.xid) IN (SELECT pid, xid
                             FROM stl_query
                             WHERE query = {} LIMIT 1)
AND q.starttime >= '{}'::timestamp
ORDER BY q.starttime;
"""
VALIDATION_QUERY2 = """
SELECT q.concurrency_scaling_status, q.aborted
FROM stl_query q
WHERE q.xid = (SELECT xid FROM stl_query WHERE query = {} LIMIT 1)
  ORDER BY q.query, q.concurrency_scaling_status, q.aborted;
"""


@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS)
@pytest.mark.load_tpcds_data
@pytest.mark.usefixtures("setup_teardown_burst")
class TestBurstVoltTTQueueHopping(BurstTest):
    """
    This test runs the first CTAS of a Volt TT query on the Burst cluster, but
    then moves the following Volt-generated query to a non-burst queue and
    verifies that the query bursted nevertheless.
    """

    def test_burst_volt_tt_queue_hopping(self, cluster, db_session):
        # Note that the service class id needs to remain stable based on
        # above WLM config. Should it change, please update it here. It needs
        # to be the second, non-burstable, service class.
        QID = None
        with cluster.event('EtWlmBurstForceSCForVoltTT', 'service_class=7'):
            with self.burst_db_cursor(db_session) as cursor:
                cursor.execute("SELECT now();")
                TIMESTAMP = cursor.fetchall()[0][0]
                cursor.execute(TPCDS_Q1_SQL)
                # We expect that the query succeeds and returns 0 rows.
                assert cursor.fetchall() == []
                cursor.execute(QUERY_ID)
                QID = cursor.fetchall()[0][0]

        # Validate concurrency scaling status and service classes.
        result = run_bootstrap_sql(cluster,
                                   VALIDATION_QUERY.format(QID, TIMESTAMP))
        assert result == [['CREATE TEMP TABLE vo', '1', '6'],
                          [' WITH /* TPC-DS quer', '1', '7']]

    def test_burst_volt_tt_undo(self, cluster, db_session):
        # Ensures that we do not attempt to Burst an UNDO query if we are in a
        # sticky Volt session. Note that if we were bursting the UNDO query,
        # then the test would get stuck. The stuck query would only repro in
        # JDBC testing.
        error_msg = '.*This type of correlated subquery pattern.*'
        with cluster.event('EtAssertFinalVoltCTAS'):
            with self.burst_db_cursor(db_session) as cursor:
                cursor.execute("BEGIN;")
                cursor.execute("CREATE TABLE burst_volt_tt_undo (c1 int);")
                # Run a query that uses Volt temporary tables and, hence,
                # creates a sticky session. The event will fail this query after
                # the final Volt-generated CTAS finished.
                self.execute_failing_query(TPCDS_Q1_SQL, error_msg, db_session)
                cursor.execute("ABORT;")
