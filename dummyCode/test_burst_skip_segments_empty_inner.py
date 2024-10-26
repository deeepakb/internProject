# Copyright 2019 Amazon.com, Inc. or its affiliates.
# All Rights Reserved.

import logging
import pytest
import time
import datetime

from raff.common.aws_clients.redshift_client import RedshiftClient
from raff.burst.burst_test import (BurstTest, setup_teardown_burst,
                                   is_burst_hydration_on)
from raff.util.utils import run_bootstrap_sql
from raff.common.profile import Profiles
from raff.common.region import DEFAULT_REGION

log = logging.getLogger(__name__)

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst]

test_marker = int(time.time())

CUSTOM_GUCS = dict(burst_mode='3', enable_short_query_bias='false')
CUSTOM_GUCS_PRECOMMIT = dict(enable_short_query_bias='false')

scan_summary = '''
SELECT
SEGMENT,
step,
sum(ROWS)::int AS ROWS,
trim(perm_table_name) AS perm_table,
is_rrscan
FROM
stl_scan
WHERE
QUERY IN (
  SELECT
    query
  FROM
    svl_qlog
  WHERE
    xid IN (
      SELECT
        xid
      FROM
        svl_qlog
      WHERE
        query = {0}
        and starttime > '{1}'
    )
    and starttime > '{1}'
)
AND TYPE in (2,31,29)
GROUP BY
SEGMENT,
step,
perm_table,
is_rrscan
ORDER BY
SEGMENT,
STEP;
'''

query_id_sql = '''
select pg_last_query_id()
'''

query_id_burst = '''
select concurrency_scaling_query from stl_concurrency_scaling_query_mapping
where primary_query = {}
'''

concurrency_scaling_status = '''
select concurrency_scaling_status from svl_query_concurrency_scaling_status
where query={0};
'''

query_list = [
    '''
-- Q0. Right deep tree.
SELECT
  *
FROM
  tpch1_lineitem_redshift,
  tpch1_orders_redshift,
  tpch1_partsupp_redshift
WHERE
  o_orderkey = l_orderkey
  AND l_partkey = ps_partkey
  AND l_suppkey = ps_suppkey
  AND o_totalprice < 100;
''', '''
-- Q1. Left deep tree.
SELECT
  *
FROM
  tpch1_lineitem_redshift,
  tpch1_orders_redshift,
  tpch1_supplier_redshift
WHERE
  o_orderkey = l_orderkey
  AND l_suppkey = s_suppkey
  AND o_totalprice < 22000
  AND s_comment LIKE '%kiran%'
  AND l_extendedprice < 5000;
''', '''
-- Q2. Bush tree.
SELECT
  *
FROM
  tpch1_lineitem_redshift,
  tpch1_orders_redshift,
  tpch1_supplier_redshift,
  tpch_nation_redshift
WHERE
  o_orderkey = l_orderkey
  AND l_suppkey = s_suppkey
  AND o_totalprice < 1000
  AND s_nationkey = n_nationkey
  AND s_acctbal < 2000
  AND n_name LIKE '%kiran%';
''', '''
-- Q3. Bush tree, left outer join on the left side.
SELECT
  *
FROM
  tpch1_lineitem_redshift
  LEFT OUTER JOIN tpch1_orders_redshift ON (
    o_orderkey = l_orderkey
    AND o_totalprice > 100
  ),
  tpch1_supplier_redshift,
  tpch_nation_redshift
WHERE
  l_suppkey = s_suppkey
  AND s_nationkey = n_nationkey
  AND s_acctbal < 2000
  AND n_name LIKE '%kiran%';
''', '''
-- Q4. Left deep tree with second scan from top being empty.
SELECT
  *
FROM
  tpch1_lineitem_redshift,
  tpch1_orders_redshift,
  tpch1_supplier_redshift,
  tpch1_part_redshift
WHERE
  o_orderkey = l_orderkey
  AND l_suppkey = s_suppkey
  AND p_comment LIKE '%lixxs%'
  AND l_extendedprice < 5000
  AND o_totalprice < 50000
  AND s_phone LIKE '%259%'
  AND l_partkey = p_partkey;
''', '''
-- Q5. Full outer join with an empty inner.
SELECT
  count(*)
FROM
  tpch1_lineitem_redshift
  INNER JOIN tpch1_orders_redshift ON o_orderkey = l_orderkey
  AND o_totalprice < 100 FULL
  OUTER JOIN tpch1_partsupp_redshift ON l_partkey = ps_partkey
  AND l_suppkey = ps_suppkey;
''', '''
-- Q6.
select
  s_acctbal,
  count(*)
from
  tpch1_supplier_redshift,
  s3.lineitem_1t_part,
  tpch_nation_redshift
where
  l_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'aaa'
group by
  s_acctbal;
''', '''
-- Q7.
select
  s_acctbal,
  count(*)
from
  tpch1_supplier_redshift,
  s3.lineitem_1g,
  tpch_nation_redshift
where
  l_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'aaa'
group by
  s_acctbal;
''', '''
-- Q8.
SELECT
  *
FROM
  public.tpch_nation_redshift,
  public.tpch1_part_redshift,
  tpch1_supplier_redshift
WHERE
  n_nationkey = p_partkey
  AND n_nationkey = s_nationkey
  AND s_comment LIKE '%kiran%'
  and p_partkey < 0
  and n_nationkey < 0;
''', '''
-- Q9.
SELECT
  *
FROM
  public.tpch_nation_redshift,
  public.tpch1_part_redshift,
  tpch1_supplier_redshift
WHERE
  n_nationkey > p_partkey
  AND n_nationkey = s_nationkey
  AND s_comment LIKE '%kiran%'
  and p_partkey < 0
  and n_nationkey < 0;
'''
]

scan_stats_list = []
q0_scan_result_set = [
    ['0', '0', '0', 'Burst Scan tpch1_orders_redshift', 'f'],
    ['1', '0', '0', 'Burst Scan tpch1_lineitem_redshift', 't'],
    ['3', '0', '0', 'Burst Scan tpch1_partsupp_redshift', 'f']
]
scan_stats_list.append(q0_scan_result_set)
q1_scan_result_set = [[
    '0', '0', '0', 'Burst Scan tpch1_supplier_redshift', 'f'
], ['2', '0', '0', 'Burst Scan tpch1_orders_redshift',
    'f'], ['3', '0', '0', 'Burst Scan tpch1_lineitem_redshift', 't']]
scan_stats_list.append(q1_scan_result_set)
q2_scan_result_set = [
    ['0', '0', '0', 'Burst Scan tpch_nation_redshift', 'f'],
    ['2', '0', '0', 'Burst Scan tpch1_supplier_redshift', 't'],
    ['4', '0', '0', 'Burst Scan tpch1_orders_redshift', 'f'],
    ['5', '0', '0', 'Burst Scan tpch1_lineitem_redshift', 't']
]
scan_stats_list.append(q2_scan_result_set)
q3_scan_result_set = [
    ['0', '0', '0', 'Burst Scan tpch_nation_redshift', 'f'],
    ['2', '0', '0', 'Burst Scan tpch1_supplier_redshift', 't'],
    ['4', '0', '0', 'Burst Scan tpch1_orders_redshift', 'f'],
    ['5', '0', '0', 'Burst Scan tpch1_lineitem_redshift', 't']
]
scan_stats_list.append(q3_scan_result_set)
q4_scan_result_set = [
    ['0', '0', '62', 'Burst Scan tpch1_supplier_redshift', 'f'],
    ['1', '0', '0', 'Burst Scan tpch1_part_redshift', 'f'],
    ['3', '0', '0', 'Burst Scan tpch1_orders_redshift', 'f'],
    ['4', '0', '0', 'Burst Scan tpch1_lineitem_redshift', 't']
]
scan_stats_list.append(q4_scan_result_set)
q5_scan_result_set = [
    ['0', '0', '0', 'Burst Scan tpch1_orders_redshift', 'f'],
    ['1', '0', '0', 'Burst Scan tpch1_lineitem_redshift', 't'],
    ['3', '0', '800000', 'Burst Scan tpch1_partsupp_redshift', 'f']
]
scan_stats_list.append(q5_scan_result_set)
q6_scan_result_set = [[
    '0', '0', '0', 'Burst Scan tpch_nation_redshift', 'f'
], ['2', '0', '0', 'Burst Scan tpch1_supplier_redshift', 't'],
                      ['4', '1', '0', 'S3 Subquery dev_s3_lineitem_1t_part', 't']]
scan_stats_list.append(q6_scan_result_set)
q7_scan_result_set = [['0', '0', '0', 'Burst Scan tpch_nation_redshift', 'f'],
                      [
                          '2', '0', '0', 'Burst Scan tpch1_supplier_redshift',
                          't'
                      ], ['4', '0', '0', 'S3 Subquery dev_s3_lineitem_1g', 't']]
scan_stats_list.append(q7_scan_result_set)
q8_scan_result_set = [[
    '0', '0', '0', 'Burst Scan tpch1_supplier_redshift', 'f'],
    ['2', '0', '0', 'Burst Scan tpch1_part_redshift', 't'],
    ['2', '11', '0', 'Burst Scan tpch_nation_redshift', 'f']]
scan_stats_list.append(q8_scan_result_set)
q9_scan_result_set = [[
    '0', '0', '0', 'Burst Scan tpch1_supplier_redshift', 'f'
], ['2', '0', '0', 'Burst Scan tpch_nation_redshift', 'f'],
                      ['4', '0', '0', 'Burst Scan tpch1_part_redshift', 'f']]
scan_stats_list.append(q9_scan_result_set)

# We will execute all queries in the query list and check scan stats to
# assert that we skipped scanning tables after a empty inner is found.
num_queries = len(query_list)


@pytest.mark.burst
@pytest.mark.serial_only
# Load TPCH data.
@pytest.mark.load_data
@pytest.mark.cluster_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS)
class TestBurstSkipSegmentsEmptyInner(BurstTest):
    """
    Test that we skip scans of tables when a higher up inners hash table is
    empty.
    """

    def get_burst_cluster(self, cluster):
        """
        Get the burst cluster instance.

        Args:
            cluster: Main cluster instance

        Returns: Burst cluster instance if one is attached.
        """
        results = cluster.list_acquired_burst_clusters()
        if len(results) > 0:
            burst_cluster_arn = results[0]
            burst_cluster_id = burst_cluster_arn.split(':cluster:')[-1].strip()
            log.info("Burst cluster id: {}".format(burst_cluster_id))
            client = RedshiftClient(profile=Profiles.QA_BURST_TEST,
                                    region=DEFAULT_REGION)
            return client.describe_cluster(burst_cluster_id)
        else:
            return None

    def test_burst_skip_segments_empty_inner(self, cluster, db_session):
        """
        Test that we skip scans of tables when a higher up inners hash table is
        empty. The following queries are tested on both main and burst cluster.
        1. Unpartitioned Spectrum (External) table.
        2. Partitioned Spectrum (External) table
        3. Work-stealable Burst (Redshift) query
        4. Non work-stealable Burst query
        """
        # Since this is a cluster only test, we cannot set the guc:
        #   1) version switch might wipe them out
        #   2) if we already cold started the cluster will not come up
        #   3) if gucs are not setup correclty we will release the cluster
        # Skip this test for now.
        if is_burst_hydration_on(cluster):
            pytest.skip("Hydration build doesn't use redshift format. "
                        "See https://issues.amazon.com/issues/Burst-2972")
        # Get the handle for burst cluster. We will use this directly query the
        # stats for the burst queries.
        burst_cluster = self.get_burst_cluster(cluster)

        # It is possible that xids get reused. To be safe, filter rows from the
        # test start.
        test_start_time = datetime.datetime.now().replace(microsecond=0)

        for query in range(num_queries):
            with self.burst_db_cursor(db_session) as cursor:
                cursor.execute("set enable_result_cache_for_session to off")
                cursor.execute(query_list[query])
                burst_results = cursor.result_fetchall()
                log.info("Burst results: {}".format(burst_results))
                cursor.execute(query_id_sql)
                primary_query_id = cursor.fetch_scalar()
                log.info("Primary query id for burst query: "
                         "{}".format(primary_query_id))
                run_on_burst = run_bootstrap_sql(
                    cluster,
                    concurrency_scaling_status.format(primary_query_id))
                log.info("Query concurrency scaling status: "
                         " {0}".format(run_on_burst[0][0]))
                message = ("Query did not burst")
                assert int(run_on_burst[0][0]) == int(1), message
                burst_query_id = run_bootstrap_sql(
                    burst_cluster, query_id_burst.format(primary_query_id))
                log.info("Burst query id: {}".format(burst_query_id[0][0]))
            burst_scan_stats = run_bootstrap_sql(
                burst_cluster,
                scan_summary.format(burst_query_id[0][0],
                                    test_start_time))
            log.info("Burst Scan results, q{0}: {1}".format(
                query, burst_scan_stats))
            message = ("Query scan stats did not match. Query: {0} "
                       "Expected: {1} Received: {2}".format(
                           query_list[query], scan_stats_list[query],
                           burst_scan_stats))
            assert burst_scan_stats == scan_stats_list[query], message


# This tests dead code and should be deprecated.
# Skipping for now since it is incompatible with S3 Commit.
@pytest.mark.skip(reason="DP-60653")
@pytest.mark.burst_precommit
@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.load_data
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS_PRECOMMIT)
class TestBurstSkipSegmentsEmptyInnerLocal(BurstTest):
    """
    Test that we skip scans of tables when a higher up inners hash table is
    empty. This is same the above test, except that it runs in simulated burst
    mode and is part of precommit.
    """

    def test_burst_skip_segments_empty_inner_local(self, cluster, db_session):
        """
        Test that we skip scans of tables when a higher up inners hash table is
        empty. The following queries are tested on both main and burst cluster.
        1. Unpartitioned Spectrum (External) table.
        2. Partitioned Spectrum (External) table
        3. Work-stealable Burst (Redshift) query
        4. Non work-stealable Burst query
        """

        # It is possible that xids get reused. To be safe, filter rows from the
        # test start.
        test_start_time = datetime.datetime.now().replace(microsecond=0)

        for query in range(num_queries):
            with self.burst_db_cursor(db_session) as cursor:
                cursor.execute(query_list[query])
                burst_results = cursor.result_fetchall()
                log.info("Burst results: {}".format(burst_results))
                cursor.execute(query_id_sql)
                primary_query_id = cursor.fetch_scalar()
                log.info("Primary query id for burst query: "
                         "{}".format(primary_query_id))
            with self.db.cursor() as cursor:
                cursor.execute(
                    concurrency_scaling_status.format(primary_query_id))
                run_on_burst = cursor.fetch_scalar()
                message = ("Query did not burst")
                assert int(run_on_burst) == int(1), message
                cursor.execute(query_id_burst.format(primary_query_id))
                burst_query_id = cursor.fetch_scalar()
                log.info("Burst query id: {}".format(burst_query_id))
                cursor.execute(scan_summary.format(burst_query_id,
                               test_start_time))
                burst_scan_stats = cursor.result_fetchall()
                log.info("Burst Scan results, q{0}: {1}".format(
                    query, burst_scan_stats))
                message = ("Query scan stats did not match. Query: {0} "
                           "Expected: {1} Received: {2}".format(
                               query_list[query], scan_stats_list[query],
                               burst_scan_stats.rows))
                # scan_stats_list contains each query expected list of lists
                # result set. The list of lists are written to match the return
                # type of run_bootstrap_sql which is used in the above cluster
                # test. But cursor.execute (used here in localhost test)
                # returns a SelectResult object which contains tuples in the
                # list. So we will need to convert the existing list of lists
                # to list of tuples.
                scan_stats = [tuple(elem) for elem in scan_stats_list[query]]
                log.info("scan_stats: {}".format(scan_stats))
                # scan_stats_list contains each query expected list of lists
                # result set. The list elements are written to match the return
                # type of run_bootstrap_sql, which returns all fields in the
                # list as strings. But cursor.execute returns a SelectResult
                # object which contains fields as returned by the client. So we
                # will need to convert the strings that are actually integers
                # to int before comparing.
                scan_stats_expected = [
                    int(x) if x.isdigit() else x for xl in scan_stats
                    for x in xl
                ]
                log.info("scan_stats_expected: {}".format(scan_stats_expected))
                # The above python list comprehension flattens the list of
                # lists to a single list. Before we compare, flatten the
                # returned result list of tuples from the client.
                client_result = [x for xl in burst_scan_stats.rows for x in xl]
                log.info("client_result: {}".format(client_result))
                # assert burst_scan_stats.rows == scan_stats_expected, message
                assert client_result == scan_stats_expected, message
