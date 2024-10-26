# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid
from raff.burst.burst_test import BurstTest
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions
from raff.burst.burst_test import setup_teardown_burst
from raff.data.data_utils import load_table

log = logging.getLogger(__name__)
__all__ = [setup_teardown_burst]

STL_SKR_SCAN_METRICS = """ SELECT segment,
       col,
       observation_type,
       sum(observed_rrscan_rows_filtered),
       sum(observed_post_rrscan_rows_filtered)
FROM   stl_skr_scan_metrics
WHERE  query = {}
GROUP  BY 1,
          2,
          3
ORDER BY segment, col, observation_type
"""

SKR_METRICS_DEBUG_QUERY = """ SELECT *
FROM stl_skr_scan_metrics
WHERE query = {}
ORDER BY segment, slice
"""

RRSCAN_QUERY_W_FILTER_PREDICATE = """ SELECT COUNT(*)
FROM customer
WHERE c_nationkey >= 12
"""

RRSCAN_QUERY_W_JOIN_PREDICATE = """ SELECT COUNT(*)
FROM orders o
INNER JOIN
(
    SELECT c_custkey
    FROM customer
    ORDER BY 1 LIMIT 100
) c ON o.o_custkey = c.c_custkey
"""

MAPPING = """ SELECT concurrency_scaling_query
FROM stl_concurrency_scaling_query_mapping
WHERE primary_query = {}
"""


@pytest.yield_fixture(scope="class")
def setup_data_for_local_burst_mode(cluster):
    """
    Load the data to be used in the test. We create one copy each for queries
    that should burst and those that shouldn't. This allows us to test equivalence
    between the metrics captured for both cases.

    Args:
        db_session: db_session object
    """
    with DbSession(cluster.get_conn_params()) as db_session:
        db_session.cursor().execute("drop table if exists customer")
        db_session.cursor().execute("drop table if exists orders")
        load_table(
            db=db_session,
            dataset_name='tpch',
            table_name='customer',
            scale='1',
            table_name_suffix='',
            grant_to_public=True)
        load_table(
            db=db_session,
            dataset_name='tpch',
            table_name='orders',
            scale='1',
            table_name_suffix='',
            grant_to_public=True)
        SNAPSHOT_IDENTIFIER = ("{}-{}".format(cluster.cluster_identifier,
                                              str(uuid.uuid4().hex)))
        cluster.backup_cluster(SNAPSHOT_IDENTIFIER)
        yield
        db_session.cursor().execute("drop table if exists customer")
        db_session.cursor().execute("drop table if exists orders")


@pytest.mark.serial_only
@pytest.mark.localhost_only
# Skip these tests in Precompiled Executors configurations.
# PCE scans do not yet implement the logging to `stl_skr_scan_metrics`.
@pytest.mark.cg_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.usefixtures("setup_data_for_local_burst_mode")
class TestBurstSkrScanMetrics(BurstTest):

    def _verify_skr_metrics_equivalence(self, main_qid, burst_qid):
        '''
        Verifies that the rows scanned during the query running on main
        are the same as the query running in burst mode.

        Args:
            main_qid: id of query that ran on main
            burst_qid: id of query that ran on burst
        '''
        with self.db.cursor() as cursor:
            cursor.execute(STL_SKR_SCAN_METRICS.format(main_qid))
            main_stats = cursor.fetchall()
            assert len(main_stats) > 0
            cursor.execute(MAPPING.format(burst_qid))
            burst_qid = cursor.fetch_scalar()
            log.info("Burst query id after mapping: {}".format(burst_qid))
            cursor.execute(STL_SKR_SCAN_METRICS.format(burst_qid))
            burst_stats = cursor.fetchall()
            assert len(burst_stats) > 0
            if not main_stats == burst_stats:
                cursor.execute(SKR_METRICS_DEBUG_QUERY.format(main_qid))
                main_stats = cursor.fetchall()
                log.info("Metrics on main: {}".format(main_stats))
                cursor.execute(SKR_METRICS_DEBUG_QUERY.format(burst_qid))
                burst_stats = cursor.fetchall()
                log.info("Metrics on burst: {}".format(burst_stats))
                assert False, (
                    "Difference in skr scan metrics. See RAFF log output for details"
                )

    def run_query_do_verification(self, query, cursor, cluster):
        # Run the query on main.
        cursor.execute("set query_group to metrics")
        cursor.execute(query)
        main_qid = self.last_query_id(cursor)
        log.info("Main query id: {}".format(main_qid))
        self.verify_query_didnt_bursted(cluster, main_qid)

        # Run the query on burst.
        cursor.execute("set query_group to burst")
        cursor.execute(query)
        burst_qid = self.last_query_id(cursor)
        log.info("Burst query id: {}".format(burst_qid))
        self.verify_query_bursted(cluster, burst_qid)

        self._verify_skr_metrics_equivalence(main_qid, burst_qid)


    @pytest.mark.session_ctx(user_type='super')
    def test_burst_skr_scan_metrics(self, cluster, db_session):
        """
        Verifies that stl_skr_scan_metrics is populated on burst clusters.
        This is not using a real burst cluster/super simulated mode, however
        it gets the burst query id and makes sure that the stl is populated
        in burst mode.
        """
        with db_session.cursor() as cursor:
            self.run_query_do_verification(RRSCAN_QUERY_W_FILTER_PREDICATE,
                                           cursor, cluster)
            self.run_query_do_verification(RRSCAN_QUERY_W_JOIN_PREDICATE,
                                           cursor, cluster)
