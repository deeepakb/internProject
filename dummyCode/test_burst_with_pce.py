# Copyright 2024 Amazon.com, Inc. or its affiliates.
# All Rights Reserved.

import logging
import pytest
import time

from raff.burst.burst_test import (BurstTest, setup_teardown_burst,
                                   burst_build_to_acquire)
from raff.common.aws_clients.redshift_client import RedshiftClient
from raff.common.profile import Profiles
from raff.common.region import DEFAULT_REGION
from raff.util.utils import run_bootstrap_sql

log = logging.getLogger(__name__)

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst", "burst_build_to_acquire"]


@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(
    gucs={
        'enable_burst_async_acquire': 'false',
        'burst_propagated_static_gucs':
        '{"padbGucs": ['
        '{"name": "force_use_precompiled_executor", "value": "true"},'
        '{"name": "can_use_precompiled_executor", "value": "true"},'
        '{"name": "can_use_precompiled_executor_on_ds", "value": "true"},'
        '{"name": "can_use_precompiled_executor_on_burst", "value": "true"},'
        '{"name": "skip_compilation_with_executor", "value": "true"},'
        '{"name": "max_vectorization_buffer_size", "value": "1024000"},'
        '{"name": "log_stl_executor_steps", "value": "2"},'
        '{"name": "precompiled_executor_sq_cutoff_seconds", "value": "0"}'
        ']}',
    })
class TestBurstWithPCE(BurstTest):
    """
    Test that burst cluster uses precompiled executors.
    """

    def _acquire_and_personalize_burst_cluster(
            self, cluster, cursor, burst_build_to_acquire):
        """
        This will release any previously attached burst clusters and create
        and personalize a new burst cluster.
        """
        self._acquire_burst_cluster(cluster, burst_build_to_acquire)
        burst_arn = self._get_burst_cluster_arn(cluster)
        cluster.personalize_burst_cluster(burst_arn)
        cluster.run_xpx("burst_start_refresh")
        # Waiting for refresh to complete before continuing
        time.sleep(10)

        assert self._is_burst_cluster_personalized(cluster), (
            "Burst cluster is not personalized.")

    def _acquire_burst_cluster(self, cluster, burst_build_to_acquire):
        """
        This will release any previously attached burst clusters and acquire a
        new burst cluster.
        """
        log.info("Release any previously attached burst clusters.")
        cluster.release_all_burst_clusters()
        log.info("Acquire a burst cluster.")
        cluster.acquire_burst_cluster(burst_build_to_acquire)

    def _get_burst_cluster(self, cluster):
        """
        Get the burst cluster instance.
        Returns: Burst cluster instance if one is attached.
        """
        burst_cluster_arn = self._get_burst_cluster_arn(cluster)
        burst_cluster_id = burst_cluster_arn.split(':cluster:')[-1].strip()
        log.info("Burst cluster id: {}".format(burst_cluster_id))
        client = RedshiftClient(
            profile=Profiles.QA_BURST_TEST, region=DEFAULT_REGION)
        return client.describe_cluster(burst_cluster_id)

    def _verify_run_stats(self, burst_cluster, burst_query_id):
        """
        Asserts that on the burst cluster, at least one segment ran using PCEs.
        """
        query = """
          with cinfo as (
            select path
            from stl_compile_info
            where query = {0} and cachehit = 13
          union all
            select path
            from stl_bkg_compile_info
            where query = {0} and cachehit = 13)

          select count(distinct path) from cinfo
        """.format(burst_query_id)

        num_of_pce_segs = run_bootstrap_sql(burst_cluster, query)[0][0]

        assert int(num_of_pce_segs) > 0, (
            "With PCEs enabled, the number of PCE segments should be a "
            "positive number.")

    def _run_burstable_query_in_main_cluster(self, cursor, burst_cluster):
        # Issue the query on the main cluster, this will be run on
        # burst cluster.
        cursor.execute("""
          set query_group to burst;
          select a, b from tbl2 limit 2;
        """)
        query_id = cursor.last_query_id()
        log.info("Query ID on main cluster: {}".format(query_id))

        sql_get_burst_query_id = """
          select concurrency_scaling_query
          from stl_concurrency_scaling_query_mapping
          where primary_query = {}
        """.format(query_id)
        burst_query_id = run_bootstrap_sql(
            burst_cluster, sql_get_burst_query_id)[0][0]
        log.info("Query ID on burst cluster: {}".format(burst_query_id))

        return burst_query_id

    def _run_query_directly_in_burst_cluster(self, burst_cluster):
        # Because we need to run this query before personalization,
        # we need to issue query in the burst cluster. If we run
        # a burstable query in the main cluster, it will personalize
        # burst cluster automatically.
        test_query = "select * from tbl limit 2"
        run_bootstrap_sql(burst_cluster, test_query)

        # When we don't have the session where the previous query is executed,
        # we need to use this SQL to get the query id.
        sql_get_previous_query_id = """
          select query from stl_query
          where querytxt like '%{}%'
          order by starttime desc limit 1;
        """.format(test_query.strip())
        burst_query_id = run_bootstrap_sql(burst_cluster,
                                           sql_get_previous_query_id)[0][0]
        log.info("Query ID on burst cluster: {}".format(burst_query_id))
        return burst_query_id

    def _is_burst_cluster_personalized(self, cluster):
        burst_cluster_arn = self._get_burst_cluster_arn(cluster)

        sql_is_burst_cluster_personalized = """
          select count(*) from stl_burst_manager_personalization
          where trim(cluster_arn)='{}'
        """.format(burst_cluster_arn)

        is_burst_cluster_personalized = run_bootstrap_sql(
            cluster, sql_is_burst_cluster_personalized)[0][0]
        return int(is_burst_cluster_personalized) > 0

    def _get_burst_cluster_arn(self, cluster):
        results = cluster.list_acquired_burst_clusters()
        assert len(results) > 0, ("Cannot get burst cluster arn.")
        return results[0].strip()

    def test_burst_works_with_pce_before_personalization(
            self, db_session, cluster, burst_build_to_acquire):
        if cluster.get_guc_value('can_use_precompiled_executor') != 'on':
            pytest.skip("This test requires PCE enabled")

        self._acquire_burst_cluster(cluster, burst_build_to_acquire)
        burst_cluster = self._get_burst_cluster(cluster)
        run_bootstrap_sql(burst_cluster, """
          create table tbl (a int, b int);
          insert into tbl values (1, 2), (3, 4), (5, 6)
        """)
        burst_query_id = self._run_query_directly_in_burst_cluster(
            burst_cluster)
        self._verify_run_stats(burst_cluster, burst_query_id)
        assert self._is_burst_cluster_personalized(cluster) is False, (
            "Burst cluster is personalized.")

    def test_burst_works_with_pce_after_personalization(
            self, db_session, cluster, burst_build_to_acquire):
        if cluster.get_guc_value('can_use_precompiled_executor') != 'on':
            pytest.skip("This test requires PCE enabled")

        with db_session.cursor() as cursor:
            cursor.execute("""
              create table tbl2 (a int, b int);
              insert into tbl2 values (1, 2), (3, 4), (5, 6);
            """)

        with self.burst_db_cursor(db_session) as cursor:
            self._acquire_and_personalize_burst_cluster(
                cluster, cursor, burst_build_to_acquire)
            burst_cluster = self._get_burst_cluster(cluster)
            burst_query_id = self._run_burstable_query_in_main_cluster(
                cursor, burst_cluster)
            self._verify_run_stats(burst_cluster, burst_query_id)
