# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
import time

from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst]

log = logging.getLogger(__name__)


def verify_acquired(cursor, clusters_to_acquire):
    # Verify we acquired the right number of clusters.
    cursor.execute("""
                SELECT * FROM stl_burst_service_client where action='ACQUIRE'
                and len(btrim(error)) = 0
                """)
    cursor.result_fetchall()
    if cursor.rowcount < clusters_to_acquire:
        pytest.fail(
            "Failed to acquire {} clusters".format(clusters_to_acquire))

    # Verify we didn't hit any limit.
    cursor.execute("""
                SELECT * FROM stl_burst_prepare where error like
                '%Rate limit for cluster acquisition exceeded%'
                """)
    results = cursor.result_fetchall()
    if cursor.rowcount != 0:
        pytest.fail("Hit rate limit: ".format(results.rows[0]))


def verify_not_acquired(cursor, clusters_to_acquire):
    # Verify we hit the limit.
    cursor.execute("""
                SELECT * FROM stl_burst_prepare where error like
                '%Rate limit for cluster acquisition exceeded%'
                """)
    cursor.result_fetchall()
    if cursor.rowcount < clusters_to_acquire:
        pytest.fail("Didn't hit rate limit")


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.burst_precommit
@pytest.mark.custom_burst_gucs(
    gucs={
        'burst_acquire_limiter_refill_rate_sec': '100',
        'burst_acquire_limiter_max_tokens': '1',
    })
@pytest.mark.serial_only
class TestBurstManagerRateLimiterFast(BurstTest):
    def test_fast_bucket_refill(self, cluster, db_session):
        '''
        This test tries to acquire 10 clusters, even if the limit of max
        acquire token is set only to 1, the refill rate is very fast and
        will refill in time for the next acquisition.
        '''
        clusters_to_acquire = 10
        for i in range(clusters_to_acquire):
            self.execute_test_file('burst_query', session=db_session)
            self.release_localmode_burst(cluster)

        with self.db.cursor() as cursor:
            # Verify we acquired 10 clusters.
            verify_acquired(cursor, clusters_to_acquire)

            # Sleep to refill the tokens
            time.sleep(1)

            # Run one more query to verify we succeed.
            self.execute_test_file('burst_query', session=db_session)
            self.release_localmode_burst(cluster)

            # Verify we acquired 1 clusters.
            verify_acquired(cursor, 1)


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.burst_precommit
@pytest.mark.custom_burst_gucs(
    gucs={
        'burst_acquire_limiter_refill_rate_sec': '0.0000000001',
        'burst_acquire_limiter_max_tokens': '10',
    })
@pytest.mark.serial_only
class TestBurstManagerRateLimiterNoRefill(BurstTest):
    @pytest.mark.skip(reason="DP-29308")
    def test_bucket_no_refill(self, cluster, db_session):
        '''
        This test tries to acquire 10 clusters to reach the limit of the
        max tokens. Since the acquire tokens are never refilled(refill_rate~0)
        any other acquire after the first 10 will fail
        '''
        clusters_to_acquire = 10
        for i in range(clusters_to_acquire):
            self.execute_test_file('burst_query', session=db_session)
            self.release_localmode_burst(cluster)

        with self.db.cursor() as cursor:
            # Verify we acquired 10 clusters.
            verify_acquired(cursor, clusters_to_acquire)

            # Run one more query to verify we fail.
            self.execute_test_file('burst_query', session=db_session)

            # Verify we hit the limit for 1 clusters.
            verify_not_acquired(cursor, 1)
