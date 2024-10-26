# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest
import time

from threading import Thread
from raff.burst.burst_test import (BurstTest, setup_teardown_burst,
                                   burst_build_to_acquire)
from raff.common.dimensions import Dimensions

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst", "burst_build_to_acquire"]

BURST_MODE_3 = {'burst_mode': '3', 'max_concurrency_scaling_clusters': '2'}


@pytest.mark.skip(reason=("Burst-1808: Disabling until burst mode 3 "
                          "can run in functional tests"))
@pytest.mark.usefixtures('setup_teardown_burst')
@pytest.mark.custom_burst_gucs(gucs=BURST_MODE_3)
@pytest.mark.cluster_only
@pytest.mark.serial_only
class TestBurstUpdateCredentialsConcurrentRelease(BurstTest):
    CREDENTIAL_SLOWDOWN_SECONDS = 10

    @classmethod
    def modify_test_dimensions(cls):
        # Events slowing down the credential update in different places.
        return Dimensions(
            dict(event=[
                "EtBurstSimSlowBurstCredsUpdate1",
                "EtBurstSimSlowBurstCredsUpdate2"
            ]))

    def update_credentials(self, cluster):
        cluster.run_xpx('burstop update_credentials')

    def test_burst_update_credentials_concurrent_release(
            self, cluster, vector, burst_build_to_acquire):
        '''
        Test that tries to release clusters during the credentials update.
        '''
        starttime = cluster.get_cluster_start_time()
        # Acquire at least 2 burst clusters such that the update credentials
        # loop does at least 1 iteration.
        cluster.run_xpx('burst_release_all')
        cluster.acquire_burst_cluster(burst_build_to_acquire)
        cluster.acquire_burst_cluster(burst_build_to_acquire)

        with cluster.event(vector.event, 'sleep={}'.format(
                self.CREDENTIAL_SLOWDOWN_SECONDS)):
            # Starts a new thread to update the credentials, so we can release
            # the clusters in parallel.
            thread = Thread(target=self.update_credentials, args=[cluster])
            thread.start()

            # Release the cluster in parallel
            time.sleep(5)
            cluster.run_xpx('burst_release_all')

            # Wait for the thread to finish
            thread.join()

            # Run a simple query to make sure the cluster is up
            cluster.run_xpx('burst_list_acquired')

            # Checks that the cluster didn't restart during the test
            assert starttime == cluster.get_cluster_start_time(
            ), "Cluster crashed during test"


CREDENTIALS_ERROR_TRESHOLD = 3
CREDENTIALS_ERROR_TRESHOLD_GUCS = {
    'burst_credentials_update_errors_threshold': CREDENTIALS_ERROR_TRESHOLD
}
CREDENTIAL_ERRORS_TRACE_SQL = """
select * from stl_event_trace
    where message like '%UpdateCredentials failed%'
"""
CREDENTIAL_ERRORS_STV_SQL = """
select credentials_errors from stv_burst_manager_cluster_info
"""


@pytest.mark.usefixtures('setup_teardown_burst')
@pytest.mark.custom_burst_gucs(gucs=CREDENTIALS_ERROR_TRESHOLD_GUCS)
@pytest.mark.burst_precommit
@pytest.mark.localhost_only
@pytest.mark.load_tpcds_data
@pytest.mark.serial_only
class TestBurstUpdateCredentialsFailures(BurstTest):
    CREDENTIAL_SLOWDOWN_SECONDS = 10

    def test_burst_update_credentials_error(self, cluster, db_session):
        '''
        Test that tries to simulate an error during the credentials update
        process and verifies the cluster is released correctly. Marked as
        localhost only because otherwise we need to set the event on the
        burst cluster and we also need a bootstrap session to query system
        tables.
        '''
        # Run a query to acquire a cluster
        self.execute_test_file('burst_query', session=db_session)

        with cluster.event('EtSimulateRestAgentCredentialsError'
                           ), self.db.cursor() as cursor:

            # Get the initial error count
            cursor.execute(CREDENTIAL_ERRORS_TRACE_SQL)
            cursor.result_fetchall()
            error_count = cursor.rowcount

            # Tries to update credentials multiple times.
            # Since we set an event all of them are expected to fail.
            for i in range(CREDENTIALS_ERROR_TRESHOLD + 1):
                cluster.run_xpx('burstop update_credentials')

                # Checks the error count from stl_event_trace
                cursor.execute(CREDENTIAL_ERRORS_TRACE_SQL)
                cursor.result_fetchall()
                assert error_count + 1 == cursor.rowcount, "Update credentials didn't fail"
                error_count = cursor.rowcount

                if i != CREDENTIALS_ERROR_TRESHOLD:
                    # Checks error count from stv_burst_manager_cluster_info
                    cursor.execute(CREDENTIAL_ERRORS_STV_SQL)
                    results = cursor.result_fetchall()
                    if cursor.rowcount == 0:
                        pytest.fail("No rows in {}".format(
                            "stv_burst_manager_cluster_info"))
                    assert i + 1 == results.rows[0][
                        0], "Update credentials didn't fail"

            # Verify that the cluster is released because we reached the
            # errors threshold.
            cursor.execute('''
                Select action, reason from stl_burst_service_client
                order by eventtime desc
                ''')
            results = cursor.result_fetchall()
            if cursor.rowcount == 0:
                pytest.fail(
                    "No rows in {}".format("stl_burst_service_clients"))
            row = results.rows[0]
            action = row[0].strip()
            reason = row[1].strip()
            assert action == 'RELEASE'
            assert reason == 'FailedUpdateCredentials'
