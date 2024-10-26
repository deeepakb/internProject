# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
import time
import datetime
import uuid

from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst
from raff.common.base_test import FailedTestException
from raff.common.base_test import run_priviledged_query_scalar
from raff.common.base_test import run_priviledged_query_scalar_int

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst]

log = logging.getLogger(__name__)


def verify_acquired(cursor, clusters_to_acquire, starttime):
    start_str = starttime.replace(microsecond=0).isoformat(' ')
    # Verify we acquired the right number of clusters.
    cursor.execute("""
                SELECT * FROM stl_burst_service_client where action='ACQUIRE'
                and len(btrim(error)) = 0 and eventtime >= '{}'
                """.format(start_str))
    cursor.result_fetchall()
    if cursor.rowcount < clusters_to_acquire:
        raise FailedTestException(
            "Failed to acquire {} clusters".format(clusters_to_acquire))


def count_refresh_messages(cursor, starttime, action):
    start_str = starttime.replace(microsecond=0).isoformat(' ')
    # Verify we acquired the right number of clusters.
    cursor.execute("""
                SELECT * FROM stl_burst_manager_refresh where eventtime >= '{}'
                and action = '{}'
                """.format(start_str, action))
    cursor.result_fetchall()
    return cursor.rowcount


def get_burst_cluster_backup_version(cluster, cursor):
    query = """
                SELECT backup_version FROM
                stv_burst_manager_cluster_info
                order by backup_version LIMIT 1
                """
    return run_priviledged_query_scalar_int(cluster, cursor, query)


def get_burst_cluster_arn(cluster, cursor):
    query = """
                SELECT cluster_arn FROM
                stv_burst_manager_cluster_info LIMIT 1
                """
    return run_priviledged_query_scalar(cluster, cursor, query).strip()


BURST_REFRESH_START_SECONDS_V1 = 10
BURST_REFRESH_CHECK_SECONDS_V1 = 10


@pytest.mark.load_tpcds_data
@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.burst_precommit
@pytest.mark.encrypted_only
@pytest.mark.custom_burst_gucs(
    gucs=dict(
        burst_refresh_start_seconds=BURST_REFRESH_START_SECONDS_V1,
        burst_refresh_check_seconds=BURST_REFRESH_CHECK_SECONDS_V1,
        multi_az_refresh_check_frequency_seconds=BURST_REFRESH_START_SECONDS_V1,
        multi_az_enabled='true',
        is_multi_az_primary='true',
        enable_burst_refresh='true'))
class TestMultiAzRefresh(BurstTest):
    def test_multi_az_refresh(self, cluster, db_session):
        '''
        This test verifies that multi az clusters are refreshed properly.
        '''
        start_time = datetime.datetime.now().replace(microsecond=0)

        # When commit based refresh is enabled in simulated mode
        # the live context it's not initilazed, so we need to override it
        # manually
        rest_agent_args = "backup_version=9999"
        burst_manager_args = "sb_version=9999"
        # Set an event such that additional messages are logged and can be
        # used for verification.
        with cluster.event("EtBurstTracing", "level=ElDebug5"), \
             self.auto_release_local_burst(cluster), \
             cluster.event("EtSimulateRestAgentRefresh"), \
             cluster.event("EtSimulateRestAgentCommitRefresh"), \
             cluster.event("EtSimulateRestAgentRefreshVerMismatch", rest_agent_args), \
             cluster.event("EtSimulateS3CommitBasedRefreshClusterVersion", burst_manager_args) :
            # Run a query to make sure we acquire a cluster.
            with self.db.cursor() as cursor:
                self.execute_test_file('burst_query', session=db_session)
                verify_acquired(cursor, 1, start_time)
                start_backup_version = get_burst_cluster_backup_version(
                    cluster, cursor)

                # Convert the burst cluster into a multiAZ cluster
                arn = get_burst_cluster_arn(cluster, cursor)
                cluster.run_xpx(
                    "update_burst_cluster {} MultiAZ true".format(arn))

                # Take a new backup to trigger a refresh that should fail
                # because of the event
                backup_name = "TestBurstManagerRefreshInvoked" + str(
                    uuid.uuid4()).replace('-', '')
                cluster.backup_cluster(backup_name)

                # Each second checks if we triggered a refresh
                for _ in range(0, 4 * BURST_REFRESH_START_SECONDS_V1):
                    time.sleep(1)
                    start_count = count_refresh_messages(
                        cursor, start_time, 'RefreshStart')
                    if start_count == 0:
                        log.info("Refresh didn't start yet")
                    else:
                        end_count = count_refresh_messages(
                            cursor, start_time, 'RefreshEnd')

                        if end_count == 0:
                            log.info("Refresh didn't end yet")
                        else:
                            log.info("Refresh completed succesfully")
                            break

                backup_version = get_burst_cluster_backup_version(
                    cluster, cursor)
                log.info("Refreshed from {} to {}".format(
                    start_backup_version, backup_version))

                assert backup_version != start_backup_version, (
                    "Backup version should change")
