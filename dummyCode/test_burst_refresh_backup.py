# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
import time
import datetime
import uuid
import json
import multiprocessing

from contextlib import contextmanager
from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import get_burst_cluster_name
from raff.burst.burst_test import setup_teardown_burst
from raff.common.base_test import FailedTestException
from raff.common.aws_clients.redshift_client import RedshiftClient
from raff.common.host_type import HostType
from raff.common.profile import Profiles
from raff.common.region import Regions
from raff.util.utils import run_bootstrap_sql
from raff.common.dimensions import Dimensions

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


def count_refresh_start_messages(cursor, starttime):
    start_str = starttime.replace(microsecond=0).isoformat(' ')
    # Verify we acquired the right number of clusters.
    cursor.execute("""
                SELECT * FROM stl_event_trace where eventtime >= '{}'
                and message like '%Received refresh backup request%'
                """.format(start_str))
    cursor.result_fetchall()
    return cursor.rowcount


def get_burst_cluster_backup_version(cluster, cursor):
    query = """
                SELECT backup_version FROM
                stv_burst_manager_cluster_info
                order by backup_version LIMIT 1
                """
    return int(run_priledged_query(cluster, cursor, query)[0][0])


def get_burst_cluster_refresh_version(cluster, cursor):
    query = """
                SELECT refresh_backup_version FROM
                stv_burst_manager_cluster_info
                order by backup_version LIMIT 1
                """
    return int(run_priledged_query(cluster, cursor, query)[0][0])


def get_burst_cluster_arn(cluster, cursor):
    query = """
                SELECT cluster_arn FROM
                stv_burst_manager_cluster_info LIMIT 1
                """
    return run_priledged_query(cluster, cursor, query)[0][0].strip()


def get_burst_cluster_number(cluster, cursor):
    query = """
                SELECT count(*) FROM
                stv_burst_manager_cluster_info
                """
    return int(run_priledged_query(cluster, cursor, query)[0][0])


def run_priledged_query(cluster, bootstrap_cursor, query):
    if cluster.host_type == HostType.CLUSTER:
        nested_lst = run_bootstrap_sql(cluster, query)
        nested_lst_of_tuples = [tuple(l) for l in nested_lst]
        return nested_lst_of_tuples
    else:
        bootstrap_cursor.execute(query)
        return bootstrap_cursor.fetchall()


def wait_for_refresh_start(cluster, cursor, start_str, timeout_sec):
    success = False
    for iteration in range(0, timeout_sec):
        time.sleep(1)

        query = """
        SELECT count(*) FROM stl_burst_manager_refresh
        where eventtime >= '{}'
        and action like '%RefreshStart%'
                    """.format(start_str)
        start = int(run_priledged_query(cluster, cursor, query)[0][0])
        log.info("Waiting for refresh to start")
        if start > 0:
            log.info("Refresh started")
            success = True
            break
    if not success:
        raise FailedTestException(
            "Refresh was not started on main cluster within {} seconds.".
            format(timeout_sec))


def wait_for_query_execution(cluster, cursor, start_str, timeout_sec,
                             cluster_arn):
    success = False
    for iteration in range(0, timeout_sec):
        time.sleep(1)

        query = """
        SELECT count(*) FROM stl_burst_query_execution
        where starttime >= '{}'
        and cluster_arn like '%{}%'
        and action ilike '%FETCH_TOTAL%'
                    """.format(start_str, cluster_arn)
        start = int(run_priledged_query(cluster, cursor, query)[0][0])
        log.info("Waiting for query to end on cluster {}".format(cluster_arn))
        if start > 0:
            log.info("Query ended on cluster {}".format(cluster_arn))
            success = True
            break
    if not success:
        raise FailedTestException(
            "Query didn't end on cluster {} within {} seconds.".format(
                cluster_arn, timeout_sec))


@contextmanager
def autocleanup_processes(mastercur, cursor, schema, cluster):
    # Run a query that will be frozen in the main queue using a
    # different user (master user using the self.db cursor)
    # such that the process will not be blocked
    process_main = multiprocessing.Process(
        target=concurrent_query, args=(mastercur, schema))
    process_main.start()

    # Sleep to ensure the master user query runs before the user one
    time.sleep(5)

    # Run a query that will try to burst but it will not be able to
    # because the burst cluster backup version is too low.
    # This query should trigger a refresh if it didn't start yet.
    process_burst = multiprocessing.Process(
        target=concurrent_query, args=(cursor, schema))
    process_burst.start()

    yield

    # Cleanup the queries in flight
    query = "SELECT pid from stv_inflight where userid > 1"
    rows = run_priledged_query(cluster, mastercur, query)
    for row in rows:
        pid = row[0]
        if pid:
            query = "select pg_cancel_backend({})".format(pid)
            rows = run_priledged_query(cluster, mastercur, query)

    process_main.terminate()
    process_burst.terminate()


def concurrent_query(cursor, schema):
    cursor.execute("set query_group to burst")
    cursor.execute("select * from {}.davide".format(schema))


def get_burst_cluster_refresh_info(cursor):
    cursor.execute("""
                SELECT refresh_backup_version,refresh_backup_id FROM
                stv_burst_manager_cluster_info LIMIT 1
                """)
    return cursor.fetchone()


def get_last_successful_backup_info(cursor):
    cursor.execute("""
                SELECT sb_version, backup_id
                FROM   stl_backup_leader
                WHERE  in_progress = 0
                       AND partial = 0
                       AND error_code = 0
                ORDER  BY starttime DESC
                LIMIT  1
                """)
    return cursor.fetchone()


BURST_REFRESH_START_SECONDS_V1 = 10
BURST_REFRESH_CHECK_SECONDS_V1 = 10


@pytest.mark.load_tpcds_data
@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.burst_precommit
@pytest.mark.custom_burst_gucs(
    gucs=dict(
        burst_refresh_start_seconds=BURST_REFRESH_START_SECONDS_V1,
        burst_refresh_check_seconds=BURST_REFRESH_CHECK_SECONDS_V1,
        enable_burst_refresh='true'))
class TestBurstManagerRefresh(BurstTest):
    @classmethod
    def modify_test_dimensions(cls):
        # Events simulating errors on the burst cluster.
        return Dimensions(
            dict(event=[
                "EtSimulateRestAgentRefreshError",
                "EtSimulateRestAgentRefreshIdMismatch"
            ]))

    def test_burst_manager_refresh_error_handling(self, cluster, vector,
                                                  db_session):
        '''
        This test simulates an error during the refresh process and verifies
        that this is handled correctly on the burst manager side.
        '''
        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')

        # Set an event such that additional messages are logged and can be
        # used for verification.
        with cluster.event("EtBurstTracing", "level=ElDebug5"), \
             self.auto_release_local_burst(cluster), \
             cluster.event("EtSimulateRestAgentRefresh"):
            # Run a query to make sure we acquire a cluster.
            with self.db.cursor() as cursor:
                self.execute_test_file('burst_query', session=db_session)
                verify_acquired(cursor, 1, start_time)
                start_backup_version = get_burst_cluster_backup_version(
                    cluster, cursor)

                with cluster.event(vector.event):
                    # Take a new backup to trigger a refresh that should fail
                    # because of the event
                    backup_name = "TestBurstManagerRefreshInvoked" + str(
                        uuid.uuid4()).replace('-', '')
                    cluster.backup_cluster(backup_name)

                    # Each second checks if we triggered a refresh
                    for iteration in range(0,
                                           4 * BURST_REFRESH_START_SECONDS_V1):
                        time.sleep(1)
                        count = count_refresh_start_messages(
                            cursor, start_time)
                        if count >= 1:
                            num_clusters = get_burst_cluster_number(
                                cluster, cursor)
                            if num_clusters == 0:
                                log.info("Refresh failed and released")
                                query = """
                                        SELECT reason
                                        from stl_burst_service_client
                                        where eventtime >= '{}'
                                        and action ilike '%release%'
                                        order by eventtime desc
                                        """.format(start_str)

                                if vector.event == 'EtSimulateRestAgentRefreshError':
                                    reason_txt = 'CheckRefreshError'
                                else:
                                    reason_txt = 'RefreshIdMismatch'

                                reason = run_priledged_query(
                                    cluster, cursor, query)[0][0]
                                assert reason.strip() == reason_txt, (
                                    "Cluster was not released after error during refresh"
                                )
                                break
                            else:
                                log.info("Refresh started but didn't release")
                                backup_version = get_burst_cluster_backup_version(
                                    cluster, cursor)
                                assert backup_version == start_backup_version, (
                                    """Refresh increased backup version of the
                                    cluster even in presence of errors""")
                        else:
                            log.info("Refresh didn't start yet")


BURST_UPDATE_CREDS_SECONDS = 10
BURST_REFRESH_START_SECONDS_V2 = 10
BURST_REFRESH_CHECK_SECONDS_V2 = 1000


@pytest.mark.load_tpcds_data
@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.burst_precommit
@pytest.mark.custom_burst_gucs(
    gucs=dict(
        enable_burst_s3_commit_based_refresh='false',
        enable_burst_s3_commit_based_cold_start='false',
        enable_burst_refresh_changed_tables='false',
        burst_update_creds_seconds=BURST_UPDATE_CREDS_SECONDS,
        burst_refresh_start_seconds=BURST_REFRESH_START_SECONDS_V2,
        burst_refresh_check_seconds=BURST_REFRESH_CHECK_SECONDS_V2,
        enable_burst_refresh='true'))
class TestBurstManagerRefreshNonBlocking(BurstTest):
    def test_burst_manager_refresh_non_blocking(self, cluster, db_session):
        '''
        This test triggers a refresh using an event such that the refresh
        process takes very long.
        In the meantime an update credential message is sent too.
        The test verifies that the update credential is not blocked by long
        refresh request and that refresh requests are not issued while one
        of them is already in progress.
        '''
        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')

        # Set an event such that additional messages are logged and can be
        # used for verification.
        with cluster.event(
                "EtBurstTracing",
                "level=ElDebug5"), self.auto_release_local_burst(
                    cluster), cluster.event("EtSimulateRestAgentRefresh"):
            # Set an event to simulate successful refresh requests.
            # TODO(nbhanoor): Introduce a context manager that accepts multiple
            # events. This event does not need to be unset specifically after
            # the test pass/fail because "setup_teardown_burst" fixture ends up
            # restarting the cluster.
            cluster.set_event("EtSimulateRestAgentRefresh")
            # Run a query to make sure we acquire a cluster.
            with self.db.cursor() as cursor:
                self.execute_test_file('burst_query', session=db_session)
                verify_acquired(cursor, 1, start_time)
                # Take a new backup to trigger a refresh
                backup_name = "TestBurstManagerRefreshInvoked" + str(
                    uuid.uuid4()).replace('-', '')
                cluster.backup_cluster(backup_name)

                credentials_updated = False
                concurrent_refresh = False
                # Each second checks if we triggered a refresh
                for iteration in range(0, 4 * BURST_REFRESH_START_SECONDS_V2):
                    time.sleep(1)
                    # Get the timestamp of the refresh request
                    cursor.execute("""
                    SELECT min(eventtime) FROM stl_event_trace
                    where eventtime >= '{}'
                    and message like '%Received refresh backup request%'
                                """.format(start_str))
                    min_event_time = cursor.fetch_scalar()
                    if min_event_time is None:
                        continue

                    # Check that the refresh version/id of the clusters is
                    # the same as the latest backup version/id
                    burst_refresh_info = get_burst_cluster_refresh_info(cursor)
                    backup_info = get_last_successful_backup_info(cursor)
                    assert burst_refresh_info[0] == backup_info[
                        0], "Refresh backup version not set correctly"
                    assert burst_refresh_info[1].strip() == backup_info[
                        1].strip(), "Refresh backup id not set correctly"

                    # Check if there is an update credentials message after
                    # the refresh start
                    cursor.execute("""
                    SELECT * from stl_event_trace where eventtime >= '{}'
                    and message like '%Begin Updating credentials%'
                    """.format(min_event_time))
                    cursor.result_fetchall()
                    credentials_updated = (cursor.rowcount >= 1)

                    # Check if there is an refresh message during the first
                    # refresh
                    cursor.execute("""
                    SELECT * from stl_event_trace where eventtime > '{}'
                    and message like '%Received refresh backup request%'
                    """.format(min_event_time))
                    cursor.result_fetchall()
                    concurrent_refresh = (cursor.rowcount >= 1)

                    if concurrent_refresh:
                        raise FailedTestException(
                            "Refresh should not be attempted if a refresh is "
                            "already in progress")

                if not credentials_updated:
                    raise FailedTestException(
                        "Update credentials wasn't triggered during refresh.")


@pytest.mark.no_jdbc
@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.load_tpcds_data
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(
    gucs=dict(
        enable_burst_s3_commit_based_refresh='false',
        enable_burst_s3_commit_based_cold_start='false',
        enable_burst_refresh_changed_tables='false',
        burst_refresh_start_seconds=BURST_REFRESH_START_SECONDS_V1,
        burst_refresh_check_seconds=BURST_REFRESH_CHECK_SECONDS_V1,
        enable_burst_refresh='true'))
class TestBurstManagerRefreshDisabling(BurstTest):
    @contextmanager
    def auto_enable_refresh(self, cluster):
        cluster.run_xpx("burst_enable_refresh")
        yield
        cluster.run_xpx("burst_enable_refresh")

    def test_burst_manager_refresh_disabling(self, cluster, db_session):
        '''
        This test triggers a refresh on a burst cluster where
        enable_burst_refresh is set to false (simulated on burst with event)
        and then checks that refresh is disabled on main too.
        '''
        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')

        # Set an event such that additional messages are logged and can be
        # used for verification.
        with cluster.event("EtBurstTracing", "level=ElDebug5"), \
             self.auto_release_local_burst(cluster), \
             self.auto_enable_refresh(cluster), \
             cluster.event("EtDisableBurstRefreshOnBurst"):
            # Run a query to make sure we acquire a cluster.
            with self.db.cursor() as cursor:
                self.execute_test_file('burst_query', session=db_session)
                verify_acquired(cursor, 1, start_time)

                # Take a new backup to trigger a refresh
                backup_name = "TestBurstManagerDisabling" + str(
                    uuid.uuid4()).replace('-', '')
                cluster.backup_cluster(backup_name)

                success = False
                # Each second checks if we triggered a refresh
                for iteration in range(0, 4 * BURST_REFRESH_START_SECONDS_V1):
                    time.sleep(1)

                    query = """
                    SELECT * FROM stl_burst_manager_refresh
                    where eventtime >= '{}'
                    and error like '%Burst refresh is disabled%'
                                """.format(start_str)
                    cursor.execute(query)
                    result = cursor.fetchall()
                    if len(result) > 0:
                        success = True
                        break
                    else:
                        log.info("Refresh not disabled yet")
                if not success:
                    raise FailedTestException(
                        "Refresh was not disabled on main cluster.")


BURST_REFRESH_START_SECONDS_V3 = 10
WLM_CFG = [{
    "query_group": ["burst"],
    "user_group": ["burst"],
    "concurrency_scaling": "auto",
    "query_concurrency": 1
}, {
    "query_group": ["noburst"],
    "user_group": ["noburst"],
    "query_concurrency": 5
}]


@pytest.mark.no_jdbc
@pytest.mark.cluster_only
@pytest.mark.serial_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(
    gucs=dict(
        enable_burst_s3_commit_based_refresh='false',
        enable_burst_s3_commit_based_cold_start='false',
        enable_burst_refresh_changed_tables='false',
        burst_propagated_static_gucs='{"padbGucs": ['
        '{"name": "enable_burst_s3_commit_based_refresh", "value": "false"},'
        '{"name": "enable_burst_refresh_changed_tables", "value": "false"}]}',
        burst_refresh_start_seconds=BURST_REFRESH_START_SECONDS_V3,
        burst_refresh_check_seconds=100000,
        burst_refresh_expiration_seconds=100000,
        burst_mode=3,
        enable_burst_refresh='true',
        try_burst_first='false',
        wlm_json_configuration=json.dumps(WLM_CFG),
        enable_burst_async_acquire='false'))
class TestBurstManagerRefreshNoAcquisition(BurstTest):
    def test_burst_manager_refresh_no_acquire(self, cluster, db_session):
        '''
        This test tries to burst a query on a superblock version that is not
        available on any burst cluster. When the new backup is detected refresh
        will start. The expected Burst Manager behavior in this case is to
        not acquire any more burst cluster, but wait for refresh to finish.
        In order to simulate a long refresh we are checking for refresh
        completion only after a long period of time
        (burst_refresh_check_seconds).
        We are also setting the refresh expiration to be a very long time such
        that the refresh will never expire no matter how long the refresh will
        run.
        This test cannot use try_burst_first otherwise the query will
        simply run on main when we don't have a cluster ready with the right
        superblock version. For this reason we are artificially freezing the
        queue with the smallest amount of query_concurrency possible such that
        queries will be forced to burst.
        '''
        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')

        # Set an event such that additional messages are logged and can be
        # used for verification.
        with cluster.event("EtBurstTracing", "level=ElDebug5"), \
            cluster.event("EtBurstFindClusterTracing", "level=ElDebug5"), \
            cluster.event("EtBurstFreezeQueueForTesting", "seconds=10000,group=burst"), \
            db_session.cursor() as cursor, \
            self.db.cursor() as mastercur:

            # Personalize the cluster such that refresh is triggered
            cluster_arn = get_burst_cluster_arn(cluster, mastercur)
            # Personalize the cluster such that refresh is triggered
            cluster.run_xpx(
                'burst_personalize_latest_backup {}'.format(cluster_arn))

            cursor.execute("set query_group to noburst")
            cursor.execute("CREATE TABLE if not exists davide (col1 int)")
            cursor.execute("INSERT INTO davide VALUES (1)")

            num_clusters = get_burst_cluster_number(cluster, mastercur)
            assert num_clusters == 1, "Should have 1 acquired cluster"

            initial_cluster_backup_version = get_burst_cluster_backup_version(
                cluster, mastercur)
            log.info("Initial backup version {}".format(
                initial_cluster_backup_version))

            # Take a new backup to trigger a refresh
            backup_name = "TestBurstManagerRefreshNoAcquire" + str(
                uuid.uuid4()).replace('-', '')
            cluster.backup_cluster(backup_name)

            schema = db_session.session_ctx.schema
            with autocleanup_processes(mastercur, cursor, schema, cluster):

                success = False
                # Loop with an arbitrary timeout of 10 mins to prevent the
                # test from hanging forever in case of errors
                for iteration in range(0, 10 * 60):
                    time.sleep(1)

                    # In all the scenarios below we are never supposed to
                    # acquire more burst clusters
                    num_clusters = get_burst_cluster_number(cluster, mastercur)
                    assert num_clusters == 1, "Should have 1 acquired cluster"

                    # We can have the following situations:
                    # 1) Refresh didn't start yet
                    # 2) Refresh started but not completed
                    # 3) Refresh completed but query didn't finish yet
                    # 4) Refresh completed and query run on refreshed cluster

                    cluster_backup_version = get_burst_cluster_backup_version(
                        cluster, mastercur)
                    refresh_version = get_burst_cluster_refresh_version(
                        cluster, mastercur)
                    if refresh_version == -1 and \
                       initial_cluster_backup_version == cluster_backup_version:
                        # We are in case 1), this should be impossible because
                        # the burst query should trigger the refresh on demand
                        raise FailedTestException(
                            "Query failed to trigger refresh")

                    if refresh_version != -1:
                        # In this case the refresh started but is not completed
                        # yet. This is normal because we are not checking for
                        # refresh completion because we want to simulate a long
                        # refresh. In this case we trigger an xpx to mark the
                        # refresh complete after waiting 5 seconds such that if
                        # there is a bug in the WLM, the query has time to be
                        # scheduled on main instead of burst.
                        log.info("Refresh started")
                        time.sleep(5)
                        cluster.run_xpx('burst_check_refresh')
                        continue

                    if refresh_version == -1 and \
                       initial_cluster_backup_version < cluster_backup_version:
                        log.info(
                            "Refresh completed with backup version {}".format(
                                cluster_backup_version))
                        query = """
                                SELECT count(*)
                                from stl_burst_query_execution
                                where starttime >= '{}'
                                """.format(start_str)
                        count = int(
                            run_priledged_query(cluster, mastercur,
                                                query)[0][0])
                        if count == 0:
                            # Case 3
                            log.info(
                                """ Refresh is complete but query didn't"""
                                """ run on burst yet""")
                            continue
                        else:
                            # Case 4
                            log.info("Query run on burst")
                            success = True
                            break

                num_clusters = get_burst_cluster_number(cluster, mastercur)
                assert num_clusters == 1, "Should have 1 acquired cluster"

            if not success:
                raise FailedTestException("Failed to burst refreshed query")


BURST_REFRESH_EXPIRATION_SECONDS_V3 = 10


@pytest.mark.no_jdbc
@pytest.mark.cluster_only
@pytest.mark.serial_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(
    gucs=dict(
        burst_refresh_start_seconds=BURST_REFRESH_START_SECONDS_V3,
        burst_refresh_check_seconds=100000,
        burst_refresh_expiration_seconds=BURST_REFRESH_EXPIRATION_SECONDS_V3,
        burst_mode=3,
        enable_burst_refresh='true',
        try_burst_first='false',
        wlm_json_configuration=json.dumps(WLM_CFG)))
class TestBurstManagerRefreshExpirationAcquisition(BurstTest):
    def wait_for_cluster_acquisition(self, cluster, cursor, start_str,
                                     timeout_sec, reason, other_cluster):
        cluster_arn = None
        error = ""
        for iteration in range(0, timeout_sec):
            time.sleep(1)

            query = """
            SELECT reason, cluster_arn, error FROM stl_burst_service_client
            where eventtime >= '{}'
            and action like '%ACQUIRE%'
            and cluster_arn not like '%{}%'
            order by eventtime desc limit 1
                        """.format(start_str, other_cluster)
            results = run_priledged_query(cluster, cursor, query)
            log.info("Waiting for cluster to be acquired")
            if len(results) > 0:
                actual_reason = results[0][0]
                cluster_arn = results[0][1]
                error = results[0][2]
                log.info(
                    "Cluster {} acquired for reason {}, error {}".format(
                        cluster_arn, actual_reason, error))
                assert actual_reason.strip() == reason
                break
        if not cluster_arn:
            raise FailedTestException(
                "Cluster was not acquired within {} seconds.".format(
                    timeout_sec))
        return [cluster_arn, error]

    def verify_query_single_cluster(self, cluster, cursor, start_str):
        # Verifies that only one cluster was used for this query
        query = """
                SELECT count(distinct(cluster_arn))
                from stl_burst_query_execution
                where starttime >= '{}'
                """.format(start_str)
        count = int(run_priledged_query(cluster, cursor, query)[0][0])
        assert count == 1, "Query ran on 2 different clusters"

    @pytest.mark.skip(reason="DP-23907")
    def test_burst_manager_refresh_expiration(self, cluster, db_session):
        '''
        This test tries to burst a query on a superblock version that is not
        available on any burst cluster. When the new backup is detected refresh
        will start, but this test will simulate an expiration of the refresh.
        The expected Burst Manager behavior in this case is to
        acquire a new burst cluster, instead of waiting for refresh to finish.
        In order to simulate a long refresh we are never checking for refresh
        completion such that it will expire in the meantime, so we are also
        setting the refresh expiration to be quite short such that
        the refresh will expire soon.
        This test cannot use try_burst_first otherwise the query will
        simply run on main when we don't have a cluster ready with the right
        superblock version. For this reason we are artificially freezing the
        queue with the smallest amount of query_concurrency possible such that
        queries will be forced to burst.
        '''
        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')

        burst_name = get_burst_cluster_name(cluster)
        if burst_name is None:
            raise ValueError("No burst clusters acquired")

        # Set an event such that additional messages are logged and can be
        # used for verification.
        with cluster.event("EtBurstTracing", "level=ElDebug5"), \
            cluster.event("EtBurstFindClusterTracing", "level=ElDebug5"), \
            cluster.event("EtBurstFreezeQueueForTesting", "seconds=10000,group=burst"), \
            db_session.cursor() as cursor, \
            self.db.cursor() as mastercur:

            # Personalize the cluster such that refresh is triggered
            cluster_arn = get_burst_cluster_arn(cluster, mastercur)
            # Personalize the cluster such that refresh is triggered
            cluster.run_xpx(
                'burst_personalize_latest_backup {}'.format(cluster_arn))

            cursor.execute("set query_group to noburst")
            cursor.execute("CREATE TABLE if not exists davide (col1 int)")
            cursor.execute("INSERT INTO davide VALUES (1)")

            num_clusters = get_burst_cluster_number(cluster, mastercur)
            assert num_clusters == 1, "Should have 1 acquired cluster"

            initial_cluster_backup_version = get_burst_cluster_backup_version(
                cluster, mastercur)
            log.info("Initial backup version {}".format(
                initial_cluster_backup_version))

            # Take a new backup to trigger a refresh
            backup_name = "TestBurstManagerRefreshNoAcquire" + str(
                uuid.uuid4()).replace('-', '')
            cluster.backup_cluster(backup_name)

            schema = db_session.session_ctx.schema
            with autocleanup_processes(mastercur, cursor, schema, cluster):

                wait_for_refresh_start(cluster, mastercur, start_str,
                                       4 * BURST_REFRESH_START_SECONDS_V1)
                acq_results = self.wait_for_cluster_acquisition(
                    cluster, mastercur, start_str,
                    4 * BURST_REFRESH_EXPIRATION_SECONDS_V3, '0B 0S 1E 0P 1T',
                    burst_name)
                cluster_arn = acq_results[0]
                error = acq_results[1]
                if "NoRequestedPadbAvailableException" in error:
                    # This test is still valid if we can't acquire because the
                    # version is not installed, but we need to skip the checks
                    # below.
                    return
                assert get_burst_cluster_number(
                    cluster, mastercur) == 2, "Should have 2 cluster"
                # Verify the query actually ran on the new cluster
                wait_for_query_execution(
                    cluster, mastercur, start_str,
                    10 * BURST_REFRESH_EXPIRATION_SECONDS_V3, cluster_arn)
                assert get_burst_cluster_number(
                    cluster, mastercur) == 2, "Should have 2 cluster"
                self.verify_query_single_cluster(cluster, mastercur, start_str)
                assert get_burst_cluster_number(
                    cluster, mastercur) == 2, "Should have 2 cluster"

                # Check the refresh to cleanup the state for the next run and
                # release the additional cluster
                cluster.run_xpx('burst_check_refresh')
                cluster.run_xpx('burst_release {}'.format(cluster_arn))


@pytest.mark.no_jdbc
@pytest.mark.cluster_only
@pytest.mark.serial_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(
    gucs=dict(
        burst_refresh_start_seconds=BURST_REFRESH_START_SECONDS_V3,
        burst_refresh_check_seconds=100000,
        burst_refresh_expiration_seconds=BURST_REFRESH_EXPIRATION_SECONDS_V3,
        burst_mode=3,
        enable_burst_refresh='true',
        try_burst_first='false',
        wlm_json_configuration=json.dumps(WLM_CFG),
        max_concurrency_scaling_clusters=1,
        enable_burst_async_acquire='false'))
class TestBurstManagerRefreshExpirationAcquisitionLimit(BurstTest):
    def wait_for_cluster_limit(self, cluster, cursor, start_str, timeout_sec):
        success = False
        for iteration in range(0, timeout_sec):
            time.sleep(1)

            query = """
                SELECT count(*)
                from stl_burst_prepare
                where starttime >= '{}'
                and error like
                '%Maximum allowed limit of burst clusters%'
                    """.format(start_str)
            limit_count = int(
                run_priledged_query(cluster, cursor, query)[0][0])

            if limit_count > 0:
                log.info("Query hit the cluster limit")
                success = True
                break
            else:
                log.info("Query didn't try to acquire a new " "cluster yet")
        if not success:
            raise FailedTestException(
                "Query didn't try to acquire a new cluster within {} seconds.".
                format(timeout_sec))

    def test_burst_manager_refresh_expiration_limit(self, cluster, db_session):
        '''
        This test tries to burst a query on a superblock version that is not
        available on any burst cluster. When the new backup is detected refresh
        will start, but this test will simulate an expiration of the refresh.
        The expected Burst Manager behavior in this case is to
        acquire a new burst cluster, instead of waiting for refresh to finish
        but this acquisition will fail because it will reach the limit of
        acquirable clusters.
        '''
        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')

        # Set an event such that additional messages are logged and can be
        # used for verification.
        with cluster.event("EtBurstTracing", "level=ElDebug5"), \
            cluster.event("EtBurstFindClusterTracing", "level=ElDebug5"), \
            cluster.event("EtBurstFreezeQueueForTesting", "seconds=10000,group=burst"), \
            db_session.cursor() as cursor, \
            self.db.cursor() as mastercur:

            # Personalize the cluster such that refresh is triggered
            cluster_arn = get_burst_cluster_arn(cluster, mastercur)
            # Personalize the cluster such that refresh is triggered
            cluster.run_xpx(
                'burst_personalize_latest_backup {}'.format(cluster_arn))

            cursor.execute("set query_group to noburst")
            cursor.execute("CREATE TABLE if not exists davide (col1 int)")
            cursor.execute("INSERT INTO davide VALUES (1)")

            num_clusters = get_burst_cluster_number(cluster, mastercur)
            assert num_clusters == 1, "Should have 1 acquired cluster"

            initial_cluster_backup_version = get_burst_cluster_backup_version(
                cluster, mastercur)
            log.info("Initial backup version {}".format(
                initial_cluster_backup_version))

            # Take a new backup to trigger a refresh
            backup_name = "TestBurstManagerRefreshNoAcquire" + str(
                uuid.uuid4()).replace('-', '')
            cluster.backup_cluster(backup_name)

            schema = db_session.session_ctx.schema
            with autocleanup_processes(mastercur, cursor, schema, cluster):

                wait_for_refresh_start(cluster, mastercur, start_str,
                                       4 * BURST_REFRESH_START_SECONDS_V1)
                self.wait_for_cluster_limit(
                    cluster, mastercur, start_str,
                    4 * BURST_REFRESH_EXPIRATION_SECONDS_V3)
                assert get_burst_cluster_number(
                    cluster, mastercur) == 1, "Should have 1 cluster"

                # Check the refresh to cleanup the state for the next run
                cluster.run_xpx('burst_check_refresh')
