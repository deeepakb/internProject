# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import json
import logging
import pytest
import time
import datetime
from contextlib import contextmanager
import threading

from raff.common import retry
from raff.common.base_test import (run_priviledged_query,
                                   run_priviledged_query_scalar_int)
from raff.common.cluster.cluster_session import ClusterSession
from raff.common.db.db_exception import Error
from raff.common.db.redshift_db import RedshiftDb
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import (setup_teardown_burst,
                                   verify_query_didnt_burst)
from raff.system_tests.suites.burst_suite import log
from raff.util.utils import PropagatingThread

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst, verify_query_didnt_burst]

log = logging.getLogger(__name__)
LONG_QUERY = "select count(*) from web_sales A, web_sales B, web_sales C;"
LONG_BURST_QUERY = """
    set query_group to burst;
    select count(*) from catalog_sales A, catalog_sales B, catalog_sales C"""
INFLIGHT_BURST_QUERIES = """
    set query_group to metrics;
    select pid from stv_inflight where label = 'burst'"""
QUERY_WAITING_ACQUIRE_COUNT = """
    set query_group to metrics;
    select nvl(sum(count),0) from stv_burst_manager_async_queries"""
ACQUIRE_COUNT_QUERY = """
    set query_group to metrics;
    SELECT * FROM stl_burst_service_client where action=
    'ACQUIRE' and len(btrim(error)) = 0 and eventtime > '{}'"""
CANCEL_QUERY = "select pg_cancel_backend({})"
INFLIGHT_QUERIES = """
    set query_group to metrics;
    select pid from stv_inflight where pid<>pg_backend_pid()"""
RAN_ON_BURST_CHECK = """
    set query_group to metrics;
    select count(*) from stl_query where concurrency_scaling_status = 1 and
    pid like '{}' and starttime > '{}'"""
RAN_ON_MAIN_CHECK = """
    set query_group to metrics;
    select count(*) from stl_query where concurrency_scaling_status = 0 and
    pid like '{}' and starttime > '{}'"""
RUNNING_QUERY_PIDS = """
    SELECT pid from stv_wlm_query_state join stv_inflight on
    stv_wlm_query_state.query=stv_inflight.query where
    service_class = {} and state = 'Running'"""
UNMARK_ON_MAIN_CHECK = """
    set query_group to metrics;
    select count(*) from stl_burst_async_mark where event ilike
    '%Unmark: run on main%' and pid like '{}' and eventtime > '{}'"""
UNMARK_ON_BURST_CHECK = """
    set query_group to metrics;
    select count(*) from stl_burst_async_mark where event ilike
    '%Unmark: run on burst%' and pid like '{}' and eventtime > '{}'"""
UNMARK_ON_QUERY_CONPLETE = """
    set query_group to metrics;
    select count(*) from stl_burst_async_mark where event ilike
    '%Unmark: query complete%' and pid like '{}' and eventtime > '{}'"""
QUEUED_QUERY_COUNT = """
    SELECT count(*) from stv_wlm_query_state where state = 'QueuedWaiting'"""
QUERY_CONCURRENCY = 2
WLM_CFG = [{
    "query_group": ["burst"],
    "user_group": ["burst"],
    "concurrency_scaling": "auto",
    "query_concurrency": QUERY_CONCURRENCY
}, {
    "query_group": ["noburst"],
    "user_group": ["noburst"],
    "query_concurrency": 5
}]
GUCS = dict(
    wlm_json_configuration=json.dumps(WLM_CFG),
    try_burst_first='false',
    enable_result_cache_for_session="false",
    enable_burst_async_acquire='true')


@pytest.mark.load_tpcds_data
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.serial_only
@pytest.mark.no_jdbc
class TestBurstAsyncPrepare(BurstTest):
    main_queue_blocking_query = 0
    # If run_long_burst_query_thread or run_burst_query_thread is called,
    # the query pid is recorded here.
    burst_query = 0

    def teardown(self):
        self.kill_inflight_queries()

    @retry(retries=10, delay_seconds=2)
    def verify_acquired(self, cluster, cursor, clusters_to_acquire, starttime):
        # Verify we acquired the right number of clusters.
        result = run_priviledged_query(cluster, self.db.cursor(),
                                       ACQUIRE_COUNT_QUERY.format(starttime))
        assert clusters_to_acquire == len(result), \
            "Failed to acquire {} clusters, acquired {} instead" \
            .format(clusters_to_acquire, len(result))

    def run_long_burst_query_thread(self, cluster):
        conn_params = cluster.get_conn_params()
        ctx = SessionContext(user_type='super')
        with DbSession(conn_params, session_ctx=ctx).cursor() as cursor:
            cursor.execute("select pg_backend_pid()")
            self.burst_query = cursor.fetch_scalar()
            cursor.execute(LONG_BURST_QUERY)

    def run_burst_query_thread(self, cluster):
        conn_params = cluster.get_conn_params()
        ctx = SessionContext(user_type='super')
        user_session = DbSession(conn_params, session_ctx=ctx)
        with user_session.cursor() as cursor:
            cursor.execute("select pg_backend_pid()")
            self.burst_query = cursor.fetch_scalar()
        self.execute_test_file('burst_query', session=user_session)

    def kill_inflight_queries(self, pids=None):
        with self.db.cursor() as cursor:
            if not pids:
                cursor.execute(INFLIGHT_QUERIES)
                rows = cursor.fetchall()
                pids = [row[0] for row in rows]
            for pid in pids:
                log.info("Killing {}".format(pid))
                cursor.execute("select pg_cancel_backend({})".format(pid))

    def run_blocking_query(self, cluster):
        conn_params = cluster.get_conn_params()
        ctx = SessionContext(user_type='super')
        with DbSession(conn_params, session_ctx=ctx).cursor() as cursor:
            cursor.execute("set enable_result_cache_for_session to off")
            cursor.execute("set query_group to burst")
            cursor.execute(LONG_QUERY)
            cursor.fetchall()
            log.info("Blocking query finished.")

    @retry(retries=10, delay_seconds=2)
    def _ensure_query_running(self, cluster, num_running, service_class=6):
        """
        Ensure the query is running on Main.

        Args:
            num_running (int): Number of queries supposed to be running.
            service_class (int): Service class of the WLM query is in.
        """
        with RedshiftDb(cluster.get_conn_params()).cursor() as cursor:
            cursor.execute('set query_group to metrics')
            cursor.execute(RUNNING_QUERY_PIDS.format(service_class))
            pids = [row[0] for row in cursor.fetchall()]
            assert len(pids) == num_running, \
                "Queries in main queue hasn't start running"
        return pids

    @contextmanager
    def main_queue_full(self, cluster):
        """
        Helper function that blocks main queue of burst query_group.
        """
        with cluster.event("EtBurstFreezeQueueForTesting",
                           "seconds={},group=burst".format(20)):
            for _ in range(QUERY_CONCURRENCY):
                thread = PropagatingThread(
                    target=self.run_blocking_query, args=(cluster, ))
                thread.start()
            pids = self._ensure_query_running(
                cluster, num_running=QUERY_CONCURRENCY)
        yield
        self.kill_inflight_queries(pids=pids)

    @retry(retries=10, delay_seconds=2)
    def verify_acquire_map_size(self, cluster, target):
        '''
        Verifies the total number of queries waiting for acquire is correct.
        '''
        result = run_priviledged_query(cluster, self.db.cursor(),
                                      QUERY_WAITING_ACQUIRE_COUNT)
        count = 0 if len(result[0]) == 0 else result[0][0]
        assert count == target

    @pytest.mark.localhost_only
    def test_stv_burst_manager_async_queries_permission(self, cluster):
        """
        This test verifies that only bootstrap user can access table
        stv_burst_manager_async_queries.
        """
        QUERY = 'select * from stv_burst_manager_async_queries'
        PERMISSION_ERROR = \
            'permission denied for relation stv_burst_manager_async_queries'
        conn_params = cluster.get_conn_params()

        ctx1 = SessionContext(user_type='super')
        ctx2 = SessionContext(user_type='regular')
        ctx3 = SessionContext(user_type='bootstrap')

        with DbSession(conn_params, session_ctx=ctx1).cursor() as super, \
                DbSession(conn_params, session_ctx=ctx2).cursor() as regular, \
                    DbSession(conn_params, session_ctx=ctx3).cursor() as bs:
            bs.execute(QUERY)
            try:
                super.execute(QUERY)
                pytest.fail("""Super user should not have access to
                               stv_burst_manager_async_queries""")
            except Error as e:
                assert PERMISSION_ERROR in e.pgerror
            try:
                regular.execute(QUERY)
                pytest.fail("""Regular user should not have access to
                               stv_burst_manager_async_queries""")
            except Error as e:
                assert PERMISSION_ERROR in e.pgerror

    @pytest.mark.cluster_only
    @pytest.mark.skip("reenabling after burst fixture is fixed: DP-32054")
    def test_burst_async_prepare_cancel(self, cluster):
        '''
        This test verifies that cancelling a query before its ran on burst
        will successfully remove itself from burst prepare count.
        We do this by freezing the main queue, and simulate a slow burst
        acquire, so that the query starts preparing but does not run on
        burst. We verify that the map is successfully updated and no more
        burst clusters get prepared.
        '''
        cluster_session = ClusterSession(cluster)
        with cluster_session(gucs=GUCS):
            # Release all burst clusters.
            cluster.run_xpx('burst_release_all')
            starttime = datetime.datetime.now().replace(microsecond=0)

            with self.main_queue_full(cluster), \
                    cluster.event("EtBurstTracing", "level=ElDebug5"), \
                    cluster.event("EtSimulateBurstSlownessBeforeAcquire",
                    "sleep=100"):
                # Set up a long running query.
                # This query cannot run on burst because of
                # EtSimulateBurstSlownessBeforeAcquire;
                # cannot run on main because of frozen queue
                # (self.main_queue_full).
                # As a result this query should wait.
                thread = PropagatingThread(
                    target=self.run_burst_query_thread, args=(cluster, ))
                thread.start()
                # Verify that query is waiting for a burst cluster.
                # self.verify_acquire_map_size(cluster, 1)

                conn_params = cluster.get_conn_params()
                with RedshiftDb(conn_params).cursor() as cursor:
                    # Cancel 1 waiting query and verify.
                    cursor.execute(QUEUED_QUERY_COUNT)
                    result = cursor.fetch_scalar()
                    assert result == 1
                    pid = self.burst_query
                    cursor.execute(CANCEL_QUERY.format(int(pid)))
                    # Verify cancel was successful.
                    assert cursor.fetchone()[0] == 1
                    cursor.execute(QUEUED_QUERY_COUNT)
                    result = cursor.fetch_scalar()
                    assert result == 0

            conn_params = cluster.get_conn_params()
            with RedshiftDb(conn_params).cursor() as cursor:
                # Verify that the query unmarked itself from burst prepare
                # correctly.
                count = run_priviledged_query_scalar_int(
                    cluster, self.db.cursor(),
                    UNMARK_ON_QUERY_CONPLETE.format(pid, starttime))
                assert count == 1

                # verify that no more query is waiting for burst cluster.
                self.verify_acquire_map_size(cluster, 0)

    @pytest.mark.cluster_only
    @pytest.mark.skip("reenabling after burst fixture is fixed: DP-32054")
    def test_burst_async_prepare_not_queued_run_on_main(self, cluster):
        '''
        This test verifies that a query will run on main when a slot is open,
        without enqueuing itself on the prepare queue.
        '''
        cluster_session = ClusterSession(cluster)
        with cluster_session(gucs=GUCS):
            # Release all existing burst clusters.
            cluster.run_xpx('burst_release_all')
            starttime = datetime.datetime.now().replace(microsecond=0)

            with cluster.event("EtBurstTracing", "level=ElDebug5"), \
                cluster.event("EtSimulateBurstSlownessBeforeAcquire",
                "sleep=100"):
                # Set up a long running query.
                thread = PropagatingThread(
                    target=self.run_burst_query_thread, args=(cluster, ))
                thread.start()
                # Verify that query is not waiting for a burst cluster.
                self.verify_acquire_map_size(cluster, 0)

            thread.join()
            pid = self.burst_query
            mastercur = self.db.cursor()
            # Verify that the above query is running on main.
            count = run_priviledged_query_scalar_int(cluster, mastercur,
                                                     RAN_ON_MAIN_CHECK.format(
                                                         pid, starttime))
            assert count == 1
            count = run_priviledged_query_scalar_int(
                cluster, mastercur, UNMARK_ON_MAIN_CHECK.format(
                    pid, starttime))
            assert count == 1
            count = run_priviledged_query_scalar_int(cluster, mastercur,
                                                     RAN_ON_BURST_CHECK.format(
                                                         '%', starttime))
            assert count == 0
