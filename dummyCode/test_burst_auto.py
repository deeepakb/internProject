# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

from contextlib import contextmanager
from raff.common.db.redshift_db import RedshiftDb
from psycopg2 import InternalError
import logging
import threading
import uuid
import copy

import pytest
from time import sleep
from raff.burst.burst_test import (BurstTest, setup_teardown_burst,
                                   verify_query_bursted,
                                   verify_query_didnt_burst)
from raff.common.db.session import DbSession

log = logging.getLogger(__name__)
# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [
    setup_teardown_burst, verify_query_bursted, verify_query_didnt_burst
]

LOCALHOST_QUEUE_SLOT_COUNT = 2


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.burst_precommit
@pytest.mark.serial_only
@pytest.mark.no_jdbc
class TestBurstAutoBase(BurstTest):
    event_thread_mutex = threading.Condition()

    @contextmanager
    def event_context(self, cluster, event_name, **kwargs):
        """
        Set event and unset on exit.
        """
        try:
            cluster.set_event(event_name + ', {}'.format(
                ', '.join(key + '=' + kwargs[key] for key in kwargs)))
            yield
        finally:
            cluster.unset_event(event_name)

    def run_long_query(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute(
                "set query_group to burst; "
                "select count(*) from catalog_sales A, catalog_sales B,"
                "catalog_sales C;"
            )

    def kill_inflight_queries(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to metrics;")
            cursor.execute(
                "select pid from stv_inflight A, stv_wlm_query_state B "
                "where A.query = B.query and B.service_class >= 6 and "
                "service_class <= 14")
            rows = cursor.fetchall()
            for row in rows:
                pid = row[0]
                log.info("Killing {}".format(pid))
                cursor.execute("select pg_terminate_backend({})".format(pid))
            return len(rows)

    def kill_all_queries(self, db_session):
        max_trial = 5
        while max_trial > 0:
            # Try to find the processes multiple times because some
            # queries may be queued and not show up in stv_inflight.
            num_killed = self.kill_inflight_queries(db_session)
            if num_killed == 0:
                break
            else:
                sleep(5)
            max_trial -= 1

    def fail_bm_prepare(self, cluster):
        max_trial = 60
        self.event_thread_mutex.acquire()
        with self.event_context(
                cluster, "EtSimulateBurstManagerError", error="Prepare"):
            self.event_thread_mutex.notify()
            self.event_thread_mutex.release()
            q_count = 0
            while max_trial > 0 and q_count < 1:
                sleep(1)
                with self.db.cursor() as cursor:
                    cursor.execute("set query_group to metrics;")
                    cursor.execute(
                        "select count(*) from stv_wlm_query_state B "
                        "where B.service_class >= 6 and service_class <= 14 "
                        "and state = 'QueuedWaiting'")
                    q_count = int(cursor.fetch_scalar())
                max_trial -= 1
        if max_trial == 0:
            pytest.fail("fail_bm_prepare timed out")

    def wait_until_queries_running(self, sc_id, num_running, state):
        """
        Wait until num_running queries are running. The test is failed if this
        doesn't happen even after checking 10 times.
        """
        max_trial = 60
        q_count = 0
        while max_trial > 0 and q_count < num_running:
            sleep(1)
            with self.db.cursor() as cursor:
                cursor.execute("set query_group to metrics;")
                cursor.execute("select count(*) from stv_wlm_query_state "
                               "where service_class = {} "
                               "and state = '{}'".format(sc_id, state))
                q_count = int(cursor.fetch_scalar())
            max_trial -= 1
        if max_trial == 0:
            pytest.fail("wait_until_queries_running timed out")

    def open_slots_for_queued_query(self):
        """
        Checks that a query is queued and when it does kills currently
        executing queries to open a slot for a queued query.
        """
        max_trial = 60
        q_count = 0
        while max_trial > 0 and q_count < 1:
            sleep(1)
            with self.db.cursor() as cursor:
                cursor.execute("set query_group to metrics;")
                cursor.execute(
                    "select count(*) from stv_wlm_query_state B "
                    "where B.service_class >= 6 and service_class <= 14 "
                    "and state = 'QueuedWaiting'")
                q_count = int(cursor.fetch_scalar())
            max_trial -= 1
        self.kill_inflight_queries(self.db)
        if max_trial == 0:
            pytest.fail("open_slots_for_queued_query timed out")

    def run_race_scheduler(self, cluster):
        self.wait_until_queries_running(6, 1, 'QueuedWaiting')
        cluster.unset_event("EtSimulateBurstManagerError")
        self.wait_until_queries_running(6, 1, 'QueuedWaiting')
        self.kill_inflight_queries(self.db)
        self.wait_until_queries_running(6, 0, 'Running')
        cluster.unset_event("EtWlmBurstStatusRace")


CUSTOM_AUTO_GUCS = {"try_burst_first": "false"}


@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_AUTO_GUCS)
@pytest.mark.burst_precommit
@pytest.mark.no_jdbc
class TestBurstAuto(TestBurstAutoBase):
    def test_queued_query_burst(self, cluster, db_session,
                                verify_query_bursted):
        """
        This tests that a query would only burst if queued, otherwise not.
        """
        with db_session.cursor() as cursor:
            cursor.execute("select 1;")
        for i in range(LOCALHOST_QUEUE_SLOT_COUNT):
            thread = threading.Thread(
                target=self.run_long_query,
                args=((DbSession(cluster.get_conn_params())), ))
            thread.start()
        try:
            # Give some time for queries in the threads to begin.
            self.wait_until_queries_running(6, 2, 'Running')
            self.execute_test_file('burst_query', session=db_session)
        finally:
            self.kill_all_queries(self.db)

    def test_queued_query_burst_after_retry(self, cluster, db_session,
                                            verify_query_bursted):
        """
        This tests that a queued query runs on burst when it gets a chance
        to run burst manager can successfully prepare a burst call.
        """
        with db_session.cursor() as cursor:
            cursor.execute("select 1;")
        for i in range(LOCALHOST_QUEUE_SLOT_COUNT):
            thread = threading.Thread(
                target=self.run_long_query,
                args=((DbSession(cluster.get_conn_params())), ))
            thread.start()
        thread = threading.Thread(
            target=self.fail_bm_prepare, args=(cluster, ))
        thread.start()
        self.event_thread_mutex.acquire()
        # Wait until the event is set inside fail_bm_prepare.
        self.event_thread_mutex.wait(5)
        try:
            self.wait_until_queries_running(6, 2, 'Running')
            self.execute_test_file('burst_query', session=db_session)
        finally:
            self.kill_all_queries(self.db)

    def test_queued_query_main_after_retry(self, cluster, db_session,
                                           verify_query_didnt_burst):
        """
        This tests that a queued query runs on main when it gets a chance to
        run on main before burst manager can successfully prepare a burst call.
        """
        with db_session.cursor() as cursor:
            cursor.execute("select 1;")
        for i in range(LOCALHOST_QUEUE_SLOT_COUNT):
            thread = threading.Thread(
                target=self.run_long_query,
                args=((DbSession(cluster.get_conn_params())), ))
            thread.start()
        self.wait_until_queries_running(6, 2, 'Running')
        # Kill some executing queries to make room for a queued query to
        # execute on main.
        thread = threading.Thread(target=self.open_slots_for_queued_query)
        thread.start()
        with self.event_context(
                cluster, "EtSimulateBurstManagerError", error="Prepare"):
            try:
                self.execute_test_file('burst_query', session=db_session)
            finally:
                self.kill_all_queries(self.db)

    def test_query_race_burst_or_run_main_after_queuing(
            self, cluster, db_session):
        """
        Test that there is no race between a query about to burst after prepare
        and at the same time it is scheduled on main. The race is described in
        Burst-1522
        """
        for i in range(LOCALHOST_QUEUE_SLOT_COUNT):
            thread = threading.Thread(
                target=self.run_long_query,
                args=((DbSession(cluster.get_conn_params())), ))
            thread.start()
        self.wait_until_queries_running(6, 2, 'Running')

        thread = threading.Thread(
            target=self.run_race_scheduler, args=(cluster, ))
        thread.start()

        with self.event_context(cluster, "EtWlmBurstStatusRace"):
            with self.event_context(
                    cluster, "EtSimulateBurstManagerError", error="Prepare"):
                with db_session.cursor() as cursor:
                    cursor.execute("set query_group to burst;")
                    cursor.execute(
                        "select count(*) from catalog_sales "
                        " union select count(*) - 1 from catalog_returns")


CUSTOM_AUTO_GUCS = {"try_burst_first": "true"}


@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_AUTO_GUCS)
@pytest.mark.burst_precommit
class TestBurstAutoWithTryBurstFirst(TestBurstAutoBase):
    def test_prepare_failure_retry_on_burst(self, cluster, db_session,
                                            verify_query_bursted):
        """
        Tests that prepare failures causes the queue to be full, and
        a successive query is enqueued to the queue. After the query is
        queued, if prepare succeeds, the query successfully bursts.
        """
        with db_session.cursor() as cursor:
            cursor.execute("select 1;")
        thread = threading.Thread(
            target=self.fail_bm_prepare, args=(cluster, ))
        thread.start()
        self.event_thread_mutex.acquire()
        # Wait until the event is set inside fail_bm_prepare.
        self.event_thread_mutex.wait(5)
        # Since prepare will fail the long running queries in the following
        # thread should be running on the main.
        for i in range(LOCALHOST_QUEUE_SLOT_COUNT):
            thread = threading.Thread(
                target=self.run_long_query,
                args=((DbSession(cluster.get_conn_params())), ))
            thread.start()
        try:
            self.wait_until_queries_running(6, 2, 'Running')
            # After the above threads are keeping the queue full,
            # the following query will start to wait until the prepare
            # succeeds. As soon as the prepare succeeds, the following
            # query will run on burst.
            self.execute_test_file('burst_query', session=db_session)
        finally:
            self.kill_all_queries(self.db)

    def test_prepare_failure_retry_on_main(self, cluster, db_session,
                                           verify_query_didnt_burst):
        # Tests that query runs on main when there is a prepare failure
        # and the queue is empty.
        with self.event_context(
                cluster, "EtSimulateBurstManagerError", error="Prepare"):
            self.execute_test_file('burst_query', session=db_session)

    @contextmanager
    def verify_non_retriable_prepare_failure(self, code):
        """
        Take a note of the time and verify if query bursted by checking
        stl_query.
        """
        with self.db.cursor() as cursor:
            cursor.execute("set query_group to metrics")
            cursor.execute("SELECT EXTRACT(epoch FROM SYSDATE)")
            ts = cursor.fetch_scalar()
        yield
        with self.db.cursor() as cursor:
            cursor.execute("set query_group to metrics;")
            cursor.execute(
                "select query from stl_burst_prepare "
                "where code = {} and "
                "EXTRACT(epoch FROM starttime) >= {}".format(code, ts))
            assert 1 == cursor.rowcount, \
                "Prepare error with non-retriable code not found."
            query = cursor.fetch_scalar()
            cursor.execute(
                "select concurrency_scaling_status_txt from "
                "svl_query_concurrency_scaling_status "
                "where concurrency_scaling_status = 0 and "
                "query = {}".format(query))
            assert 1 == cursor.rowcount, "Query did not fail"
            row = cursor.fetchone()
            txt = row[0]
            assert txt.strip() == ("Concurrency Scaling eligible query - "
                                   "Failed to prepare cluster")

    def test_non_retriable_prepare_failure_run_on_main(
            self, cluster, db_session, verify_query_didnt_burst):
        # Tests that query runs on main when there is a non retriable
        # prepare failure.
        with self.event_context(
                cluster, "EtSimulateBurstManagerError",
                error="NonRetriablePrepare"):
            with self.verify_non_retriable_prepare_failure(3):
                self.execute_test_file('burst_query', session=db_session)

    def simulate_no_suitable_cluster(self, cluster):
        max_trial = 60
        self.event_thread_mutex.acquire()
        with self.event_context(cluster, "EtSimulateBurstManagerError",
                                error="NoSuitableCluster"):
            self.event_thread_mutex.notify()
            self.event_thread_mutex.release()
            q_count = 0
            while max_trial > 0 and q_count < 1:
                sleep(1)
                with self.db.cursor() as cursor:
                    cursor.execute("set query_group to metrics;")
                    cursor.execute(
                        "select count(*) from stv_wlm_query_state B "
                        "where B.service_class >= 6 and service_class <= 14 "
                        "and state = 'QueuedWaiting'")
                    q_count = int(cursor.fetch_scalar())
                max_trial -= 1
        if max_trial == 0:
            pytest.fail("simulate_no_suitable_cluster timed out")

    def test_non_retriable_prepare_failure_run_on_burst(
            self, cluster, db_session, verify_query_bursted):
        """
        Tests that non-retriable prepare failures causes the queue to be full,
        and a successive query is enqueued to the queue. After the query is
        queued, if there is a suitable cluster, the query successfully bursts.
        """
        with db_session.cursor() as cursor:
            cursor.execute("select 1;")
        thread = threading.Thread(
            target=self.simulate_no_suitable_cluster, args=(cluster, ))
        thread.start()
        self.event_thread_mutex.acquire()
        # Wait until the event is set in simulate_no_suitable_cluster.
        self.event_thread_mutex.wait(5)
        # Since prepare will fail the long running queries in the following
        # thread should be running on the main.
        for i in range(LOCALHOST_QUEUE_SLOT_COUNT):
            thread = threading.Thread(
                target=self.run_long_query,
                args=((DbSession(cluster.get_conn_params())), ))
            thread.start()
        try:
            self.wait_until_queries_running(6, 2, 'Running')
            # After the above threads are keeping the queue full,
            # the following query will start to wait until the prepare
            # succeeds. As soon as the prepare succeeds, the following
            # query will run on burst.
            self.execute_test_file('burst_query', session=db_session)
        finally:
            self.kill_all_queries(self.db)
            sleep(1)

    def burst_worker(self, db_session):
        self.execute_test_file('burst_query', session=db_session,
                               query_group='burst')

    def non_burst_worker(self, db_session):
        self.execute_test_file('burst_query', session=db_session,
                               query_group='noburst')

    def get_db_session(self, cluster):
        db_session = DbSession(cluster.get_conn_params())
        with db_session.cursor() as cursor, self.db.cursor() as cursor_db:
            # Get regular user name.
            user = db_session.session_ctx.username
            # Get the user id of the regular user.
            cursor_db.execute(
                "select usesysid from pg_user "
                "where usename = '{}'".format(user)
            )
            userid = cursor_db.fetch_scalar()
            log.info("Regular user id: {}".format(userid))
            cursor_db.execute(
                "GRANT ALL ON ALL TABLES IN SCHEMA public "
                "to {}".format(user)
            )
            cursor.close()
        return db_session

    def test_burst_slow_prepare(self, cluster, verify_query_bursted):
        """
        Tests that burst slow prepare does not cause wlm slowness.
        """
        with self.db.cursor() as cursor:
            cursor.execute("set query_group to metrics")
            cursor.execute("SELECT SYSDATE")
            ts = cursor.fetch_scalar()

        with cluster.event("EtBurstTracing", "level=ElDebug5"), \
                cluster.event("EtSimulateBurstSlownessBeforePrepare",
                              "sleep=100"):
            burst_thread = threading.Thread(
                target=self.burst_worker,
                args=(self.get_db_session(cluster),))
            burst_thread.daemon = True
            burst_thread.start()
            with self.db.cursor() as cursor:
                cursor.execute("set query_group to metrics")
                trial = 30
                prepare_start_count = 0
                while prepare_start_count == 0 and trial > 0:
                    cursor.execute("""
                        SELECT eventtime FROM stl_event_trace
                        where eventtime >= '{}'
                        and message like '%BurstManager::Prepare%'
                                    """.format(ts))
                    prepare_start_count = cursor.rowcount
                    sleep(1)
                    trial -= 1
                if trial <= 0:
                    pytest.fail("Burst prepare never started")
            # We now know that prepare started. Let's run non-burst query
            # and then check that prepare hasn't finished.
            non_burst_thread = threading.Thread(
                target=self.non_burst_worker,
                args=(self.get_db_session(cluster),))
            non_burst_thread.daemon = True
            non_burst_thread.start()

            with self.db.cursor() as cursor:
                cursor.execute("set query_group to metrics")
                trial = 100
                while trial > 0:
                    # while non burst thread is alive prepare should not
                    # finish, which is marked by logging in stl_burst_prepare.
                    cursor.execute("""
                        select * from stl_burst_prepare
                        where starttime >= '{}'""".format(ts))
                    assert 0 == cursor.rowcount
                    if not non_burst_thread.is_alive():
                        # No need to check anymore since non-burst query
                        # finished while the prepare hasn't finished.
                        # In case non_burst_thread finishes too quick
                        # we want to make sure that above assert is exercised
                        # at least once.
                        break
                    sleep(1)
                    trial -= 1
                if trial <= 0:
                    # This means that non_burst_thread hung.
                    pytest.fail("Non burst query did not finish")
        # Prepare should go through soon and burst thread should die.
        # If that doesn't happen in 100 sec let's quit, verify_query_bursted
        # will fail the test.
        burst_thread.join(100)


CUSTOM_MV_BURST_GUCS = {
    'try_burst_first': 'true',
    'enable_mv': 'true',
    'enable_burst_refresh': 'false'
}


@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_MV_BURST_GUCS)
@pytest.mark.burst_precommit
class TestMvBurst(TestBurstAutoBase):
    @contextmanager
    def mv_setup_context(self, db_session):
        """
        Setup and clean IVM setup.
        """
        try:
            with db_session.cursor() as cursor:
                cursor.execute('''CREATE TABLE catalog_sales_copy AS
                SELECT * FROM catalog_sales''')
                cursor.execute('''CREATE MATERIALIZED VIEW catalog_sales_mv AS
                SELECT * FROM catalog_sales_copy''')
            yield
        finally:
            with db_session.cursor() as cursor:
                cursor.execute('DROP MATERIALIZED VIEW catalog_sales_mv')
                cursor.execute('DROP TABLE catalog_sales_copy')

    def _validate_burst_mv_query(self, cursor):
        """
        Check that we get the same result from burst as we got from main.
        """
        # Get result from main.
        cursor.execute('''set query_group to noburst;
                SELECT count(*) FROM catalog_sales_mv''')
        main_result = cursor.fetch_scalar()
        # This query bursts.
        cursor.execute('''set query_group to burst;
                SELECT count(*) FROM catalog_sales_mv''')
        burst_result = cursor.fetch_scalar()
        # Check if the query above bursted.
        cursor.execute('''SELECT concurrency_scaling_status FROM stl_query
            where query = pg_last_query_id()''')
        assert 1 == cursor.fetch_scalar()
        # Check if burst query result match those of main query.
        assert main_result == burst_result

    def _create_mv_backup(self, cluster, do_refresh):
        """
        Creates back up for current state of MV and refreshes rest agent.
        """
        SNAPSHOT_IDENTIFIER_1 = ("{}-{}".format(cluster.cluster_identifier,
                                                str(uuid.uuid4().hex)))
        cluster.backup_cluster(SNAPSHOT_IDENTIFIER_1, wait=True)
        if (do_refresh):
            self.release_localmode_burst(cluster)

    def test_mv_query_bursts(self, db_session, cluster):
        """
        Tests that a query on MV can burst:
        1. When MV is newly created
        2. When MV is incrementally updated
        3. When MV is recomputed
        """
        with self.mv_setup_context(db_session), \
                db_session.cursor() as cursor:
            self._create_mv_backup(cluster, False)
            self._validate_burst_mv_query(cursor)

            # Change base table and refresh MV incrementally.
            cursor.execute('''INSERT INTO catalog_sales_copy SELECT * FROM
                catalog_sales''')
            with cluster.event('EtStopRefreshUntilDmlXidLargest'):
                # Make sure refresh is successful.
                retries_left = 10
                cursor.execute('''set query_group to noburst;
                        SELECT count(*) FROM catalog_sales_copy''')
                expected_rows = cursor.fetch_scalar()
                while (retries_left > 0):
                    # Refresh incrementally.
                    cursor.execute('''REFRESH MATERIALIZED VIEW
                        catalog_sales_mv''')
                    cursor.execute('''set query_group to noburst;
                            SELECT count(*) FROM catalog_sales_mv''')
                    if (expected_rows == cursor.fetch_scalar()):
                        break
                    sleep(1)
                    retries_left = retries_left - 1
                assert retries_left >= 0
            # Take backup again and validate MV query bursts.
            self._create_mv_backup(cluster, True)
            self._validate_burst_mv_query(cursor)

            # Now test recompute.
            # Change base table and refresh MV using recompute.
            cursor.execute('''INSERT INTO catalog_sales_copy SELECT * FROM
                catalog_sales''')
            with self.db.cursor() as bs_cur:
                bs_cur.execute('set search_path = {}'.format(
                    db_session.session_ctx.schema))
                bs_cur.execute("xpx 'refresh_mv catalog_sales_mv recompute'")
            # Take backup again and validate MV query bursts.
            self._create_mv_backup(cluster, True)
            self._validate_burst_mv_query(cursor)

CUSTOM_SERIALIZABILITY_BURST_GUCS = {
    'try_burst_first': 'true',
    'enable_burst_refresh': 'false',
    'enable_query_level_code_generation': 'false'
}


@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_SERIALIZABILITY_BURST_GUCS)
@pytest.mark.burst_precommit
@pytest.mark.localhost_only
@pytest.mark.serial_only
class TestSerilizabilityBurst(TestBurstAutoBase):
    @contextmanager
    def _setup_context(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute('''CREATE TABLE t1 AS
            SELECT * FROM catalog_sales''')
            cursor.execute('''CREATE TABLE t2 AS
            SELECT * FROM catalog_sales''')
        yield

    def _create_backup(self, cluster, do_refresh):
        SNAPSHOT_IDENTIFIER_1 = ("{}-{}".format(cluster.cluster_identifier,
                                                str(uuid.uuid4().hex)))
        cluster.backup_cluster(SNAPSHOT_IDENTIFIER_1, wait=True)
        if (do_refresh):
            self.release_localmode_burst(cluster)

    @pytest.mark.session_ctx(user_type='super')
    def test_serializability_burst(self, db_session, cluster):
        """
        Tests that read queries executed on burst also lead to serializability
        violation in the same manner it does when run on main. Test ensures
        that read queries acquire read lock on tables and release them at
        commit time.
        """
        with self._setup_context(db_session), \
                db_session.cursor() as cursor:
            self._create_backup(cluster, False)
            conn_params = copy.deepcopy(cluster.get_conn_params())

            conn_params['user'] = db_session.session_ctx.username
            conn_params['password'] = db_session.session_ctx.password
            session1 = RedshiftDb(conn_params, settings={'autocommit': False})
            session2 = RedshiftDb(conn_params, settings={'autocommit': False})

            with session1.cursor() as cursor1, session2.cursor() as cursor2:
                search_path = db_session.session_ctx.set_search_path_sql
                cursor1.execute(search_path)
                cursor2.execute(search_path)
                cursor1.execute("END")
                cursor2.execute("END")
                cursor1.execute("BEGIN")
                cursor2.execute("BEGIN")
                # Orchestrating the classic case of serializability violation
                # where each txn reads from the table that other txn is writing
                # to.
                cursor1.execute(
                    '''set query_group to burst; SELECT count(*) from t1''')
                cursor2.execute(
                    '''set query_group to burst; SELECT count(*) from t2''')
                cursor1.execute('''INSERT INTO t2 select * from t2 limit 5''')
                found_error = False
                # Based on the implementation, we could get error at the
                # time of executing either INSERT or END statement.
                try:
                    cursor2.execute(
                        '''INSERT INTO t1 select * from t1 limit 5''')
                    cursor1.execute("COMMIT")
                    cursor2.execute("COMMIT")
                except InternalError as e:
                    found_error = True
                    cursor1.execute("END")
                    cursor2.execute("END")

                assert found_error

                # Ensuring that table locks get released at commit/rollback
                # time.
                cursor1.execute("SELECT pg_backend_pid()")
                cursor2.execute("SELECT pg_backend_pid()")
                pid_list = str(cursor1.fetch_scalar()) + "," + str(
                    cursor2.fetch_scalar())

                cursor.execute("SELECT COUNT(*) from STV_LOCKS where "
                               "lock_owner_pid in (" + pid_list + ")")
                assert cursor.fetch_scalar() == 0
