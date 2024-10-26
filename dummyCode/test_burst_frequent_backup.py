# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import threading
import logging
from contextlib import contextmanager

import pytest
import datetime
from time import sleep
from raff.burst.burst_test import (
    BurstTest,
    get_burst_cluster_arn,
    setup_teardown_burst
)
from raff.common.db.session import DbSession
from raff.util.utils import run_bootstrap_sql

log = logging.getLogger(__name__)
# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst"]

LOCALHOST_QUEUE_SLOT_COUNT = 2
# Wait for a max of 15 minutes for a burst cluster to be available.
MAX_BURST_WAIT = 900


@pytest.mark.load_tpcds_data
@pytest.mark.burst_precommit
@pytest.mark.serial_only
# This tests to make sure backups are taken frequent enough to keep
# bursting successfully. This is not a client dependent test, so no need
# test with jdbc.
@pytest.mark.no_jdbc
class TestBurstFrequentBackupBase(BurstTest):
    @contextmanager
    def verify_burst(self, cluster, burst_count):
        """
        Take a note of the time and verify if query bursted by checking
        stl_query.
        """
        start_time = datetime.datetime.now()
        ts = start_time.isoformat(' ')
        yield
        with self.db.cursor() as cursor:
            cursor.execute("set query_group to metrics")
            # The queries will trigger a new backup and refresh the clusters.
            # Since the main queue is full the query is always expected to
            # eventually run on burst, in some cases after waiting for
            # refresh to finish.
            cursor.execute('select count(*) from stl_query '
                           'where concurrency_scaling_status = 1 '
                           'and aborted = 0 '
                           'and starttime >= \'{}\' '
                           ''.format(ts))
            actual_count = cursor.fetch_scalar()
            if actual_count != burst_count:
                burst_prepare_errors = run_bootstrap_sql(
                    cluster,
                    'set query_group to metrics;'
                    'select pid, query, starttime, error '
                    'from stl_burst_prepare '
                    'where len(btrim(error)) > 0 '
                    'and starttime >= \'{}\' '
                    'order by starttime;'
                    ''.format(ts))
        assert actual_count == burst_count, \
            ("{} queries bursted instead of the {} expected after {}"
             "Prepare errors: {}").format(actual_count, burst_count, ts,
                                          burst_prepare_errors)

    def verify_burst_queued_waiting(self, event):
        with self.db.cursor() as cursor:
            rowcount = 0
            while rowcount == 0 and not event.is_set():
                sleep(1)
                cursor.execute("set query_group to metrics;")
                cursor.execute(
                    "select count(*) from stv_wlm_query_state B "
                    "where B.service_class >= 6 and service_class <= 14 "
                    "and state = 'QueuedWaiting'")
                rowcount = cursor.rowcount

    def run_long_query(self, db_session):
        try:
            with db_session.cursor() as cursor:
                cursor.execute("select pg_backend_pid();")
                pid = cursor.fetch_scalar()
                log.info("Running long query with pid {}".format(pid))
                cursor.execute(
                    "set query_group to burst; "
                    "select count(*) from catalog_sales A, catalog_sales B,"
                    "catalog_sales C;"
                )
        except Exception as e:
            log.info("Expected to be killed: " + str(e))

    def kill_long_running_queries(self):
        with self.db.cursor() as cursor:
            cursor.execute("set query_group to metrics;")
            cursor.execute(
                "select pid from stv_inflight A, stv_wlm_query_state B "
                "where A.query = B.query and B.service_class = 6")
            rows = cursor.fetchall()
            for row in rows:
                pid = row[0]
                log.info("Killing {}".format(pid))
                cursor.execute("select pg_terminate_backend({})".format(pid))

    def verify_queries_running(self, event):
        """
        Wait until num_running queries are running. The test is failed if this
        doesn't happen even after checking 10 times.
        """
        with self.db.cursor() as cursor:
            rowcount = 0
            while rowcount < 2 and not event.is_set():
                sleep(1)
                cursor.execute("set query_group to metrics;")
                cursor.execute("select count(*) from stv_wlm_query_state "
                               "where service_class = 6 "
                               "and state = 'Running'")
                rows = cursor.fetchall()
                for row in rows:
                    rowcount = row[0]

    def verify_state(self, fn):
        event = threading.Event()
        v_thread = threading.Thread(target=fn, args=(event,))
        v_thread.start()

        # wait a minute before failing the test
        v_thread.join(60)
        if v_thread.is_alive():
            pytest.fail("Could not verify: " + fn.__name__)
            event.set()
            v_thread.join()
            raise Exception()


CUSTOM_AUTO_GUCS = {"try_burst_first": "false",
                    "enable_result_cache": "false",
                    # This test only applies to backup based cold start/refresh.
                    # Always disabled commit based cold start/refresh.
                    "enable_burst_s3_commit_based_cold_start": "false",
                    "enable_burst_s3_commit_based_refresh": 'false',
                    "burst_percent_threshold_to_trigger_backup": 20,
                    # This test cannot acquire new clusters to prevent issues
                    # with version switch. In order to prevent this wait for
                    # clusters refresh to finish, not matter how long it takes.
                    "burst_refresh_expiration_seconds": 9999999,
                    # In addition we also force the max number of clusters to
                    # acquire to be only 1 such that we will never try to
                    # acquire additional clusters
                    'max_concurrency_scaling_clusters': '1',
                    'enable_burst_async_acquire': 'false'}


@pytest.mark.cluster_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_AUTO_GUCS)
@pytest.mark.burst_precommit
# This tests to make sure backups are taken frequent enough to keep
# bursting successfully. This is not a client dependent test, so no need
# test with jdbc.
@pytest.mark.no_jdbc
class TestBurstFrequentBackup(TestBurstFrequentBackupBase):
    def insert_some_rows(self, db_session):
        # Let's insert some more rows
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to noburst;")
            insert_into_sql = 'insert into test_backup_trigger(i,a) ' \
                              'values(1,2), (2,3), (3,4);'
            cursor.execute(insert_into_sql)

    def burst_worker(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute("select pg_backend_pid();")
            pid = cursor.fetch_scalar()
            log.info("Bursting query with pid {}".format(pid))
            cursor.execute("set query_group to burst; "
                           "select * from test_backup_trigger;")

    def burst_runner(self, db_session):
        thread = threading.Thread(target=self.burst_worker,
                                  args=(db_session,))
        thread.daemon = True
        thread.start()
        return thread

    def test_burst_after_triggering_backup(self, cluster, db_session):
        # Personalize the burst cluster such that refresh can happen
        cluster.personalize_burst_cluster(get_burst_cluster_arn(cluster))
        # Create the table for test
        create_table_sql = 'create table test_backup_trigger ' \
                           '(i int, a varchar);'
        with db_session.cursor() as cursor:
            cursor.execute(create_table_sql)

        self.insert_some_rows(db_session)

        # Keep the main cluster busy
        for i in range(LOCALHOST_QUEUE_SLOT_COUNT):
            thread = threading.Thread(
                target=self.run_long_query,
                args=((DbSession(cluster.get_conn_params())), ))
            thread.start()

        try:
            # Give some time for queries in the threads to begin.
            self.verify_state(self.verify_queries_running)

            with self.verify_burst(cluster, 1):
                # Following should trigger a backup
                burst_t = self.burst_runner(db_session)
                # Must finish is MAX_BURST_WAIT seconds
                burst_t.join(MAX_BURST_WAIT)

            # Run 4 times successfully
            with self.verify_burst(cluster, 4):
                for i in range(0, 4):
                    t = self.burst_runner(db_session)
                    # Must finish is MAX_BURST_WAIT seconds
                    t.join(MAX_BURST_WAIT)

            # Let's insert some more rows
            self.insert_some_rows(db_session)

            with self.verify_burst(cluster, 2):
                # Following query should be QueuedWaiting, because the
                # percentage of stale backup is 20%.
                # One more stale backup will exceed the threshold
                q_thread = self.burst_runner(db_session)

                event = threading.Event()
                v_thread = threading.Thread(
                    target=self.verify_burst_queued_waiting,
                    args=(event,))
                v_thread.start()

                self.verify_state(self.verify_burst_queued_waiting)

                # Following will trigger the backup.
                # Please note that this is not a typical test where you can
                # make sure if the burst cluster is available before issuing
                # the queries. Following query will issue the backup, and
                # the burst cluster needs to be acquired after that.
                r_thread = self.burst_runner(db_session)

                # Wait for the burst queries to finish in MAX_BURST_WAIT sec.
                # If they don't, we don't want to hang the test. The test will
                # fail because it will fail to verify_burst(2). All queries
                # will be cleaned by the kill_long_running_queries.
                q_thread.join(MAX_BURST_WAIT)
                r_thread.join(MAX_BURST_WAIT)
        except Exception as e:
            log.error(e)
            raise e
        finally:
            self.kill_long_running_queries()
