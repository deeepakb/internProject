# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved

from contextlib import contextmanager
import logging
import threading

import pytest
from time import sleep
from raff.burst.burst_test import (BurstTest, setup_teardown_burst)
from raff.common.burst_helper import get_query_count
from raff.common.db.session import DbSession

log = logging.getLogger(__name__)
# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [
    setup_teardown_burst
]


CUSTOM_AUTO_GUCS_TO_BURST_SHORT_QUERY = {
    'wlm_json_configuration': (
        '[{"auto_wlm": true,"concurrency_scaling":"auto"},'
        '{"short_query_queue":true,"max_execution_time":20000}]'),
    'burst_sum_queue_time_limit_s': '2',
    'try_burst_first': 'false',
    'enable_short_query_bias': 'true',
    'enable_sqa_by_default': 'true',
    'enable_elastic_sqa': 'false',
    'enable_burst_short_queries': 'true',
    # This is a localhost test. This test sets events to simulate a very
    # specific scenario with queuing in auto wlm which triggers short query
    # bursting. Disable enable_burst_auto_wlm for this very specific test.
    'enable_burst_auto_wlm': 'false',
}


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.burst_precommit
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_AUTO_GUCS_TO_BURST_SHORT_QUERY)
@pytest.mark.serial_only
@pytest.mark.no_jdbc
class TestBurstShortQueries(BurstTest):
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

    def wait_until_queries_with_state(self, sc_id, target_num, state):
        """
        Wait until target_num queries are with the given state. The test is
        failed if this doesn't happen even after checking 10 times.
        """
        with self.db.cursor() as cursor:
            count = 0
            max_trial = 10
            while count < target_num:
                sleep(5)
                cursor.execute("set query_group to metrics;")
                cursor.execute("select count(*) from stv_wlm_query_state "
                               "where service_class = {} "
                               "and state = '{}'".format(sc_id, state))
                rows = cursor.fetchall()
                for row in rows:
                    count = int(row[0])
                max_trial -= 1
                if max_trial == 0:
                    pytest.fail("wait_until_queries_with_state timed out")

    def kill_all_queued_and_running_queries(self, db_session):
        with db_session.cursor() as cursor:
            max_trial = 5
            while max_trial > 0:
                # Try to find the processes multiple times because some
                # queries may be queued and not show up in stv_inflight.
                sleep(5)
                cursor.execute("set query_group to metrics;")
                cursor.execute(
                    "select pid from stv_inflight A, stv_wlm_query_state B "
                    "where A.query = B.query and B.service_class >= 100 or "
                    "service_class = 14")
                rows = cursor.fetchall()
                for row in rows:
                    pid = row[0]
                    log.info("Killing {}".format(pid))
                    cursor.execute(
                        "select pg_terminate_backend({})".format(pid))
                if len(rows) == 0:
                    break
                max_trial -= 1

    def verify_bm_prepare_failure_in_auto_queue(self, q_count):
        max_trial = 10
        with self.db.cursor() as cursor:
            waiting_count = 0
            while waiting_count < q_count:
                sleep(5)
                cursor.execute("set query_group to metrics;")
                cursor.execute(
                    "select count(*) from stv_wlm_query_state "
                    "where service_class >= 100 or service_class = 14"
                    "and state = 'QueuedWaiting'")
                waiting_count = int(cursor.fetch_scalar())
                max_trial -= 1
                if max_trial == 0:
                    pytest.fail("fail_bm_prepare_in_auto_queue timed out")

    def print_debugging_info(self, query):
        with self.db.cursor() as cursor:
            cursor.execute(query)
            for row in cursor.fetchall():
                log.info(row)

    @contextmanager
    def verify_queries_bursted(self, cluster, num_burst):
        """
        Verify that num_burst count of queries bursted.
        """
        count_burst_before = get_query_count(cluster, bursted=True)
        yield
        count_burst_after = get_query_count(cluster, bursted=True)
        if count_burst_after - count_burst_before < num_burst:
            self.print_debugging_info(
                "select userid, query, btrim(label), xid, pid, "
                "concurrency_scaling_status, btrim(text) "
                "from stv_inflight")
            self.print_debugging_info(
                "select xid, task, query, service_class, state, "
                "queue_time, exec_time from stv_wlm_query_state")
            pytest.fail("Some queries did not burst. "
                        "Burst count before: {}, after: {}"
                        "".format(count_burst_before, count_burst_after))

    def test_burst_short_queries(self, cluster):
        """
        This tests that a short query bursts when both auto and short query
        queues are occupied. This also test potential assertion error in
        DP-25593 when reporting service classes are incorrect.
        """
        sqq_num_slot = 6
        try:
            # Make sure one query is bursting so that the burst is prepared
            # for later short queries to find a suitable cluster.
            with self.verify_queries_bursted(cluster, 1), \
                    self.event_context(cluster, "EtOccupyAutoWlmOnMain"):
                self.execute_test_file(
                    'burst_query',
                    session=DbSession(cluster.get_conn_params()))
            # Make sure next queries are assumed short.
            with self.event_context(cluster,
                                    "EtSimulateShortQuery", type="1"), \
                    self.event_context(cluster,
                                       "EtSimulateShortQueryTaskAssigned",
                                       sleep="300"), \
                    self.event_context(cluster, "EtOccupyAutoWlmOnMain"):
                # Occupy the sqq with QueryStateTaskAssigned for 300s.
                # Bursting threads also timeout after 300s, so no reason to
                # keep the short queries in assigned state longer than that.
                for i in range(sqq_num_slot):
                    thread = threading.Thread(
                        target=self.run_long_query,
                        args=(DbSession(cluster.get_conn_params()), ))
                    thread.start()
                self.wait_until_queries_with_state(
                    sc_id=14, target_num=sqq_num_slot, state='TaskAssigned')
                # These should be marked short and should burst since both auto
                # and sqq are fully occupied. However, first prepare will fail
                # and they will wait in queue 100, which are checked in a
                # thread in fail_bm_prepare_in_auto_queue.
                # To test that the following assertion holds in DP-25593,
                # num_htq_executing_ <= gconf_short_query_queue_slots +
                #   gconf_sqa_burst_slots
                # we need to run at least 7 more queries because
                # num_htq_executing is already 6 (due to 6 short queries
                # that are assigned). We will run 10 in this test.
                num_queries_to_repro_dp_25593 = 10
                with self.verify_queries_bursted(
                        cluster, num_queries_to_repro_dp_25593):
                    cluster.set_event(
                        "EtSimulateBurstManagerError, error=Prepare")
                    burst_threads = []
                    for i in range(num_queries_to_repro_dp_25593):
                        thread = threading.Thread(
                            target=self.execute_test_file,
                            args=('burst_query',
                                  DbSession(cluster.get_conn_params())))
                        thread.start()
                        burst_threads.append(thread)
                    self.verify_bm_prepare_failure_in_auto_queue(
                        num_queries_to_repro_dp_25593)
                    cluster.unset_event("EtSimulateBurstManagerError")
                    for thread in burst_threads:
                        # In case of successful bursting this thread can
                        # immediately join with other threads. But in case
                        # a failure in bursting, wait a maximum of 30s
                        # to join each thread. So, if all 10 threads are
                        # failing to burst, the test will exit this block
                        # after 300s, and verify_queries_bursted will mark
                        # the test as failed.
                        thread.join(30)
        finally:
            self.kill_all_queued_and_running_queries(self.db)
