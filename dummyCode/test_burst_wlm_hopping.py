# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
import threading
from contextlib import contextmanager
from time import sleep

from raff.burst.burst_test import (
    setup_teardown_burst,
    BurstTest
)
from raff.common.db.session import DbSession

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst"]

log = logging.getLogger(__name__)

CUSTOM_WLM_CONFIGS = '''
[
    {
        "query_group": ["burst_hop_auto_to_auto"],
        "max_execution_time": 100,
        "concurrency_scaling": "auto",
        "query_concurrency": 1
    },
    {
        "query_group": ["burst_hop_auto_to_auto"],
        "concurrency_scaling": "auto",
        "query_concurrency": 1
    },
    {
        "query_group": ["burst_hop_auto_to_never"],
        "max_execution_time": 100,
        "concurrency_scaling": "auto",
        "query_concurrency": 1
    },
    {
        "query_group": ["burst_hop_auto_to_never"],
        "query_concurrency": 5
    },
    {
        "query_group": ["burst_hop_never_to_auto"],
        "max_execution_time": 100,
        "query_concurrency": 5
    },
    {
        "query_group": ["burst_hop_never_to_auto"],
        "concurrency_scaling": "auto",
        "query_concurrency": 1
    },
    {
        "query_group": ["efficient_hop_never_to_never"],
        "max_execution_time": 100,
        "query_concurrency": 5
    },
    {
        "query_group": ["efficient_hop_never_to_never"],
        "query_concurrency": 1
    }
]
'''

CS_EVALUATION_SQL = (
    "select count(*) from stl_event_trace where "
    "event_name = 'EtAssignToTaskPoolDebugInfo' "
    "and message ilike "
    "'Evaluating concurrency scaling status for query % in service class {}%' "
    "and EXTRACT(epoch FROM eventtime) >= {};"
)

CUSTOM_SETUP_GUCS = {
    'wlm_json_configuration': ''.join(CUSTOM_WLM_CONFIGS.split('\n')),
    'burst_sum_queue_time_limit_s': 0}


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_SETUP_GUCS)
@pytest.mark.serial_only
@pytest.mark.no_jdbc
class TestBurstWlmHopping(BurstTest):
    """Test burst WLM hopping from auto to auto."""

    def run_long_query(self, query_group, db_session):
        with db_session.cursor() as cursor:
            cursor.execute(
                "set query_group to {}; "
                "select count(*) from catalog_sales A, catalog_sales B,"
                "catalog_sales C;".format(query_group)
            )

    def kill_long_running_queries(self):
        with self.db.cursor() as cursor:
            while True:
                sleep(1)
                cursor.execute("set query_group to metrics;")
                cursor.execute(
                    "select pid from stv_inflight A, stv_wlm_query_state B "
                    "where A.query = B.query and "
                    "B.service_class > 5 and "
                    "B.service_class < 14")
                rows = cursor.fetchall()
                if len(rows) == 0:
                    break
                for row in rows:
                    pid = row[0]
                    log.info("Killing {}".format(pid))
                    cursor.execute(
                        "select pg_terminate_backend({})".format(pid))

    def verify_query_evicted_then_running(self, event, ts, from_sc, to_sc):
        """
        Wait until the query is running in the sc.
        """
        with self.db.cursor() as cursor:
            rowcount = 0
            while rowcount < 1 and not event.is_set():
                sleep(1)
                cursor.execute("set query_group to metrics")
                query = ("select xid, task, query, service_class "
                         "from stl_wlm_query "
                         "where final_state = 'Evicted' and "
                         "service_class = {} and "
                         "EXTRACT(epoch FROM service_class_start_time) >= {}"
                         "".format(from_sc, ts))
                cursor.execute(query)
                rowcount = cursor.rowcount

            rowcount = 0
            while rowcount < 1 and not event.is_set():
                sleep(1)
                cursor.execute("set query_group to metrics;")
                cursor.execute("select count(*) from stv_wlm_query_state "
                               "where service_class = {} "
                               "and state = 'Running'".format(to_sc))
                rows = cursor.fetchall()
                for row in rows:
                    rowcount = row[0]

    @contextmanager
    def verify_burst_attempts(self):
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
            cursor.execute("set query_group to metrics")
            cursor.execute('select * from stl_burst_prepare '
                           'where EXTRACT(epoch FROM starttime) >= {}'
                           ''.format(ts))
            assert cursor.rowcount > 0

    @pytest.mark.parametrize("hop_group,from_sc,to_sc", [
        ('burst_hop_auto_to_auto', 6, 7),
        ('burst_hop_auto_to_never', 8, 9),
        ('burst_hop_never_to_auto', 10, 11)
    ])
    def test_burst_hopping(self, hop_group, from_sc, to_sc,
                           cluster, db_session):
        """
        Test hopping from one service class to another. Query should be
        evicted from first sc and completed in the next class.
        """
        with db_session.cursor() as cursor:
            cursor.execute("SELECT EXTRACT(epoch FROM SYSDATE)")
            ts = cursor.fetch_scalar()

        with self.verify_burst_attempts():
            thread = threading.Thread(
                target=self.run_long_query,
                args=(hop_group, DbSession(cluster.get_conn_params())))
            try:
                thread.start()

                event = threading.Event()
                v_thread = threading.Thread(
                    target=self.verify_query_evicted_then_running,
                    args=(event, ts, from_sc, to_sc))
                v_thread.start()

                # wait a minute before failing the test
                v_thread.join(60)
                if v_thread.is_alive():
                    event.set()
                    v_thread.join()
                    pytest.fail("Could not verify that the query is "
                                "evicted in sc {} and running in sc {}"
                                "".format(from_sc, to_sc))
            finally:
                self.kill_long_running_queries()

    def test_efficient_hop_no_burst_evaluation(self, cluster, db_session):
        """
        Test efficient hopping from one service class to another. Query should
        efficient hopping from first sc and completed in the next class. But,
        it should be only evaluating ShouldRunOnMain once for the first sc.
        """
        with db_session.cursor() as cursor:
            cursor.execute("SELECT EXTRACT(epoch FROM SYSDATE)")
            ts = cursor.fetch_scalar()

        with cluster.event('EtAssignToTaskPoolDebugInfo', 'level=ElDebug5'):
            thread = threading.Thread(
                target=self.run_long_query,
                args=('efficient_hop_never_to_never',
                      DbSession(cluster.get_conn_params())))
            try:
                thread.start()

                # Verify that query is evicted from sc 12 and then run in 13.
                event = threading.Event()
                v_thread = threading.Thread(
                    target=self.verify_query_evicted_then_running,
                    args=(event, ts, 12, 13))
                v_thread.start()

                # Above thread should complete soon, if not wait a minute
                # before failing the test.
                v_thread.join(60)
                if v_thread.is_alive():
                    event.set()
                    v_thread.join()
                    pytest.fail("Could not verify that the query is "
                                "evicted in sc {} and running in sc {}"
                                "".format(12, 13))

                # Check burst evaluation only in sc 12, not in sc 13.
                with self.db.cursor() as cursor:
                    cursor.execute(CS_EVALUATION_SQL.format(12, ts))
                    cs_evaluation_count = cursor.fetch_scalar()
                    assert cs_evaluation_count == 1
                    cursor.execute(CS_EVALUATION_SQL.format(13, ts))
                    cs_evaluation_count = cursor.fetch_scalar()
                    assert cs_evaluation_count == 0
            finally:
                self.kill_long_running_queries()
