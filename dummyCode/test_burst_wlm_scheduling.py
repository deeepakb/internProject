# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import threading
import logging
from contextlib import contextmanager
from raff.common.cluster.cluster_session import ClusterSession

import pytest
from time import sleep
from raff.burst.burst_test import BurstTest
from raff.common.db.session import DbSession

log = logging.getLogger(__name__)

dynconc_enabled = ('''
    [{"query_concurrency":1}]
''')


@pytest.mark.localhost_only
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstWlmScheduling(BurstTest):
    @contextmanager
    def event_context(self, cluster, event_name):
        """
        Set event and unset on exit.
        """
        try:
            cluster.set_event(event_name)
            yield
        finally:
            cluster.unset_event(event_name)

    def run_long_query(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute("create table customer (x int);")
            cursor.execute("insert into customer values (1),(2);")
            for i in range(15):
                cursor.execute("insert into customer select * from customer;")
            cursor.execute(
                "select count(*) from customer A, customer B, customer C, "
                "customer D;"
            )

    def run_race_scheduler(self, db_session, cluster):
        self.wait_until_queries_queued(6, 1)
        self.kill_all_queries(self.db)
        self.wait_until_queries_running(6, 0)
        cluster.unset_event("EtWlmMainBurstRace")

    def kill_all_queries(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to metrics;")
            cursor.execute(
                "select pid from stv_inflight A, stv_wlm_query_state B "
                "where A.query = B.query and B.service_class >= 6 and "
                "service_class < 14")
            rows = cursor.fetchall()
            for row in rows:
                pid = row[0]
                log.info("Killing {}".format(pid))
                cursor.execute("select pg_terminate_backend({})".format(pid))

    def wait_until_queries_running(self, sc_id, num_running):
        """
        Wait until num_running queries are running. The test is failed if this
        doesn't happen even after checking 10 times.
        """
        with self.db.cursor() as cursor:
            count = 0
            maxcount = 10
            while (count != num_running):
                sleep(5)
                cursor.execute("set query_group to metrics;")
                cursor.execute("select count(*) from stv_wlm_query_state "
                               "where service_class = {} "
                               "and state = 'Running'".format(sc_id))
                rows = cursor.fetchall()
                for row in rows:
                    count = row[0]
                maxcount -= 1
                if (maxcount == 0):
                    pytest.fail("wait_until_queries_running timedout")

    def wait_until_queries_queued(self, sc_id, num_running):
        """
        Wait until num_running queries are running. The test is failed if this
        doesn't happen even after checking 10 times.
        """
        with self.db.cursor() as cursor:
            count = 0
            maxcount = 10
            while (count != num_running):
                sleep(5)
                cursor.execute("set query_group to metrics;")
                cursor.execute("select count(*) from stv_wlm_query_state "
                               "where service_class = {} "
                               "and state = 'Queued'".format(sc_id))
                rows = cursor.fetchall()
                for row in rows:
                    count = row[0]
                maxcount -= 1
                if (maxcount == 0):
                    pytest.fail("wait_until_queries_running timedout")

    def test_start_query(self, cluster, db_session):
        """
        """
        dynconc_enabled_conf = dynconc_enabled.replace('\n', '')
        log.info('Using json config: ' + dynconc_enabled_conf)
        gucs = {
            'enable_burst': 'true',
            'wlm_json_configuration': dynconc_enabled_conf,
        }
        cluster_session = ClusterSession(cluster)
        with cluster_session(gucs=gucs):
            thread = threading.Thread(
                target=self.run_long_query,
                args=((DbSession(cluster.get_conn_params())), ))
            thread.start()
            self.wait_until_queries_running(6, 1)
            thread = threading.Thread(
                target=self.run_race_scheduler,
                args=((DbSession(cluster.get_conn_params())), cluster))
            thread.start()
            with db_session.cursor() as cursor:
                cluster.set_event("EtWlmMainBurstRace")
                cursor.execute("select * from stv_inflight")
