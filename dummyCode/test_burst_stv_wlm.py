# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import threading
from time import sleep
import logging

import pytest
from raff.burst.burst_test import (
        BurstTest,
        setup_teardown_burst,
        verify_query_bursted
)
from raff.common.db.session import DbSession


# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst, verify_query_bursted]

log = logging.getLogger(__name__)


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstSTVWLMQueryStateRunning(BurstTest):

    def set_event_when_running(self, event):
        with self.db.cursor() as cursor:
            rowcount = 0
            while rowcount == 0 and not event.is_set():
                sleep(1)
                cursor.execute("set query_group to metrics;")
                cursor.execute(
                    "select * from stv_wlm_query_state B "
                    "where B.service_class >= 6 and service_class <= 14 "
                    "and state = 'Running'")
                rowcount = cursor.rowcount

    def test_burst_stv_wlm_query_state_running(
            self, cluster, db_session, verify_query_bursted):
        """
        Tests that wlm running state is set in stv_wlm_query_state.
        """
        event = threading.Event()
        thread = threading.Thread(
            target=self.set_event_when_running, args=(event,))
        thread.start()
        try:
            cluster.set_event(
                'EtSimulateBurstManagerError, error=FakeDelay')
            with db_session.cursor() as cursor:
                cursor.execute("set query_group to burst")
                cursor.execute("SELECT * FROM catalog_sales A;")
        finally:
            thread.join(5)
            if thread.is_alive():
                pytest.fail("Query never started running.")
            event.set()
            thread.join()
            cluster.unset_event('EtSimulateBurstManagerError')


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstSTVWLMNumBurstedQueries(BurstTest):
    def run_burst_query(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst")
            cursor.execute("SELECT * FROM catalog_sales A;")

    @pytest.mark.skip(reason="rsqa-14530")
    def test_total_num_bursted_queries(
            self, cluster, verify_query_bursted):
        """
        Tests that num_bursted_queries are set in stv_wlm_service_class_state.
        """
        threads = []
        for i in range(2):
            thread = threading.Thread(
                target=self.run_burst_query,
                args=((DbSession(cluster.get_conn_params())), ))
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
        with self.db.cursor() as cursor:
            cursor.execute("set query_group to metrics;")
            cursor.execute(
                "select num_concurrency_scaling_queries "
                "from stv_wlm_service_class_state B "
                "where B.service_class >= 6 and service_class <= 14")
            row = cursor.fetchone()
            log.info(row)
            assert row[0] == 2
