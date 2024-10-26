# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
import logging
import pytest
import getpass
import uuid

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode, \
                                                         get_burst_conn_params
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.common.db.redshift_db import RedshiftDb
from contextlib import contextmanager

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

BURST_CONNECTION_TIMEOUT_MS = 1000 * 60
SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.load_tpcds("call_center")
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_local_gucs(
    gucs={
        'selective_dispatch_level': '0',
        'burst_service_connection_timeout_ms': BURST_CONNECTION_TIMEOUT_MS
    })
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBurstConnectionTimeout(BurstWriteTest):
    @contextmanager
    def burst_event(self, burst_cursor):
        try:
            burst_cursor.execute(
                "xpx 'event set EtSimulateRestAgentSlowUpdateCredentials, sleep={}'".
                format(BURST_CONNECTION_TIMEOUT_MS * 2))
            yield
        finally:
            burst_cursor.execute(
                "xpx 'event unset EtSimulateRestAgentSlowUpdateCredentials'")

    def test_burst_connection_timeout(self, cluster, db_session):
        """
        This test sets an event on burst to simulate an hang process,
        and verifies that the connection is aborted after the specified
        timeout.
        """
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())

        with self.db.cursor() as cur, \
            burst_session.cursor() as burst_cursor, \
            db_session.cursor() as q_cur:
            # Run a query on burst to acquire a burst cluster
            q_cur.execute("set query_group to burst")
            q_cur.execute("select * from call_center")
            self.check_last_query_bursted(cluster, q_cur)

            # Verify we have a burst cluster acquired
            cur.execute("""
                        select cluster_arn, personalized from
                        stv_burst_manager_cluster_info
                        """)
            results = cur.fetchall()
            log.info("Acquired clusters: " + str(results))
            assert len(results) == 1, "Only one cluster expected in SS"
            cluster_arn = results[0][0].strip()
            with self.burst_event(burst_cursor):
                cluster.run_xpx("burstop update_credentials")
            cur.execute("""
                        select * from
                        stl_burst_connection
                        where action = 'RecvMessage'
                        and method = 'UpdateCredentials'
                        and error = 'Operation canceled'
                        """)
            results = cur.fetchall()
            log.info("Connection logs: " + str(results))
            if len(results) == 0:
                cur.execute("""
                            select * from
                            stl_burst_connection
                            """)
                assert False, "UpdateCredentials didn't timeout {}".format(
                    cur.fetchall())
