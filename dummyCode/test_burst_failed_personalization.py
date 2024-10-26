# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import datetime
import pytest
import time
import uuid

from plumbum import ProcessExecutionError
from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst
from raff.common.burst_helper import identifier_from_arn
from raff.common.ssh_user import SSHUser
from raff.util.utils import PropagatingThread
from raff.common.base_test import run_priviledged_query
from raff.common.base_test import run_priviledged_query_scalar_int
from raff.common.db.session import DbSession
from raff.common.burst_helper import get_cluster_by_arn

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst"]

log = logging.getLogger(__name__)
CUSTOM_GUCS = {
    'enable_burst_async_acquire': 'false'
}


@pytest.mark.load_tpcds_data
@pytest.mark.cluster_only
@pytest.mark.serial_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS)
@pytest.mark.no_jdbc
class TestBurstFailedPersonalization(BurstTest):

    def wait_for_spinner_in_place(self, cluster):
        # Wait for the /tmp/burst_pause_personalization file to be created
        timer = 0
        timeout_s = 600
        while timer < timeout_s:
            log.info("Waiting for spinner to be setup")
            with cluster.get_leader_ssh_conn() as ssh_conn:
                try:
                    remote_cmd = "cat"
                    args = ["/tmp/burst_pause_personalization"]
                    rc, stdout, stderr = ssh_conn.run_remote_cmd(
                        remote_cmd, args, user=SSHUser.RDSDB)
                    log.info("Spinner created")
                    break
                except ProcessExecutionError as err:
                    log.info("Spinner not yet created")
                    assert "No such file or directory" in str(err)
            time.sleep(10)
            timer += 10
        if timer == timeout_s:
            pytest.fail("Spinner was not setup in {}s".format(timeout_s))

    def run_burst_query_thread(self, cluster):
        log.info("Starting query")
        user_session = DbSession(cluster.get_conn_params())
        self.execute_test_file('burst_query', session=user_session)
        log.info("Ended query")

    def get_debugging_info(self, cluster, mastercur, start_str):
        # Gather some info for troubleshooting in case of error
        query = '''
                SELECT * from stl_burst_service_client
                where eventtime >= '{}'
                order by eventtime
                '''.format(start_str)
        sc_content = run_priviledged_query(cluster, mastercur, query)
        query = '''
                SELECT * from stl_burst_prepare
                where starttime >= '{}'
                order by starttime
                '''.format(start_str)
        bp_content = run_priviledged_query(cluster, mastercur, query)
        message_for_error = '''
                            Service client content = {}
                            Burst prepare content = {}
                            '''.format(sc_content, bp_content)
        return message_for_error

    def test_burst_failed_personalization(self, cluster, db_session):
        '''
        This test does the following:
        - An event is set to fail personalization on the burst cluster.
        - Two queries are run on that burst cluster while a personalization
          spinner is set to make sure they are trying to personalize
          concurrently.
        - The first query will fail to personalize because of the event.
        - The second query is supposed to notice that personalization failed
          and run on a different cluster instead.
        '''
        cluster.run_xpx('burst_release_all')
        cluster.run_xpx("backup backup_{}".format(uuid.uuid4()))
        cluster.run_xpx("burst_acquire")
        burst_cluster_arns = cluster.list_acquired_burst_clusters()
        cluster.run_xpx("burst_ping {}".format(burst_cluster_arns[0]))
        assert len(burst_cluster_arns) > 0, "No burst clusters acquired"
        burst_cluster_arn = burst_cluster_arns[0]
        burst_cluster_name = identifier_from_arn(burst_cluster_arn)
        burst_cluster = get_cluster_by_arn(burst_cluster_arn)
        if burst_cluster.running_db_version() != cluster.running_db_version():
            pytest.fail("Main and Burst clusters are not on same db version."
                        " Main version: {} Burst version: {}".format(
                            burst_cluster.running_db_version(),
                            cluster.running_db_version()))

        # Set an event such that the first query will fail. The event doesn't
        # need to be unset since we will release the burst cluster anyway.
        burst_cluster.set_event('EtSimulateRestAgentPersonalizationError')

        starttime = datetime.datetime.now().replace(microsecond=0)
        start_str = starttime.isoformat(' ')
        # Set an event to delay burst cluster release from cluster map, so that
        # 2nd burstable query can detect failed personalization from 1st burstable
        # query. Not placed in a context manager as we need to unset event after
        # both queries have completed execution.
        cluster.run_xpx("event set EtBurstSimSlowBurstClusterMapRelease, sleep=20")
        # Setup a spinner to make sure q1 is not too fast and finishes before
        # q2 starts.
        with burst_cluster.leader_xen_spin('burst_pause_personalization'):
            self.wait_for_spinner_in_place(burst_cluster)
            # Start a thread running the first query. This query is supposed to
            # fail personalization and release the cluster.
            q1_thread = PropagatingThread(
                target=self.run_burst_query_thread, args=(cluster, ))
            q1_thread.start()
            # Start a thread running the second query. This query is supposed
            # to be blocked in prepare phase while the first query tries to
            # personalize the cluster, see the cluster was released, and try to
            # run on a different burst cluster.
            q2_thread = PropagatingThread(
                target=self.run_burst_query_thread, args=(cluster, ))
            q2_thread.start()

        # Wait for all the queries to finish
        q1_thread.join()
        q2_thread.join()
        cluster.run_xpx("event unset EtBurstSimSlowBurstClusterMapRelease")

        with self.db.cursor() as mastercur:
            message_for_error = self.get_debugging_info(
                cluster, mastercur, start_str)

            # Verify first burst cluster was released
            query = '''
                    SELECT count(*) from stl_burst_service_client
                    where cluster_arn like '%{}%'
                    and eventtime >= '{}'
                    and reason = 'FailedPersonalization'
                    '''.format(burst_cluster_name, start_str)
            count = run_priviledged_query_scalar_int(cluster, mastercur, query)
            assert count == 1, (
                "Cluster {} was not released as expected {}".format(
                    burst_cluster_name, message_for_error))

            # Verify the second query was made aware of the failed
            # personalization and failed prepare once
            error = 'Cluster released while waiting for personalization'
            query = '''
                    SELECT count(*) from stl_burst_prepare
                    where starttime >= '{}'
                    and error like '%{}%'
                    '''.format(start_str, error)
            count = run_priviledged_query_scalar_int(cluster, mastercur, query)
            assert count == 1, (
                "Query was not aware burst cluster {} was released {}".format(
                    burst_cluster_name, message_for_error))
