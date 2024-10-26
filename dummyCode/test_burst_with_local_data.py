# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import json
import logging
import pytest
import time
import datetime
import uuid

from plumbum import ProcessExecutionError
from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst
from raff.common.burst_helper import get_cluster_by_arn
from raff.common.aws_clients.redshift_client import RedshiftClient
from raff.common.db.redshift_db import RedshiftDb
from raff.common.profile import Profiles
from raff.common.region import Regions
from raff.common.ssh_user import SSHUser
from raff.util.utils import run_bootstrap_sql

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst"]

log = logging.getLogger(__name__)

CUSTOM_GUCS = {'enable_burst_async_acquire': 'false'}

sql_is_burst_cluster_personalized = '''
select trim(cluster_arn) from stl_burst_manager_personalization where starttime >= '{}'
'''


@pytest.mark.load_tpcds_data
@pytest.mark.cluster_only
@pytest.mark.serial_only
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS)
@pytest.mark.no_jdbc
class TestBurstWithLocalData(BurstTest):
    def _wait_for_health_table_on_burst(self, burst_cluster):
        """
        Wait for 10 minutes for health check table to be created.
        Returns True, if health check table got created.
        """
        timer = 0
        log.info("Health table creation begins")
        while timer <= 600:
            # Health check tables are hardcoded with Oid 100004.
            result = run_bootstrap_sql(
                burst_cluster, 'select * from pg_class where oid = 100004')
            if len(result) <= 0:
                log.info("Waiting for health check table creation")
                time.sleep(10)
                timer += 10
            else:
                log.info("Health table created")
                return True
        log.info("Health table creation failed.")
        return False

    def _query_health_table_on_burst(self, burst_cluster):
        # Health check tables are hardcoded with Oid 100004.
        rel_name = run_bootstrap_sql(
            burst_cluster,
            'select relname from pg_class where oid = 100004')[0][0]
        timeout = 600
        while True:
            result = run_bootstrap_sql(
                burst_cluster,
                'select * from  pg_internal.{}'.format(rel_name))
            if len(result) > 0:
                log.info("Successfully queries local burst data.")
                break
            else:
                time.sleep(10)
                timeout -= 10
            assert timeout >= 0, "Querying local burst data did not work"

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

    def get_ongoing_backup_if_any(self, cluster):
        """
        Return the ongoing backup id if there is one, else return None.
        """
        query = """
            select trim(backup_id), in_progress from
            stl_backup_leader order by currenttime
            desc limit 1;
        """
        res = run_bootstrap_sql(cluster, query)
        if not res or not res[0]:
            return None
        backup_id, is_in_progress = res[0]
        return backup_id if int(is_in_progress) == 1 else None

    def test_burst_with_local_data(self, cluster, db_session):
        '''
        Wait for health check table to be created on burst cluster before it
        gets personalized. Once personalized, burst a permanent table, followed
        by ensuring health check queries run on burst cluster successfully.
        '''
        cluster.run_xpx('burst_release_all')
        cluster.run_xpx("backup backup_{}".format(uuid.uuid4()))
        # Wait for the backup to complete.
        while self.get_ongoing_backup_if_any(cluster) is not None:
            time.sleep(1)
        cluster.run_xpx("burst_acquire")
        burst_cluster_arn = cluster.list_acquired_burst_clusters()
        cluster.run_xpx("burst_ping {}".format(burst_cluster_arn[0]))
        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')
        assert len(burst_cluster_arn) > 0, "No burst clusters acquired"
        burst_cluster_arn = burst_cluster_arn[0]
        burst_cluster_name = burst_cluster_arn.split(':cluster:')[-1]
        burst_client = RedshiftClient(
            profile=Profiles.QA_BURST_TEST, region=Regions.QA)
        burst_cluster = burst_client.describe_cluster(burst_cluster_name)
        if burst_cluster.running_db_version() != cluster.running_db_version():
            pytest.fail("Main and Burst clusters are not on same db version."
                        " Main version: {} Burst version: {}".format(
                            burst_cluster.running_db_version(),
                            cluster.running_db_version()))

        table_got_created = self._wait_for_health_table_on_burst(burst_cluster)
        log.info("Health table created = {}".format(table_got_created))
        if not table_got_created:
            pytest.fail("Health check table did not get created within"
                        " 10 minutes.")
        # Issue an xpx command to trigger personalization of acquired Burst
        # cluster. This will also invoke version switching if required.
        cluster.personalize_burst_cluster(burst_cluster_arn)
        timeout = 20
        while True:
            is_burst_cluster_personalized = run_bootstrap_sql(
                cluster, sql_is_burst_cluster_personalized.format(start_str))
            if len(is_burst_cluster_personalized) > 0:
                log.info(
                    "personalized {}".format(is_burst_cluster_personalized))
                break
            else:
                time.sleep(6)
            timeout = timeout - 1
            assert timeout >= 0, "Cluster not personalized"
        log.info("Cluster personalized")
        # This query should retrieve unencrypted data of the Main.
        self.execute_test_file('burst_query', session=db_session)
        # Run a query on the health check table on burst cluster accessing
        # encrypted local data.
        self._query_health_table_on_burst(burst_cluster)
