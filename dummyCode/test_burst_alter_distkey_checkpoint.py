# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import logging
import uuid
import datetime
import time
from raff.data.data_utils import load_table
from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import (get_burst_cluster_arn, setup_teardown_burst,
                                   verify_query_bursted,
                                   verify_query_didnt_burst)
from raff.common.cluster.cluster_helper import RedshiftClusterHelper
from raff.system_tests.system_test_base import SystemTestBase
from raff.util.utils import run_bootstrap_sql
from raff.storage.storage_test import disable_vacuum_autoworkers

__all__ = [
    "setup_teardown_burst", "verify_query_bursted", "verify_query_didnt_burst"
]

log = logging.getLogger('test_burst_alter_distkey_checkpoint')

ALTER_DISTAUTO = "alter table {} alter diststyle auto"
ALTER_DISTEVEN = "alter table {} alter diststyle even"

BURST_CANARY_QUERY = '''
select count(*) from customer_atc_giant_table;
'''

ATC_TABLE_NAME = 'customer_atc_giant_table'

CUSTOM_ALTER_GUCS = {
    'enable_alter_sortkey_auto': "true",
    'enable_alter_sortkey_none': "true",
    'small_table_row_threshold': 20,
    'small_table_row_lower_bound': 1,
    'small_table_block_threshold': 2,
    'burst_percent_threshold_to_trigger_backup': 100,
    'burst_cumulative_time_since_stale_backup_threshold_s': 86400,
    'burst_commit_refresh_check_frequency_seconds': -1,
    # enable_atd_checkpoint_gucs
    "enable_alter_diststyle_multi_table_checkpoint": "true",
    "alter_diststyle_checkpoint_cn_threshold": "10",
    "vacuum_auto_worker_enable": "false",
    "enable_auto_alter_distall_table": "true",
    "atw_checkpoint_table_multiplier": "50",
    # some extra gucs
    "enable_advisor_immutable_cluster_id": "false",
    "enable_advisor_client_proc": "true",
    "recommendations_file_fetch_interval": 1,
    "auto_alter_table_schedule_interval_sec": 10,
    "auto_alter_worker_interval_per_task_sec": 10,
    "auto_alter_table_giant_table_threshold": 10000,
    "auto_analyze": "false"
}


@pytest.mark.usefixtures("disable_vacuum_autoworkers")
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(
    gucs=dict(**CUSTOM_ALTER_GUCS), initdb_before=True)
@pytest.mark.serial_only
@pytest.mark.cluster_only
class TestBurstAlterDistkeyCheckpoint(BurstTest):
    def take_snapshot(self, cluster):
        """
        Take a snapshot.

        Args:
            cluster: cluster object
        """
        snapshot_identifier = ("{}-{}".format(cluster.cluster_identifier,
                                              str(uuid.uuid4().hex)))
        log.info("take snapshot {}".format(snapshot_identifier))
        cluster.backup_cluster(snapshot_identifier)
        return snapshot_identifier

    def setup_data(self, cluster, db_session):
        """
        Create the load the data to be used in the test.

        Args:
            db_session: db_session object
        """
        drop_table = "DROP TABLE IF EXISTS customer_atc_giant_table;"
        run_bootstrap_sql(cluster, drop_table)
        load_table(
            db=db_session,
            dataset_name='tpch',
            table_name='customer',
            scale='1',
            table_name_suffix='_atc_giant_table',
            grant_to_public=True)
        with db_session.cursor() as cursor:
            cursor.execute(ALTER_DISTEVEN.format(ATC_TABLE_NAME))
            cursor.execute(ALTER_DISTAUTO.format(ATC_TABLE_NAME))

    def _verify_atw_status(self, cluster, table, status, retries=90):
        """
        Waiting function for ATW to get the expected status.

        Args:
            cluster: cluster object
            table: target table
            status: expected status
            retries: retry count till timeout
        """
        stl_auto_worker_event_template = (
            "select tbl, recommend, status from "
            "stl_auto_alter_worker_event where tbl="
            " '{}'::regclass::oid and status like '%{}%';")
        cmd = stl_auto_worker_event_template.format(table, status)
        res = []
        # Timeout retries*10 seconds
        for _ in range(retries):
            res = run_bootstrap_sql(cluster, cmd)
            if len(res) >= 1:
                break
            else:
                time.sleep(10)
        return res

    def _run_alters(self, cluster, cursor):
        """
        Set up ATW to alter the ATC giant table, verify checkpoint status and
        wait for ATW to complete.

        Args:
            cluster: cluster object
            cursor: db cursor
        """
        sys_util = SystemTestBase()
        sys_util.create_atw_recommendations_file(cluster)
        log.info("ATW recommendation is set up...")
        checkpoint_record = self._verify_atw_status(cluster, ATC_TABLE_NAME,
                                                    'Checkpoint')
        log.info("checkpoint_record = {}".format(checkpoint_record))
        assert len(checkpoint_record) != 0
        complete_record = self._verify_atw_status(cluster, ATC_TABLE_NAME,
                                                  'Complete')
        log.info("complete_record = {}".format(complete_record))
        assert len(complete_record) != 0

    def test_burst_alter_distkey_checkpoint_with_burst(
            self, db_session, cluster, verify_query_bursted):
        """
        Test runs ATW Alter Distkey Checkpoint on a table and confirms it can
        still have queries run against it burst successfully.
        """
        # Personalize the burst cluster such that refresh can happen
        cluster.personalize_burst_cluster(get_burst_cluster_arn(cluster))

        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')

        with db_session.cursor() as cursor:
            self.setup_data(cluster, db_session)

            # RUN ATW
            log.info("run ATW")
            self._run_alters(cluster, cursor)

            snapshot_identifier = self.take_snapshot(cluster)
            self.wait_for_refresh_to_start(cluster, start_str,
                                           get_burst_cluster_arn(cluster))

            # Run query and validate the query was bursted
            with self.burst_db_cursor(db_session) as burst_db_cursor:
                burst_db_cursor.execute(BURST_CANARY_QUERY)

            cursor.execute("drop table if exists {}".format(ATC_TABLE_NAME))
            RedshiftClusterHelper.delete_snapshot(snapshot_identifier)

    def test_burst_alter_distkey_checkpoint_without_burst(
            self, db_session, cluster, verify_query_didnt_burst):
        """
        Test runs ATW Alter Distkey Checkpoint on a table and confirms it does
        not burst if no new snapshot is taken.
        """
        # Personalize the burst cluster such that refresh can happen
        cluster.personalize_burst_cluster(get_burst_cluster_arn(cluster))

        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')

        with db_session.cursor() as cursor:
            self.setup_data(cluster, db_session)

            snapshot_identifier = self.take_snapshot(cluster)
            self.wait_for_refresh_to_start(cluster, start_str,
                                           get_burst_cluster_arn(cluster))

            # RUN ATW
            log.info("run ATW")
            self._run_alters(cluster, cursor)

            # Run query and validate the query was not bursted
            with self.burst_db_cursor(db_session) as burst_db_cursor:
                burst_db_cursor.execute(BURST_CANARY_QUERY)

            cursor.execute("drop table if exists {}".format(ATC_TABLE_NAME))
            RedshiftClusterHelper.delete_snapshot(snapshot_identifier)

    def test_burst_alter_distkey_checkpoint_with_burst_between_iteration(
            self, db_session, cluster, verify_query_bursted):
        """
        Test runs ATW Alter Distkey Checkpoint on a table with multiple
        iterations and confirms it can still have queries run against it burst
        successfully between iterations.
        """
        # Personalize the burst cluster such that refresh can happen
        cluster.personalize_burst_cluster(get_burst_cluster_arn(cluster))

        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')

        with db_session.cursor() as cursor:
            self.setup_data(cluster, db_session)

            snapshot_identifier = self.take_snapshot(cluster)
            self.wait_for_refresh_to_start(cluster, start_str,
                                           get_burst_cluster_arn(cluster))

            cluster.set_event('EtAlterSimulateCheckpoint')
            cursor.execute('alter table {} alter distkey {}'.format(
                ATC_TABLE_NAME, 'c_name'))

            # Run query and validate the query was bursted
            with self.burst_db_cursor(db_session) as burst_db_cursor:
                burst_db_cursor.execute(BURST_CANARY_QUERY)

            cursor.execute("drop table if exists {}".format(ATC_TABLE_NAME))
            RedshiftClusterHelper.delete_snapshot(snapshot_identifier)
