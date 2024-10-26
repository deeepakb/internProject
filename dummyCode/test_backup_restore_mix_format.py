# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest
import logging

from raff.common.simulated_helper import (create_localhost_snapshot,
                                          copy_snapshot_post_backup,
                                          restore_from_localhost_snapshot,
                                          is_restore_complete,
                                          is_rerepp_complete,
                                          wait_for)
from raff.common.gucs import (SILENT_MODE, GUID_BASED_LOCAL_DISK_MANAGEMENT_GUCS)
from raff.backup_restore.bar_test import BARTestSuite
from raff.common.cluster.cluster_session import ClusterSession

log = logging.getLogger()


START_GUC = {
    "s3_backup_prefix_format_version": "1",
    "s3_snapshot_prefix_format_version": "2"
}


@pytest.yield_fixture(scope='function')
def backup_restore_mix_format_start_guc(request, cluster):
    session = ClusterSession(cluster)
    gucs = START_GUC
    # Disabled guid based local disk management on this test.
    # Discussed with test owner, and this test is for legacy backup
    # format, which is only used on legacy instances. TODO(DP-66385).
    gucs.update(GUID_BASED_LOCAL_DISK_MANAGEMENT_GUCS[SILENT_MODE])

    with session(gucs, clean_db_before=True, clean_db_after=True):
        yield


@pytest.mark.no_autovac
@pytest.mark.localhost_only
@pytest.mark.serial_only
class TestLocalhostBackupRestoreMixFormat(BARTestSuite):
    """
    This test case verifies two backup formats and various cases.
    steps in test case:
    ------------------
    save original versions.
    change version to backup version to v1 and restore version to v2
    take back up b1
    change version to backup version to v2 and restore version to v1
    restore from  b1
    add table/rows
    take back up b2
    change version to backup version to v2 and restore version to v2
    restore from  b2
    add table/rows
    take back up b3
    change version to backup version to v1 and restore version to v2
    restore from  b3
    add table/rows
    take back up b4
    change version to backup version to v1 and restore version to v1
    restore from  b4
    add table/rows
    restore original versions.
    """

    def _load_data(self):
        with self.db.cursor() as cursor:
            cursor.execute("create table bar_test1 (a bigint, b bigint);")
            cursor.execute("insert into bar_test1 values "
                           "(1, 1),(1, 2),(1, 3);")

    def _populate_bar_test_table(self, num_records):
        with self.db.cursor() as cursor:
            cursor.execute("drop table if exists bar_tests;")
            cursor.execute("create table bar_tests(a float, b float, "
                           "c float, d float);")
            cursor.execute("insert into bar_tests values (random(), random(), "
                           "random(), random());")
            table_size = 1

            while (table_size < num_records):
                cursor.execute("insert into bar_tests select random(), "
                               "random(), random(), random() "
                               "from bar_tests;")
                table_size *= 2

    def _execute_test(self, cluster_session, cluster):
        # load data
        self._load_data()

        # Begin backup process
        backup_id = "backup_test_1"
        create_localhost_snapshot(backup_id, wait=True)
        # Copy backup files outside of data dir
        copy_snapshot_post_backup(backup_id)

        # Update backup/snapshot format
        cluster.set_padb_conf_value_unsafe(
            's3_backup_prefix_format_version', 2)
        cluster.set_padb_conf_value_unsafe(
            's3_snapshot_prefix_format_version', 1)

        # Begin restore process
        restore_from_localhost_snapshot(backup_id)
        # wait for restore to finish
        wait_for(is_restore_complete, retries=6, delay_seconds=10)
        wait_for(is_rerepp_complete, retries=6, delay_seconds=10)

        # load more data
        self._populate_bar_test_table(16)

        # Begin backup process
        backup_id = "backup_test_2"
        create_localhost_snapshot(backup_id, wait=True)
        # Copy backup files outside of data dir
        copy_snapshot_post_backup(backup_id)

        # Update backup/snapshot format
        cluster.set_padb_conf_value_unsafe(
            's3_backup_prefix_format_version', 2)
        cluster.set_padb_conf_value_unsafe(
            's3_snapshot_prefix_format_version', 2)

        # Begin restore process
        restore_from_localhost_snapshot(backup_id)
        # wait for restore to finish
        wait_for(is_restore_complete, retries=6, delay_seconds=10)
        wait_for(is_rerepp_complete, retries=6, delay_seconds=10)

        # load more data
        self._populate_bar_test_table(32)

        # Begin backup process
        backup_id = "backup_test_3"
        create_localhost_snapshot(backup_id, wait=True)
        # Copy backup files outside of data dir
        copy_snapshot_post_backup(backup_id)

        # Update backup/snapshot format
        cluster.set_padb_conf_value_unsafe(
            's3_backup_prefix_format_version', 1)
        cluster.set_padb_conf_value_unsafe(
            's3_snapshot_prefix_format_version', 2)

        # Begin restore process
        restore_from_localhost_snapshot(backup_id)
        # wait for restore to finish
        wait_for(is_restore_complete, retries=6, delay_seconds=10)
        wait_for(is_rerepp_complete, retries=6, delay_seconds=10)

        # load more data
        self._populate_bar_test_table(64)

        # Begin backup process
        backup_id = "backup_test_4"
        create_localhost_snapshot(backup_id, wait=True)
        # Copy backup files outside of data dir
        copy_snapshot_post_backup(backup_id)

        # Update backup/snapshot format
        cluster.set_padb_conf_value_unsafe(
            's3_backup_prefix_format_version', 1)
        cluster.set_padb_conf_value_unsafe(
            's3_snapshot_prefix_format_version', 1)

        # Begin restore process
        restore_from_localhost_snapshot(backup_id)
        # wait for restore to finish
        wait_for(is_restore_complete, retries=6, delay_seconds=10)
        wait_for(is_rerepp_complete, retries=6, delay_seconds=10)

    @pytest.mark.usefixtures("backup_restore_mix_format_start_guc")
    def test_backup_restore_mix_format(self, cluster_session, cluster):
        self._execute_test(cluster_session, cluster)
