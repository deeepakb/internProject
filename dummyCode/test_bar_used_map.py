# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
import logging
from raff.common.cluster.cluster_session import ClusterSession
from raff.backup_restore.bar_test import BARTestSuite
from raff.common.db.db_helpers import drop_table
from raff.common.simulated_helper import (expect_backup_fail_with_error_code,
                                          is_restore_complete,
                                          is_rerepp_complete, wait_for)
from raff.data_loaders.tpch.tpch_helper import safely_load_tpch_table
from py_lib.common.xen_guard_control import XenGuardControl

log = logging.getLogger(__name__)


@pytest.mark.localhost_only
@pytest.mark.serial_only
class TestBarUsedMap(BARTestSuite):
    def _verify_tpch_count(self, table):
        tbl2rowcount = {
            "customer": 150000,
            "lineitem": 6001215,
            "orders": 1500000
        }
        with self.db.cursor() as cursor:
            cursor.execute("select count(*) from {}".format(table))
            count = cursor.fetch_scalar()
        expected_count = tbl2rowcount[table]
        assert count == expected_count, """Table {} has different count,
                                           Found: {} Expected: {}""".format(
            table, count, expected_count)

    def test_bar_used_map(self, cluster):
        custom_gucs = {
            's3_backup_prefix_format_version': '2',
            's3_snapshot_prefix_format_version': '2',
            'backup_superblock_format': '1',
            'backup_superblock_segment_size': '10000',
            'restore_full_superblock': 'false',
            'xen_guard_enabled': 'true'
        }
        log.info(
            "Running with backup_superblock_format: 1 and superblock_segment_size:10000"
        )
        """
        1. Load data.
        2. Start taking backup, but sleep while uploading blocks so backup is slower.
        3. Drop the tables with data.
        4. Wait for backup to finish.
        5. Restore from that backup, validate dropped tables are present after restore.
        6. Load more data.
        7. Start taking slow backup, and drop tables with data.
        8. Reboot cluster before backup finishes.
        9. Wait for backup to finish.
        10. Restore from that backup, validate dropped tables are present after restore.
        """
        xen_guard_name = (
            "storage:storage_backup:upload_superblock_and_new_blocks")
        bkp_stage2_xen_guard = XenGuardControl(
            host=None,
            username=None,
            guardname=xen_guard_name,
            debug=True,
            remote=False)
        cluster_session = ClusterSession(cluster)
        with cluster_session(
                gucs=custom_gucs, clean_db_before=True, clean_db_after=True):
            self._run_sql_file("tpch_create_schema", session=self.db)
            conn_params = cluster.get_conn_params()
            safely_load_tpch_table(conn_params, "customer", 1)
            with cluster.event('EtBackupPauseAtStage2'), bkp_stage2_xen_guard:
                backup_id = self.take_backup(should_wait=False)
                bkp_stage2_xen_guard.wait_until_process_blocks(
                    timeout_secs=300)
                drop_table(self.db.cursor(), "customer")
            # Validate the backup finishes successfully
            expect_backup_fail_with_error_code(backup_id, 0)
            self.restore_snapshot(backup_id)
            # verify data after restoring snapshot
            self._verify_tpch_count("customer")
            wait_for(is_restore_complete, retries=6, delay_seconds=10)
            wait_for(is_rerepp_complete, retries=6, delay_seconds=10)

            safely_load_tpch_table(conn_params, "lineitem", 1)
            safely_load_tpch_table(conn_params, "orders", 1)
            with cluster.event('EtBackupPauseAtStage2'), bkp_stage2_xen_guard:
                backup_id1 = self.take_backup(should_wait=False)
                bkp_stage2_xen_guard.wait_until_process_blocks(
                    timeout_secs=300)
                drop_table(self.db.cursor(), "lineitem")
                drop_table(self.db.cursor(), "orders")
                cluster.reboot_cluster()
            # Validate the backup finishes successfully
            expect_backup_fail_with_error_code(backup_id1, 0)
            self.restore_snapshot(backup_id1)
            # verify data after restoring snapshot
            self._verify_tpch_count("lineitem")
            self._verify_tpch_count("orders")
            wait_for(is_restore_complete, retries=6, delay_seconds=10)
            wait_for(is_rerepp_complete, retries=6, delay_seconds=10)
