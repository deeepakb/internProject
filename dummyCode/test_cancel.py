# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import glob
import uuid
import os
import subprocess
import time
import pytest
import logging

from raff.backup_restore.bar_test import BARTestSuite
from raff.common.simulated_helper import expect_backup_fail_with_error_code
from raff.common.cluster.cluster_session import ClusterSession
from raff.common.simulated_helper import is_backup_finished
from raff.util.utils import wait_for
from raff.common.simulated_helper import stop_sim_db
from raff.common.simulated_helper import start_sim_db
from raff.common.simulated_helper import (
    copy_snapshot_post_backup, create_localhost_snapshot,
    is_rerepp_complete, is_restore_complete,
    restore_from_localhost_snapshot, is_backup_cancelled_on_cn)

XEN_ROOT = os.getenv("XEN_ROOT")
log = logging.getLogger(__name__)


@pytest.mark.serial_only
class TestCancelBackup(BARTestSuite):
    def setup(self):
        # add half a million records so that each node ends up with
        # more than 50 blocks that need to be backed up
        subprocess.run([
            "python",
            "{}/test/leader_durability/populate_bar_test_table.py".format(
                XEN_ROOT), "500000"
        ])

    def remove_backup_spin(self, cluster):
        path = '/tmp/spin_backup_read_and_upload_blocks'
        if os.path.exists('/tmp/spinner'):
            os.remove('/tmp/spinner')
        if os.path.exists(path):
            os.remove(path)
        cluster.run_xpx('event unset EtBackupPauseAtStage2')

    def take_backup_with_spin(self, event, backup_id, cluster):
        # Clear out existing spin.
        self.remove_backup_spin(cluster)
        cluster.run_xpx('event set {}'.format(event))
        open('/tmp/spin_backup_read_and_upload_blocks', 'a').close()
        cluster.run_xpx('backup {}'.format(backup_id))
        # Wait for at least one process to spin.
        start_time = time.time()
        MAX_SEC = 60  # max wait: 1 min
        while time.time() - start_time < MAX_SEC:
            if os.path.exists('/tmp/spinner'):
                break
        log.info("Took backup {}, with event {}, spinning at \
                 /tmp/spin_backup_read_and_upload_blocks".format(
            backup_id, event))

    def backup_and_restore_snapshot(self, backup_id):
        create_localhost_snapshot(backup_id, wait=True)
        # Copy backup files outside of data dir
        copy_snapshot_post_backup(backup_id)
        restore_from_localhost_snapshot(backup_id)
        wait_for(is_restore_complete, retries=6, delay_seconds=10)
        wait_for(is_rerepp_complete, retries=6, delay_seconds=10)

    def cancel_backup(self, cluster, backup_id):
        query = "xpx 'cancel_backup';"
        insert_values_command = ["psql", "dev", "-c", query]
        subprocess.Popen(insert_values_command)
        self.remove_backup_spin(cluster)
        # wait for backup to be cancelled on CNs
        wait_for(
            is_backup_cancelled_on_cn, args=[backup_id],
            retries=10, delay_seconds=10)

    # Test case 1
    # Backup controller finishes stage1 but hasn't received any messages
    # for stage2
    def _test_bar_cancel_1(self, cluster):
        BACKUP_ID = uuid.uuid4()
        # Pause backup in stage 2, to help cancel backup while the compute nodes
        # are uploading blocks
        self.take_backup_with_spin("EtBackupPauseAtStage2", BACKUP_ID, cluster)
        # Cancel the backup and remove the spin for the cancel to be processed.
        self.cancel_backup(cluster, BACKUP_ID)
        # check backup fail error code
        expect_backup_fail_with_error_code(BACKUP_ID, 1010)

    # Test case 2
    # Stage1 succeeds, and 2 nodes send stage2done response. But
    # one node is still in the middle of stage2 when backup is
    # cancelled
    def _test_bar_cancel_2(self, cluster):
        BACKUP_ID = uuid.uuid4()
        query = "select node_num,count(*) from stl_backup_compute \
                where backup_id = '{}' and \
                blocks_to_upload = blocks_uploaded and \
                node_num <> 1 \
                group by node_num;".format(BACKUP_ID)
        # Pause the backup only in node 1
        self.take_backup_with_spin("EtBackupPauseAtStage2,node=1", BACKUP_ID,
                                   cluster)
        log.info("waiting for node 0 and 2 to finish backup")
        with self.db.cursor() as cursor:
            for count in range(30):
                cursor.execute(query)
                out = cursor.fetchall()
                if len(out) > 2:
                    break
                time.sleep(1)
        log.info("Backup's on node 0 and 2 are finished")
        self.cancel_backup(cluster, BACKUP_ID)
        # check backup fail error code
        expect_backup_fail_with_error_code(BACKUP_ID, 1010)
        # restart the previous backup so all the blocks are uploaded
        # successfully. For the next test, we want only a few blocks
        # to be uploaded.
        cluster.run_xpx('backup {}'.format(BACKUP_ID))
        wait_for(
            is_backup_finished, args=[BACKUP_ID], retries=10, delay_seconds=10)

    # Test case 3: cancel backup should not fail if there is no active backup
    def _test_bar_cancel_3(self, cluster):
        cluster.run_xpx("cancel_backup")

    # Test case 4:
    # 1. Reload more data
    # 2. Cancel backup during stage2
    # 3. Take a backup
    # 4. Restore from that backup
    def _test_bar_cancel_4(self, cluster):
        self.setup()
        BACKUP_ID = uuid.uuid4()
        # Pause backup in stage 2, to help cancel backup while the compute nodes
        # are uploading blocks
        self.take_backup_with_spin("EtBackupPauseAtStage2", BACKUP_ID, cluster)
        # Cancel the backup and remove the spin for the cancel to be processed.
        self.cancel_backup(cluster, BACKUP_ID)
        # check backup fail error code
        expect_backup_fail_with_error_code(BACKUP_ID, 1010)
        with self.db.cursor() as cursor:
            cursor.execute("select count(*) from bar_tests;")
            pre_row_count = cursor.fetch_scalar()
        BACKUP = uuid.uuid4()
        self.backup_and_restore_snapshot(BACKUP)
        cluster.reboot_cluster()
        with self.db.cursor() as cursor:
            cursor.execute("select count(*) from bar_tests;")
            post_row_count = cursor.fetch_scalar()
        assert pre_row_count == post_row_count, "Error, rows are different"

    # Test for Segmented superblock backup
    # 1. Reload more data
    # 2. Slow down the backup, crash padb
    # 3. Forcibly remove local spec
    # 4. Start padb, this should restart backup
    # 5. Verify backup got cancelled because of missing Spec.
    def _test_bar_cancel_5(self, cluster):
        backup_superblock_format = cluster.get_guc_value(
            "backup_superblock_format")
        if backup_superblock_format == 0:
            log.info(
                "This test is only for segmented superblock backup. Returning."
            )
        else:
            self.setup()
            BACKUP_ID = uuid.uuid4()
            # Pause backup in stage 2, to help cancel backup while the compute nodes
            # are uploading blocks
            self.take_backup_with_spin("EtBackupPauseAtStage2", BACKUP_ID,
                                       cluster)
            stop_sim_db()
            path = '{}/data/backup0/local_*'.format(XEN_ROOT)
            remove_files = glob.glob(path)
            for file in remove_files:
                os.remove(file)
            start_sim_db()
            # check backup fail error code
            expect_backup_fail_with_error_code(BACKUP_ID, 1067)

    def test_cancel_backups(self, cluster):
        with ClusterSession(cluster)(
                clean_db_before=True, clean_db_after=True):
            try:
                self.setup()
                self._test_bar_cancel_1(cluster)
                self._test_bar_cancel_2(cluster)
                self._test_bar_cancel_3(cluster)
                self._test_bar_cancel_4(cluster)
                self._test_bar_cancel_5(cluster)
            except Exception as e:
                pytest.fail(e)
            finally:
                self.remove_backup_spin(cluster)
