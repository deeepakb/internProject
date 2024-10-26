# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import glob
import logging
import os
import pytest

from raff.backup_restore.bar_test import BARTestSuite
from raff.common.simulated_helper import restart_simulated_db
from raff.data.data_utils import load_table

log = logging.getLogger(__name__)


@pytest.mark.no_jdbc
class TestBackupClearSbSegFiles(BARTestSuite):
    """
    Test to validate that superblock segment files of previous backup are
    cleared when the size of the superblock being backed up is smaller than
    the superblock for the previous backup (i.e. shrinking superblock)
    """
    XEN_ROOT = os.getenv("XEN_ROOT")
    BACKUP_DIR = os.path.join(XEN_ROOT, 'data', 'backup')
    BASE_TABLE = "lineitem"

    def _list_all_sb_seg_files(self, backup_id, node):
        backup_dir = self.BACKUP_DIR + str(node)
        sb_dir = os.path.join(backup_dir, "superblocks")
        sb_seg_files = str(sb_dir + "/sb_" + backup_id + "*")
        seg_files_list = glob.glob(sb_seg_files)
        return seg_files_list

    def test_backup_clear_sb_seg_files(self, cluster, cluster_session,
                                       db_session):
        gucs = dict(var_slice_mapping_policy='0',
                    superblock_vacuum_enabled='2')
        with cluster_session(gucs=gucs,
                             clean_db_before=True,
                             clean_db_after=True):
            # Load TPCH lineitem table multiple times and take a backup
            log.info("Creating {} table with 5 clones".format(self.BASE_TABLE))
            load_table(
                db=db_session,
                dataset_name='tpch',
                scale='1',
                table_name=self.BASE_TABLE,
                schema_name=db_session.session_ctx.schema,
                num_clones=5)
            backup1 = self.take_backup()

            # Drop all tables created in the previous step, so that the cluster
            # is empty, and the superblock shrinks after a restart (due to
            # superblock vacuum)
            log.info("Dropping {} table and its clones".format(
                self.BASE_TABLE))
            with db_session.cursor() as cursor:
                cursor.execute("DROP TABLE {}".format(self.BASE_TABLE))
                for i in range(1, 6):
                    cursor.execute("DROP TABLE {}_{}".format(
                        self.BASE_TABLE, i))
            restart_simulated_db()

            # Take a new backup with the shrunk superblock, and validate that
            # superblock segment files associated with the previous backup are
            # not present in the backup superblocks dir
            log.info("Creating new backup and validating SB segment files")
            self.take_backup()
            for node in range(0, cluster.node_count):
                seg_files_list = self._list_all_sb_seg_files(backup1, node)
                assert len(seg_files_list) == 0,\
                        "Found SB seg files for previous backup"
