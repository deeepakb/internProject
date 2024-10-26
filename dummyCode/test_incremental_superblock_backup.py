# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import logging
import pytest
import uuid

from raff.backup_restore.bar_test import BARTestSuite
from raff.common.cluster.cluster_session import ClusterSession
from raff.data.data_utils import load_table
from raff.s3commit.helpers import execute_query

log = logging.getLogger(__name__)


@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.session_ctx(user_type='bootstrap')
class TestIncrementalSuperblockBackup(BARTestSuite):
    def _verify_backed_up_blocks(self, db_session):
        """
        Queries the number of blocks that haven't been backed up and verifies
        that there are none.

        Args:
            db_session (DbSession): A database session object.

        Raises:
            AssertionError: If some blocks haven't been backed up.
        """
        bad_blocks = execute_query(
            db_session,
            'select 1 from stv_blocklist '
            'where tbl > 0 and backed_up = 0;',
            fetch_scalar=True)

        assert bad_blocks is None, \
            "{} blocks were not backed up".format(bad_blocks)

    def _backup_and_verify(self, cluster, db_session, table_names):
        """
        Loads the specified table(s), performs a backup, and verifies that all
        blocks were backed up.

        Args:
            cluster (LocalhostCluster): A localhost cluster object.
            db_session (DbSession): A database session object.
            table_names (list of str): The names of the tables to load.
        """
        for name in table_names:
            load_table(
                db=db_session,
                dataset_name='tpch',
                table_name=name,
                scale='1',
                drop_recreate=True)

        # Begin backup process
        backup_id = str(uuid.uuid4())
        cluster.backup_cluster(backup_id)

        self.verify_backup_spec()
        self._verify_backed_up_blocks(db_session)

    def test_incremental_superblock_backup(self, cluster, db_session):
        """
        This test verifies that incremental backup is consistent. It loads some
        data, performs a backup, checks that all blocks were backed up, and
        repeats. Any data from a previous backup should still be present in the
        next backup.
        """
        cluster_session = ClusterSession(cluster)
        gucs = {
            'vacuum_auto_enable': 'false',
            'backup_superblock_segment_size': 32000,
            'restore_full_superblock': 'true'
        }

        with cluster_session(
                gucs=gucs, clean_db_before=True, clean_db_after=True):
            self._backup_and_verify(cluster, db_session, ['customer'])
            self._backup_and_verify(cluster, db_session, ['lineitem'])
            self._backup_and_verify(
                cluster, db_session,
                ['nation', 'orders', 'part', 'partsupp', 'region', 'supplier'])
