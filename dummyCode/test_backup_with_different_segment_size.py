# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import os
import pytest
import logging

from raff.common.simulated_helper import (create_localhost_snapshot,
                                          copy_snapshot_post_backup,
                                          restore_from_localhost_snapshot,
                                          is_restore_complete,
                                          is_rerepp_complete,
                                          wait_for, XEN_ROOT)
from raff.backup_restore.bar_test import BARTestSuite
from raff.common.cluster.cluster_session import ClusterSession
from raff.data.data_utils import load_table

log = logging.getLogger()


BACKUP_WITH_SEGMENT_SIZE_10K_GUC = {
    "s3_backup_prefix_format_version": "2",
    "backup_superblock_format": "1",
    "backup_superblock_segment_size": "10000",
    "var_slice_mapping_policy": "0"
}

BACKUP_WITH_SEGMENT_SIZE_32K_GUC = {
    "s3_backup_prefix_format_version": "2",
    "backup_superblock_format": "1",
    "backup_superblock_segment_size": "32000",
    "var_slice_mapping_policy": "0"
}


@pytest.yield_fixture(scope='function')
def backup_with_segment_size_10k(request, cluster):
    session = ClusterSession(cluster)
    gucs = BACKUP_WITH_SEGMENT_SIZE_10K_GUC

    with session(gucs, clean_db_before=True, clean_db_after=True):
        yield


@pytest.yield_fixture(scope='function')
def backup_with_segment_size_32k(request, cluster):
    session = ClusterSession(cluster)
    gucs = BACKUP_WITH_SEGMENT_SIZE_32K_GUC

    with session(gucs, clean_db_before=True, clean_db_after=True):
        yield


@pytest.mark.localhost_only
@pytest.mark.serial_only
class TestLocalhostBackupSegment(BARTestSuite):
    """
    This test suite runs backup restore tests with different segment size
    """

    def _execute_test(self, cluster_session):
        # load data
        load_table(db=self.db, dataset_name='tpch',
                   table_name='customer', scale='1',
                   drop_recreate=True)

        with self.db.cursor() as cursor:
            cursor.execute("select * from customer")
            pre_c = cursor.rowcount

        # Begin backup process
        backup_id = "backup_test_0"
        create_localhost_snapshot(backup_id, wait=True)

        # Copy backup files outside of data dir
        copy_snapshot_post_backup(backup_id)
        assert os.path.exists("{}/padb_mock_s3/{}".format(
            XEN_ROOT, backup_id))
        self.verify_backup_spec()

        # Begin restore process
        restore_from_localhost_snapshot(backup_id)
        # wait for restore to finish
        wait_for(is_restore_complete, retries=6, delay_seconds=10)
        wait_for(is_rerepp_complete, retries=6, delay_seconds=10)

        with self.db.cursor() as cursor:
            # Verify that end to end the backup/restore process persisted
            # data
            cursor.execute("select * from customer")
            res_c = cursor.rowcount
            assert pre_c == res_c

        # load data
        load_table(db=self.db, dataset_name='tpch',
                   table_name='lineitem', scale='1',
                   drop_recreate=True)
        load_table(db=self.db, dataset_name='tpch',
                   table_name='orders', scale='1',
                   drop_recreate=True)
        with self.db.cursor() as cursor:
            cursor.execute("select * from lineitem")
            pre_l = cursor.rowcount
            cursor.execute("select * from orders")
            pre_o = cursor.rowcount

        # Begin backup process
        backup_id = "backup_test_1"
        create_localhost_snapshot(backup_id, wait=True)

        # Copy backup files outside of data dir
        copy_snapshot_post_backup(backup_id)
        assert os.path.exists("{}/padb_mock_s3/{}".format(
            XEN_ROOT, backup_id))
        self.verify_backup_spec()

        # Begin restore process
        restore_from_localhost_snapshot(backup_id)
        # wait for restore to finish
        wait_for(is_restore_complete, retries=6, delay_seconds=10)
        wait_for(is_rerepp_complete, retries=6, delay_seconds=10)

        with self.db.cursor() as cursor:
            # Verify that end to end the backup/restore process persisted
            # data
            cursor.execute("select * from customer")
            res_c = cursor.rowcount
            assert pre_c == res_c
            cursor.execute("select * from lineitem")
            res_l = cursor.rowcount
            assert pre_l == res_l
            cursor.execute("select * from orders")
            res_o = cursor.rowcount
            assert pre_o == res_o

        # Begin restore process
        backup_id = "backup_test_0"
        restore_from_localhost_snapshot(backup_id)
        # wait for restore to finish
        wait_for(is_restore_complete, retries=6, delay_seconds=10)
        wait_for(is_rerepp_complete, retries=6, delay_seconds=10)

        with self.db.cursor() as cursor:
            # Verify that end to end the backup/restore process persisted
            # data
            cursor.execute("select * from customer")
            res_c = cursor.rowcount
            assert pre_c == res_c
            l_rowcount = 0
            try:
                cursor.execute("select * from lineitem")
                l_rowcount = cursor.rowcount
            except Exception as ex:
                log.debug(str(ex))
            assert l_rowcount == 0
            o_rowcount = 0
            try:
                cursor.execute("select * from orders")
                o_rowcount = cursor.rowcount
            except Exception as ex:
                log.debug(str(ex))
            assert o_rowcount == 0

    @pytest.mark.usefixtures("backup_with_segment_size_10k")
    def test_backup_restore_segment_10k(self, cluster_session):
        self._execute_test(cluster_session)

    @pytest.mark.usefixtures("backup_with_segment_size_32k")
    def test_backup_restore_segment_32k(self, cluster_session):
        self._execute_test(cluster_session)
