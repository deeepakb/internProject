# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
from copy import copy
import pytest
from raff.common.dimensions import Dimensions
from raff.backup_restore.bar_test import BARTestSuite
from raff.common.db import DEFAULT_DB_SETTINGS
from raff.common.db.redshift_db import RedshiftDb


@pytest.mark.serial_only
class TestBARAddColumn(BARTestSuite):
    @classmethod
    def modify_test_dimensions(cls):
        # Need only backup_restore format v2 and backup_sb_format v1
        gucs = BARTestSuite.backup_restore_configs_v2()
        return Dimensions(gucs)

    def test_add_column(self, cluster_session, cluster, vector):
        """
        Testing uncommitted column during commit by using 2 sessions where
        we begin transaction on 1st session, add column and then insert data into
        second table using a different session and COMMIT.
        """
        gucs = dict(
            disk_mirror_count=vector.disk_mirror_count,
            s3_backup_prefix_format_version=vector.
            backup_snapshot_prefix_format_version,
            s3_snapshot_prefix_format_version=vector.
            backup_snapshot_prefix_format_version,
            backup_superblock_format=vector.backup_sb_format_restore_full_sb[
                0],
            backup_superblock_segment_size=vector.backup_sb_segment_size,
            restore_full_superblock=vector.backup_sb_format_restore_full_sb[1],
            var_slice_mapping_policy=vector.var_slice_mapping_policy)
        # Start session.
        with cluster_session(
                gucs=gucs, clean_db_before=True, clean_db_after=True):
            settings = copy(DEFAULT_DB_SETTINGS)
            # starts 2 Sessions. One session adds a column to a table while
            # another does a padb commit.
            with RedshiftDb(cluster.get_conn_params(), settings) as cons_con, \
                RedshiftDb(cluster.get_conn_params(), settings) as cons_con2, \
                    cons_con.cursor() as cursor1, cons_con2.cursor() as cursor2:
                cursor1.execute(
                    "create table if not exists fdisk_test_add_column(a float);"
                )
                cursor1.execute(
                    "create table if not exists fdisk_test_add_column2(a float);"
                )
                cursor1.execute(
                    "insert into fdisk_test_add_column values(random());")
                cursor1.execute("BEGIN;")
                cursor1.execute(
                    "alter table fdisk_test_add_column add column b int;")
                cursor2.execute("set query_group to 'metrics';")
                cursor2.execute(
                    "insert into fdisk_test_add_column2 values(1);")
                cursor1.execute("COMMIT;")
