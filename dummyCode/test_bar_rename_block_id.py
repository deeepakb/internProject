# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Test for DP-6917: Rename Block IDs when restoring from shared snapshots
#
# ## Create Cluster (CLUSTER0)
# 1. Create a non-encrypted V2 cluster
# 2. Create an encrypted V2 cluster
#
# ## Restore Cluster
# For each created cluster, do the following tests
# 1. Load some data and take a backup BACKUP0 on CLUSTER0
# 2. Restore from BACKUP0 to another V2 cluster CLUSTER1 on the same account,
#    then take backups BACKUP1 during re-replication and after re-replication is
#    done
# 3. Restore from BACKUP1 to another V2 cluster CLUSTER2 on the same account,
#    then take backups BACKUP2 during re-replication and after re-replication is
#    done
# 4. Restore from BACKUP1 to another V2 cluster CLUSTER3 on a different account,
#    then take backups BACKUP3 during re-replication and after re-replication
#    is done
# 5. Restore from BACKUP0 to another V2 cluster CLUSTER4 on a different account,
#    then take backups BACKUP4 during re-replication and after re-replication
#    is done
# 6. Restore from BACKUP4 to another V2 cluster CLUSTER5 on the same account,
#    then take backups BACKUP5 during re-replication and after re-replication
#    is done
# 7. Restore from BACKUP4 to another V2 cluster CLUSTER6 on a different account,
#    then take backups BACKUP6 during re-replication and after re-replication
#    is done
#
# ## Cluster Name Format: CLUSTER{number}_A{account}_V{version}
#    number: cluster number
#   account: account number
#   version: backup version
#
# ## Restore Flow Chart
#                                   CLUSTER2_A1_V2
#                                 /
#                  CLUSTER1_A1_V2
#                 /               \
#               /                   CLUSTER3_A2_V2
#          CLUSTER0_A1_V2
# (V2, encrypted/non_encrypted)
#               \                   CLUSTER5_A1_V2
#                 \               /
#                  CLUSTER4_A2_V2
#                                 \
#                                   CLUSTER6_A2_V2
#
# ## Validation
# ### For the backup that is taken after re-replication is done
# #### GUC ON
# 1. Compare stv_blocklist table before and after taking a backup
#   1.1 Superblock flags should be the same
#   1.2 Most unrelated columns should be the same
#   1.3 backuped_up column should be 1 if it is restored from a non-shared V2
#       snapshot
#   1.4 backuped_up column should always be 1 after taking a backup
#   1.5 id should not change before and after taking a backup
#   1.6 The BLOCK_PERM_IN_S3 flags should be the same if it is restored
#       from a non-shared V2 snapshot
# 2. Compare stv_blockList table before and after restoring from a snapshot
#   2.1 Superblock flags should be 1 after restoring from a
#       shared V2 snapshot
#   2.2 Most unrelated columns should be the same
#   2.3 backuped_up column should be 0 after restoring from a
#       shared V2 snapshot, should be 1 after restoring from a non-shared V2
#       snapshot
#   2.4 All block ids should be changed after restoring from a
#       shared V2 snapshot (renamed)
#   2.5 All block ids should be the same after restoring from a non-shared V2
#       snapshot (not renamed)
#   2.6 flags should have BLOCK_FROM_SNAPSHOT bit if cluster is the original
#       cluster
#   2.7 flags should lose BLOCK_PERM_IN_S3 bit after restoring from
#       a shared V2 snapshot (renamed)
#
# #### GUC OFF
# 1. Compare stv_blocklist table before and after taking a backup
#    The same as GUC ON
# 2. Compare stv_blockList table before and after restoring from a snapshot
#   2.1 Superblock flags should always be 0
#   2.2 Most unrelated columns should be the same
#   2.3 backuped_up column should be 0 after restoring from a
#       shared V2 snapshot, should be 1 after restoring from a non-shared V2
#       snapshot
#   2.4 All block ids should be the same
#   2.6 flags should have BLOCK_FROM_SNAPSHOT bit if cluster is the original
#       cluster
#   2.7 flags should lose BLOCK_PERM_IN_S3 bit after restoring from
#       a shared V2 snapshot
#
# ### For the backup that is taken during re-replication
#  Query test table to validate if the result is the same as what we had when we
#  created the table
#

import pytest
import logging
import raff.tiered_storage.utils as utils
import os
from raff.backup_restore.bar_test import BARTestSuite
from raff.common.simulated_helper import (
    create_localhost_snapshot, copy_snapshot_post_backup,
    restore_from_localhost_snapshot, is_restore_complete, is_rerepp_complete,
    wait_for, XEN_ROOT)
from raff.common.db.db_helpers import (create_table, drop_table,
                                       grant_permissions_to_user)

log = logging.getLogger(__name__)

SUPERBLOCK_FLAG_SQL = "SELECT DISTINCT flags FROM stv_superblock where \
node != 1000"

TEST_TABLE_DATA = "SELECT * FROM {} ORDER BY 1, 2"

BLOCKLIST_QUERY = "SELECT slice, col, tbl, blocknum, num_values, \
extended_limits, minvalue, maxvalue, sb_pos, on_disk, modified, \
unsorted, tombstone, temporary, newblock, \
num_readers, backed_up, id, flags FROM stv_blocklist WHERE \
tbl > 0 ORDER BY slice / 2, sb_pos"


# Marking no_jdbc as JDBC fails for this test. RCA in this SIM:
# https://sim.amazon.com/issues/RedshiftDP-27425
@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.no_jdbc
class TestBarRenameBlockID(BARTestSuite):
    def _load_data(self):
        with self.db.cursor() as cursor:
            drop_table(cursor, self.test_table)
            create_table(cursor, self.test_table, {
                "a": "int",
                "b": "varchar(16)"
            })
            # cursor.execute("GRANT ALL ON {} TO PUBLIC".format(self.test_table))
            grant_permissions_to_user(cursor, "ALL", self.db.dbname, "PUBLIC")
            # Append additional values in a loop
            for i in range(1, 5001):
                cursor.execute("INSERT INTO {} VALUES (%s, %s)".format(
                    self.test_table), (i, str(i)))

    def _calculate_target_clusters(self, cluster1):
        cluster1_index = cluster1.index('R') + 1
        cluster2_index = int(cluster1[cluster1_index]) + 1
        cluster3_index = cluster2_index + 1

        cluster2 = "CLUSTER{}_A1_V2".format(cluster2_index)
        cluster3 = "CLUSTER{}_A2_V2".format(cluster3_index)

        return cluster2, cluster3

    def _restore_from_origin_cluster(self, cluster0, cluster1, same_account,
                                     rerep):
        # Calculate target clusters based on cluster1 name
        cluster2, cluster3 = self._calculate_target_clusters(cluster1)

        if rerep:
            # restore from original cluster and wait for re-replication
            self._restore_backup_and_validation(cluster0, cluster1)
            if same_account:
                # restore from the cluster (restored from original cluster) to
                # another cluster on the same account and waiting for re-replication
                self._restore_backup_and_validation(cluster1, cluster2)
            else:
                # restore from the cluster (restored from original cluster) to
                # another cluster on a different account and waiting for re-replication
                self._restore_backup_and_validation(cluster1, cluster3)
        else:
            # restore from original cluster without waiting for re-replication
            self._restore_no_rerep_backup_and_validation(cluster0, cluster1)
            if same_account:
                # restore from the cluster (restored from original cluster) to another
                # cluster on the same account without waiting for re-replication
                self._restore_no_rerep_backup_and_validation(
                    cluster1, cluster2)
            else:
                # restore from the cluster (restored from original cluster) to another
                # cluster on a different account without waiting for re-replication
                self._restore_no_rerep_backup_and_validation(
                    cluster1, cluster3)

    def _restore_backup_and_validation(self, source_cluster, target_cluster):
        # restore from source to target
        self._restore_from_to(source_cluster, target_cluster)

        # save blocklist after restoring for validation
        self._save_sb_flags_and_blocklist("{}_before".format(target_cluster))

        # validate blocklist in backup and after restoring
        self._verify_blocklist_after_restore(source_cluster, target_cluster)

        # take another backup
        self._take_backup_save_snapshot("{}_backup".format(target_cluster))

        # save blocklist after taking a backup for validation
        self._save_sb_flags_and_blocklist("{}_after".format(target_cluster))

        # validate blocklist between before and after taking a backup
        self._verify_blocklist_after_backup(target_cluster)

    def _restore_no_rerep_backup_and_validation(self, source_cluster,
                                                target_cluster):
        # restore from source to target without waiting for re-replication
        self._restore_from_to_no_rerep(source_cluster, target_cluster)

        # take another backup of source after restoration
        self._take_backup_save_snapshot("{}_after".format(source_cluster))
        self._save_sb_seg_guids("{}_after".format(source_cluster))
        self._verify_sb_seg_guids_after_restore(source_cluster)

        # take a backup of the target cluster after no re-replication restoration
        self._take_backup_save_snapshot(
            "{}_no_rerep_backup".format(target_cluster))
        self._save_sb_seg_guids("{}_before".format(target_cluster))

        with self.db.cursor() as cursor:
            cursor.execute(TEST_TABLE_DATA.format(self.test_table))
            test_table_data_ret = cursor.fetchall()
        assert self.test_table_data == test_table_data_ret

    def _restore_from_to(self, from_cluster, to_cluster):
        # Set snapshot and backup configurations
        self._set_snapshot(from_cluster)
        self._set_backup(to_cluster)

        # Restore from backup
        self._my_restore_backup("{}_backup".format(from_cluster))
        wait_for(is_rerepp_complete, retries=6, delay_seconds=10)

        # Check if block IDs need to be renamed
        if self._extract_cluster_version(from_cluster) == "1" or \
                self._extract_cluster_account(from_cluster) != \
                self._extract_cluster_account(to_cluster):
            self.should_rename = True
        else:
            self.should_rename = False

        with self.db.cursor() as cursor:
            # make sure table is query-able and handle on_disk
            # bit after restore
            cursor.execute(
                "SELECT *, insertxid, deletexid, oid FROM {}".format(
                    self.test_table))
            cursor.execute(TEST_TABLE_DATA.format(self.test_table))
            test_table_data_ret = cursor.fetchall()
        assert self.test_table_data == test_table_data_ret

    def _save_sb_flags_and_blocklist(self, cluster0):
        with self.db.cursor() as cursor:
            cursor.execute(SUPERBLOCK_FLAG_SQL)
            sb_flags = cursor.fetchall()

            sb_flags = [int(flag[0]) & 1 for flag in sb_flags]
            self.clusters_flags["{}_sb_flags".format(cluster0)] = sb_flags

            cursor.execute(BLOCKLIST_QUERY)
            self.clusters_flags["{}_blocklist_result".format(
                cluster0)] = cursor.fetchall()
            common, dist_backedup, id, dist_flags = self._get_stv_blocklist(
                self.clusters_flags["{}_blocklist_result".format(cluster0)])

            self.clusters_flags["{}_common".format(cluster0)] = common

            self.clusters_flags["{}_backedup".format(cluster0)] = dist_backedup

            self.clusters_flags["{}_id".format(cluster0)] = id

            self.clusters_flags["{}_flags".format(cluster0)] = dist_flags

    def _get_stv_blocklist(self, data):
        distinct_flag_elements = set()
        dist_flags = []
        id_set = []
        dist_backedup_set = set()
        common = []

        for item in data:
            distinct_flag_elements.add(item[-1])
            id_set.append(item[-2])
            dist_backedup_set.add(item[-3])
            common.append(item[:-3])

        for element in distinct_flag_elements:
            dist_flags.append((element, ))

        id = [(element, ) for element in id_set]
        dist_backedup = [(element, ) for element in dist_backedup_set]
        return common, dist_backedup, id, dist_flags

    def _save_sb_seg_guids(self, target_cluster):
        BACKUP_DIR = os.path.join(XEN_ROOT, 'data', 'backup')
        result = ""
        for i in range(3):
            backup_dir = BACKUP_DIR + str(i)
            spec_location = os.path.join(backup_dir, 'uploaded_sb_spec')
            with open(spec_location, 'r') as file:
                lines = file.readlines()[3:]  # Skip the first three lines
                processed_lines = [
                    line.split()[0] for line in lines if line.strip()
                ]  # Extract the first word of each line
            if i == "0":
                result = "\n".join(processed_lines)
            else:
                result += "\n" + "\n".join(processed_lines)
        self.clusters_flags["{}_sb_segguids".format(target_cluster)] = result

    def _verify_sb_seg_guids_after_restore(self, source_cluster):
        before_sb_segguids = self.clusters_flags[
            "{}_before_sb_segguids".format(source_cluster)]
        after_sb_segguids = self.clusters_flags["{}_after_sb_segguids".format(
            source_cluster)]
        # Convert the multi-line string into a list of lines
        before_lines = before_sb_segguids.strip().split('\n')
        after_lines = after_sb_segguids.strip().split('\n')
        lines = len(before_lines)
        for i in range(lines):
            assert before_lines[i] != after_lines[i]

    def _verify_blocklist_after_backup(self, target_cluster):
        before_sb_flags = self.clusters_flags["{}_before_sb_flags".format(
            target_cluster)]
        before_common = self.clusters_flags["{}_before_common".format(
            target_cluster)]
        before_backedup = self.clusters_flags["{}_before_backedup".format(
            target_cluster)]
        before_id = self.clusters_flags["{}_before_id".format(target_cluster)]
        before_flags = self.clusters_flags["{}_before_flags".format(
            target_cluster)]
        after_sb_flags = self.clusters_flags["{}_after_sb_flags".format(
            target_cluster)]
        after_common = self.clusters_flags["{}_after_common".format(
            target_cluster)]
        after_backedup = self.clusters_flags["{}_after_backedup".format(
            target_cluster)]
        after_id = self.clusters_flags["{}_after_id".format(target_cluster)]
        after_flags = self.clusters_flags["{}_after_flags".format(
            target_cluster)]

        # superblock mid's flags should be the same before and after a backup
        assert before_sb_flags == after_sb_flags

        # superblock mid's flags should be the same on all nodes
        before_sb_flags_line_count = len(before_sb_flags)
        assert before_sb_flags_line_count == 1

        # Most columns in stv_blocklist should be the same before and after a backup
        assert before_common == after_common

        # backuped_up column should be 1 before taking a backup if id is renamed
        backedup_should_be = 0
        if self.should_rename:
            backedup_should_be = 0
        else:
            backedup_should_be = 1
        assert before_backedup[0][0] == backedup_should_be

        # backuped_up column should always be 1 after taking a backup
        assert after_backedup[0][0] == 1

        # id should not change before and after taking a backup
        assert before_id == after_id

        # The only difference of flags is BLOCK_PERM_IN_S3 bit before and after taking a
        # backup
        diff_should_be = 0
        if self.should_rename:
            if utils.is_s3commit_enabled(self.cluster):
                # Lose BLOCK_MIRRORED_IN_S3 bit if S3 Commit is enabled
                diff_should_be -= self.block_mirrored_in_s3
            else:
                # Have BLOCK_PERM_IN_S3 bit
                diff_should_be += self.block_perm_in_s3

        after_flags = int(after_flags[0][0]) & self.block_hdr_mask
        before_flags = int(before_flags[0][0]) & self.block_hdr_mask

        flags_diff = after_flags - before_flags
        assert flags_diff == diff_should_be

    def _verify_blocklist_after_restore(self, source_cluster, target_cluster):
        # snapshot_sb_flags = self.clusters_flags["{}_after_sb_flags".format(
        #     source_cluster)]
        snapshot_common = self.clusters_flags["{}_after_common".format(
            source_cluster)]
        # snapshot_backedup = self.clusters_flags["{}_after_backedup".format(
        #     source_cluster)]
        snapshot_id = self.clusters_flags["{}_after_id".format(source_cluster)]
        snapshot_flags = self.clusters_flags["{}_after_flags".format(
            source_cluster)]

        restore_sb_flags = self.clusters_flags["{}_before_sb_flags".format(
            target_cluster)]
        restore_common = self.clusters_flags["{}_before_common".format(
            target_cluster)]
        restore_backedup = self.clusters_flags["{}_before_backedup".format(
            target_cluster)]
        restore_id = self.clusters_flags["{}_before_id".format(target_cluster)]
        restore_flags = self.clusters_flags["{}_before_flags".format(
            target_cluster)]

        # check if flags in superblock is correct
        if (self.should_rename and self.guc_value):
            assert restore_sb_flags[0] == 1
        else:
            assert restore_sb_flags[0] == 0

        # Most columns in stv_blocklist should be the same in both backup and restore
        assert snapshot_common == restore_common

        backedup_should_be = 0
        if self.should_rename:
            backedup_should_be = 0
        else:
            backedup_should_be = 1
        assert restore_backedup[0][0] == backedup_should_be

        if self.should_rename and self.guc_value:
            snapshot_lines_count = len(snapshot_id)
            restore_lines_count = len(restore_id)

            assert snapshot_lines_count == restore_lines_count
            for i in range(snapshot_lines_count):
                assert snapshot_id[i] != restore_id[i]
        else:
            assert snapshot_id == restore_id

        diff_should_be = 0
        if self._extract_cluster_no(source_cluster) == "0":
            diff_should_be += 65536
        if self.should_rename:
            if utils.is_s3commit_enabled(self.cluster):
                # Lose BLOCK_MIRRORED_IN_S3 bit if S3 Commit is enabled
                diff_should_be += self.block_mirrored_in_s3
            else:
                # Have BLOCK_PERM_IN_S3 bit
                diff_should_be -= self.block_perm_in_s3

        restore_flags = restore_flags[0][0] & self.block_hdr_mask
        snapshot_flags = snapshot_flags[0][0] & self.block_hdr_mask
        flags_diff = restore_flags - snapshot_flags
        assert flags_diff == diff_should_be

    def _take_backup_save_snapshot(self, backup_id):
        # Begin backup process
        create_localhost_snapshot(backup_id, wait=True)
        # Copy backup files outside of data dir
        copy_snapshot_post_backup(backup_id)

    def _restore_from_to_no_rerep(self, from_cluster, to_cluster):
        # Set snapshot and backup configurations
        self._set_snapshot(from_cluster)
        self._set_backup(to_cluster)

        self.cluster.set_events_in_padb_conf(['EtDisableRereplication'])
        self._my_restore_backup("{}_no_rerep_backup".format(from_cluster))
        self.cluster.unset_events_in_padb_conf(['EtDisableRereplication'])

    def _my_restore_backup(self, backup_id):
        # Restore backup
        # restore from original cluster and waiting for re-replication
        restore_from_localhost_snapshot(backup_id)
        # wait for restore to finish
        wait_for(is_restore_complete, retries=6, delay_seconds=10)

    def _set_snapshot(self, from_cluster):
        if not from_cluster:
            self.cluster.set_padb_conf_value_unsafe(
                's3_snapshot_prefix_format_version', 1)
            self.cluster.set_padb_conf_value_unsafe('s3_snapshot_key_prefix',
                                                    '')
            self.cluster.set_padb_conf_value_unsafe('s3_snapshot_block_midfix',
                                                    '')
        else:
            cluster = self._extract_cluster_no(from_cluster)
            account = self._extract_cluster_account(from_cluster)
            version = self._extract_cluster_version(from_cluster)
            hashid = self._generate_fake_hash(cluster)
            self.cluster.set_padb_conf_value_unsafe(
                's3_snapshot_prefix_format_version', version)
            self.cluster.set_padb_conf_value_unsafe('s3_snapshot_key_prefix',
                                                    "{}/simul/10{}".format(
                                                        hashid, cluster))
            self.cluster.set_padb_conf_value_unsafe(
                's3_snapshot_block_midfix', "simul/10{}".format(account))

        self.cluster.set_padb_conf_value_unsafe(
            'backup_superblock_segment_size', 10000)

    def _set_backup(self, to_cluster):
        cluster = self._extract_cluster_no(to_cluster)
        account = self._extract_cluster_account(to_cluster)
        version = self._extract_cluster_version(to_cluster)
        hashid = self._generate_fake_hash(cluster)
        self.cluster.set_padb_conf_value_unsafe(
            's3_backup_prefix_format_version', int(version))
        self.cluster.set_padb_conf_value_unsafe('s3_backup_key_prefix',
                                                "{}/simul/10{}".format(
                                                    hashid, cluster))
        self.cluster.set_padb_conf_value_unsafe('cluster_id',
                                                "10{}".format(cluster))
        self.cluster.set_padb_conf_value_unsafe('s3_backup_block_midfix',
                                                "simul/10{}".format(account))
        self.cluster.set_padb_conf_value_unsafe(
            'backup_superblock_segment_size', 10000)

    def _extract_cluster_no(self, cluster_name):
        index = cluster_name.find('R')
        if index != -1 and index + 1 < len(cluster_name):
            return cluster_name[index + 1]
        else:
            return None

    def _extract_cluster_account(self, cluster_name):
        # Find the index of '_A' in the string cluster_name
        index = cluster_name.find('_A')

        # Extract and return the character immediately following '_A'
        # Checking if '_A' was found and ensuring we do not go out of range
        if index != -1 and index + 2 < len(cluster_name):
            return cluster_name[index + 2]
        else:
            return None  # Return None if '_A' is not found or no character after '_A'

    def _extract_cluster_version(self, cluster_name):
        # Find the index of 'V' in the string cluster_name
        index = cluster_name.find('V')
        index = index + 1
        # Return the substring from 'V' to the end of cluster_name
        # Check if 'V' was found; if not, return the whole string
        if index != -1:
            return cluster_name[index:]
        else:
            return cluster_name

    def _generate_fake_hash(self, cluster):
        return chr(ord("A") + int(cluster)) * 6

    def _backup_restore(self,
                        cluster_session,
                        cluster,
                        encryptions="encrypted",
                        same_account=True,
                        rerep=True,
                        target_cluster="CLUSTER1_A1_V2"):
        self.block_hdr_modified = 1 << 9
        self.block_has_preferred_disk = 1 << 15
        self.block_hdr_mask = self.block_hdr_modified | \
            self.block_has_preferred_disk
        self.block_hdr_mask = ~self.block_hdr_mask
        self.block_perm_in_s3 = 1 << 1
        self.block_mirrored_in_s3 = 1 << 24

        self.test_table = "rename_block_id_tbl"
        self.clusters_flags = {}
        self.guc_value = True
        self.version = "V2"

        if encryptions == "encrypted":
            self.encryption = True
        else:
            self.encryption = False

        self.cluster = cluster
        self.restore_full_superblock_value = "false"
        self.backup_superblock_format_value = "1"

        cluster0 = "CLUSTER0_A1_V2"
        cluster_no, account_no, version_no = '0', '1', '2'
        hashid = self._generate_fake_hash(cluster_no)

        custom_gucs = {
            "enable_rename_snapshot_block_guid": self.guc_value,
            "backup_superblock_segment_size": 10000,
            "s3_backup_prefix_format_version": int(version_no),
            "s3_backup_key_prefix": "{}/simul/10{}".format(hashid, cluster_no),
            "cluster_id": "10{}".format(cluster_no),
            "s3_backup_block_midfix": "simul/10{}".format(account_no),
            "s3_snapshot_prefix_format_version": 1,
            "s3_snapshot_key_prefix": "",
            "s3_snapshot_block_midfix": "",
            "force_upgrade_systbl": True
        }

        with cluster_session(
                enable_encryption=self.encryption,
                gucs=custom_gucs,
                clean_db_before=True,
                clean_db_after=True) as cluster_session:
            with self.db.cursor() as cursor:
                self._load_data()
                cursor.execute(TEST_TABLE_DATA.format(self.test_table))
                self.test_table_data = cursor.fetchall()

                # blocklist before taking the first backup
                self._save_sb_flags_and_blocklist("{}_before".format(cluster0))

                # Backup and copy file outside of data dir
                backup_id = "{}_backup".format(cluster0)
                if not rerep:
                    backup_id = "{}_no_rerep_backup".format(cluster0)
                self._take_backup_save_snapshot(backup_id)

                self._save_sb_seg_guids("{}_before".format(cluster0))

                # Blocklist after taking the first backup
                self._save_sb_flags_and_blocklist("{}_after".format(cluster0))

                self.should_rename = True
                self.to_cluster = cluster0

                self._verify_blocklist_after_backup(cluster0)

                self._restore_from_origin_cluster(cluster0, target_cluster,
                                                  same_account, rerep)
