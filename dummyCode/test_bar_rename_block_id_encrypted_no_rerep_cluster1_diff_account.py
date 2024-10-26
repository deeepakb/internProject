# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Test for DP-6917: Rename Block IDs when restoring from shared snapshots
#
# Test Creates a non-encrypted V2 cluster and an encrypted V2 cluster.
#
# For each created cluster:
# 1. Load data and take backup BACKUP0 on CLUSTER0.
# 2. Restore BACKUP0 to CLUSTER1 on the same account, take BACKUP1.
# 3. Restore BACKUP1 to CLUSTER2 on the same account, take BACKUP2.
# 4. Restore BACKUP1 to CLUSTER3 on a different account, take BACKUP3.
# 5. Restore BACKUP0 to CLUSTER4 on a different account, take BACKUP4.
# 6. Restore BACKUP4 to CLUSTER5 on the same account, take BACKUP5.
# 7. Restore BACKUP4 to CLUSTER6 on a different account, take BACKUP6.

# Cluster Name Format: CLUSTER{number}_A{account}_V{version}

# ## Restore Flow Chart
#                                   CLUSTER2_A1_V2
#                                 /
#                  CLUSTER1_A1_V2
#                 /               \
#               /                   CLUSTER3_A2_V2
#          CLUSTER0_A1_V2
# (encrypted/non_encrypted)
#               \                   CLUSTER5_A1_V2
#                 \               /
#                  CLUSTER4_A2_V2
#                                 \
#                                   CLUSTER6_A2_V2

# Validation:

# 1. Compare stv_blocklist before/after backup and restore
# 2. Superblock flags, most columns should be the same
# 3. backuped_up should be 1 for non-shared, 0 for shared
# 4. id should not change for non-shared, changed for shared
# 5. BLOCK_PERM_IN_S3 flags should be the same for non-shared
# 6. Query test table to validate during re-replication
#

import pytest
from test_bar_rename_block_id import TestBarRenameBlockID


@pytest.mark.localhost_only
@pytest.mark.serial_only
class TestBarRenameBlockIDEncryptedNoRerepCluster1DiffAccount(
        TestBarRenameBlockID):
    def test_bar_rename_block_id_encrypted_no_rerep_cluster1_diff_account(
            self, cluster_session, cluster):
        self._backup_restore(
            cluster_session,
            cluster,
            encryptions="encrypted",
            same_account=False,
            rerep=False,
            target_cluster="CLUSTER1_A1_V2")
