# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
from raff.common.dimensions import Dimensions
from raff.backup_restore.bar_test import BARTestSuite
from raff.backup_restore.restore_delay_rerep_test import RestoreDelayRerepTestSuite


class TestRestoreDelayRerepReads(RestoreDelayRerepTestSuite):
    """
    Test to validate rerep after a restore, in the presence of events that
    delay rerep reads

    NOTE: This test is ported from the redbox test
    test/leader_durability/bar_test_restore.bsh.
    """

    @classmethod
    def modify_test_dimensions(cls):
        gucs = BARTestSuite.backup_restore_configs_v2()
        gucs['disk_mirror_count'] = ['0', '1']
        return Dimensions(gucs)

    def test_restore_delay_rerep_reads(self, cluster, cluster_session,
                                       db_session, vector):
        gucs = dict(
            disk_mirror_count=vector.disk_mirror_count,
            s3_backup_prefix_format_version=vector.
            backup_snapshot_prefix_format_version,
            s3_snapshot_prefix_format_version=vector.
            backup_snapshot_prefix_format_version,
            backup_superblock_format=vector.backup_sb_format_restore_full_sb[0],
            backup_superblock_segment_size=vector.backup_sb_segment_size,
            restore_full_superblock=vector.backup_sb_format_restore_full_sb[1],
            var_slice_mapping_policy=vector.var_slice_mapping_policy)
        self.restore_delay_rerep_reads(cluster, cluster_session, db_session,
                                       gucs)
