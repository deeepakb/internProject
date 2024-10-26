# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
from raff.util.utils import wait_for
from raff.common.simulated_helper import is_rerepp_complete
from raff.common.dimensions import Dimensions
from raff.backup_restore.bar_test import BARTestSuite

# Amount of data load.
MIN_ROWS = 500000
APPEND_ROWS = 100000
# 16777218 = 1<<24 (BLOCK_MIRRORED_IN_S3) + 1<<1 (BLOCK_PERM_IN_S3).
BACKUP_STATE_QUERY = """
(
    SELECT COUNT(*), 'not_in_s3_backup' AS state
    FROM stv_blocklist
    WHERE tbl > 0 AND col = 0 AND flags & 16777218 != 2
)
UNION
(
    SELECT COUNT(*), 'in_s3_backup' AS state
    FROM stv_blocklist
    WHERE tbl > 0 AND col = 0 AND flags & 16777218 = 2
)
ORDER BY state;
"""

# We would like to rerep those not-in-S3 blocks first, before start rereping
# blocks that are already in S3. This is to minimize the data loss window and
# for performance reason. Therefore, we check here that rerep is done in order:
# We expect that once we saw the first block in S3 is rereped, then there should
# be at most (num_rerep_chains - 1) not in S3 blocks that are not backed up yet.
# If there are more, then rerep did not happen in order.
# See SIM: https://issues.amazon.com/issues/RedshiftDP-4204 for details.
OUT_OF_ORDER_QUERY = """
SELECT
    a.new_node
    , COUNT(*)
FROM stl_rereplication AS a
    JOIN (
        SELECT new_node, MIN(recordtime) AS min
        FROM stl_rereplication
        WHERE flags & 2 = 2
        GROUP BY new_node
        ORDER BY new_node
    ) AS b
    ON a.new_node = b.new_node AND a.recordtime > b.min AND a.flags & 2 = 0
GROUP BY a.new_node;
"""


@pytest.mark.serial_only
class TestBARRerep(BARTestSuite):
    """
    Test to check if rerep happens in the expected order (i.e. not-in-s3 blocks
    first). The backup state check steps marked with "()" are optional and are
    included only in the first iteration (i.e. with the first set of parameters).
    1. Insert some data into cluster
    (1.5. Verify no block has been backed up yet)
    2. Take backup
    (2.5. Verify all blocks are backed up)
    3. insert more data after backup
    (3.5. Verify some blocks are backed up, some are not)
    4. Trigger a rerep, wait for rerep to finish
    (4.5. Verify that backup information are the same as before fail LUN)
    5. Run correctness query and check results are expected or not
    """
    is_first = True

    @classmethod
    def modify_test_dimensions(cls):
        # Need only backup_restore format v2 and backup_sb_format v1.
        gucs = BARTestSuite.backup_restore_configs_v2()
        gucs['backup_sb_format_restore_full_sb'] = [('1', 'false')]
        # List of parameters to test, each item is consists of
        # [num_rerep_threads, num_disks].
        gucs['num_rerep_threads_and_num_disks'] = [[1, 2], [8, 4]]

        return Dimensions(gucs)

    def test_bar_rerep(self, cluster_session, cluster, vector):
        # Get general gucs.
        custom_gucs = self.get_guc_dict_from_dimensions(vector)
        # Update gucs specific to this test.
        num_rerep_threads = vector.num_rerep_threads_and_num_disks[0]
        num_disks = vector.num_rerep_threads_and_num_disks[1]
        disk_list = []
        for i in range(num_disks):
            disk_list.append("disk{}".format(i))
        disk_str = ",".join(disk_list)
        custom_gucs.update({
            "num_rerep_chains": vector.num_rerep_threads_and_num_disks[0],
            "rerep_chains_min": vector.num_rerep_threads_and_num_disks[0],
            "disk_list": disk_str,
            # Disable all workers.
            "vacuum_auto_enable": "false",
            "vacuum_auto_worker_enable": "false",
            "auto_analyze": "false",
            "enable_auto_alter_table_worker": "false",
            "enable_auto_undo": "false",
            "enable_catalog_rebuild_worker": "false",
            "enable_auto_alter_encoding_mode": 0,
            "enable_auto_alter_sortkey_mode": 0,
            "enable_auto_alter_distkey_mode": 0,
            "enable_auto_alter_disteven_mode": 0,
            "mv_auto_refresh_workers_count": 0,
            "mv_enable_workers": "false",
            "ml_enable_ml_manager": "false",
        })

        # Start session.
        with cluster_session(gucs=custom_gucs,
                             clean_db_before=True, clean_db_after=True):
            with self.db.cursor() as cursor:
                # Prepare data.
                self.create_table_insert_random_rows(cursor, MIN_ROWS)

                if TestBARRerep.is_first:
                    # Check no block has been backed up yet.
                    cursor.execute(BACKUP_STATE_QUERY)
                    result = cursor.fetchall()
                    num_backed_up_blocks = result[0][0]
                    assert num_backed_up_blocks == 0, "No blocks should "\
                        "have been backed up, now {} blocks are backed up."\
                        .format(result[0][0])

                # Take a backup.
                self.take_backup(should_wait=True)

                if TestBARRerep.is_first:
                    # Check all disks are backed up.
                    cursor.execute(BACKUP_STATE_QUERY)
                    result = cursor.fetchall()
                    num_non_backed_up_blocks = result[1][0]
                    assert num_non_backed_up_blocks == 0, "All blocks should "\
                        "have been backed up, now {} blocks are non backed up."\
                        .format(result[1][0])

                # Insert more data after backup.
                self.append_random_rows(cursor, APPEND_ROWS)

                # Save currect backup stats.
                num_backed_up_blocks_before = 0
                num_non_backed_up_blocks_before = 0
                if TestBARRerep.is_first:
                    # Check some blocks are backed up and some are not.
                    cursor.execute(BACKUP_STATE_QUERY)
                    result = cursor.fetchall()
                    num_backed_up_blocks_before = result[0][0]
                    num_non_backed_up_blocks_before = result[1][0]
                    assert num_backed_up_blocks_before > 0\
                        and num_non_backed_up_blocks_before > 0,\
                        "Blocks should have been partially backed up, "\
                        "now either num of backed up blocks (= {}) is 0"\
                        "or num of non backed up blocks (= {}) is 0."\
                        .format(num_backed_up_blocks_before,
                                num_non_backed_up_blocks_before)

                # Trigger a rerep.
                cluster.run_xpx('start_block_rerep')
                # Wait 60 seconds for the backup to finish.
                wait_for(is_rerepp_complete, retries=6, delay_seconds=10)

                if TestBARRerep.is_first:
                    # Check some blocks are backed up and some are not, also
                    # check backup stats are the same as before rerep.
                    cursor.execute(BACKUP_STATE_QUERY)
                    result = cursor.fetchall()
                    num_backed_up_blocks = result[0][0]
                    num_non_backed_up_blocks = result[1][0]
                    assert num_backed_up_blocks > 0 and num_non_backed_up_blocks > 0,\
                        "Blocks should have been partially backed up, "\
                        "now either num of backed up blocks (= {}) is 0"\
                        "or num of non backed up blocks (= {}) is 0."\
                        .format(num_backed_up_blocks, num_non_backed_up_blocks)
                    assert num_backed_up_blocks == num_backed_up_blocks_before,\
                        "Number of backed up blocks should not change "\
                        "after rerep, now {} != "\
                        "expected {}.".format(num_backed_up_blocks,
                                              num_backed_up_blocks_before)
                    assert num_non_backed_up_blocks == num_non_backed_up_blocks_before,\
                        "Number of non backed up blocks should not change "\
                        "after rerep, now {} != "\
                        "expected {}.".format(num_non_backed_up_blocks,
                                              num_non_backed_up_blocks_before)

                cursor.execute(OUT_OF_ORDER_QUERY)
                result = cursor.fetchall()

                for i in range(len(result)):
                    count = result[i][1]
                    assert count <= num_rerep_threads - 1,\
                        "Num of out of order blocks should not be larger "\
                        "than num_rerep_threads - 1, now it's {} > {} - 1, "\
                        "indicates out of order rereplication."\
                        .format(count, num_rerep_threads)

                TestBARRerep.is_first = False
