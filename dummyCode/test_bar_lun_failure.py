# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
from raff.common.dimensions import Dimensions
from raff.backup_restore.bar_test import BARTestSuite
from raff.util.utils import wait_for

MIN_ROWS = 100000
NODE_NUM_TO_FAIL = [0]
DISK_NUM_TO_FAIL = [0]

# Select disk usage data on a given LUN, based on the node and disk num
# specified by the "AND" clause appended to the end of the WHERE clause, which
# is added by `_assert_disk_usage_data_existance_at_node`. If data exist, query
# will return a row like below, otherwise will return no results.
# node       | blocks
# <node_num> | <disk_num>
DISK_USAGE_QUERY = """
SELECT da.node,
    da.diskno AS blocks
FROM stv_blocklist bl, stv_disk_addresses da
WHERE bl.sb_pos = da.sb_pos AND bl.slice = da.slice
    AND bl.tbl > 0 AND tombstone = 0 {}
GROUP BY node, diskno ORDER BY node, diskno;
"""
ASSERT_EMPTY_ERROR_MSG = "Should be empty on node {} and disk {}, now it has data"
ASSERT_DATA_EXIST_ERROR_MSG = "Should have data on node {} and disk {}, now it's empty"


@pytest.mark.serial_only
class TestBARLunFailure(BARTestSuite):
    """
    Test to check if lun failure is handled properly.
    1. Load data.
    2. Check disk usage data exist on given LUN.
    3. Fail LUN.
    4. Check disk usage data is empty on given LUN.
    5. Recover LUN.
    6. Check disk usage data exist on given LUN.
    """

    @classmethod
    def modify_test_dimensions(cls):
        # Need only backup_restore format v2 and backup_sb_format v1.
        gucs = BARTestSuite.backup_restore_configs_v2()
        gucs['backup_sb_format_restore_full_sb'] = [('1', 'false')]

        return Dimensions(gucs)

    def _check_disk_usage_data_existance_at_node(self, cursor, node_num,
                                                 disk_num, expect_data_exist):
        """
        Check (node_num, disk_num) have data or not, depends on the input flag.
        This is adopted from test/raff/tiered_storage/test_ts_disk_failure.py

        Args:
            cursor: Cursor to execute queries against
            node_num (int): node to be checked
            disk_num (int): disk to be checked
            expect_data_exist (bool): True if data should exist, False if data
                should be empty.

        Returns:
            Return True if data existance state is as expected, False otherwise.
        """
        node_disk = "AND da.node = {} AND da.diskno = {}".format(node_num,
                                                                 disk_num)
        cursor.execute(DISK_USAGE_QUERY.format(node_disk))
        expected_results = cursor.fetchall()
        return (bool(expected_results) == expect_data_exist)

    def test_bar_lun_failure(self, cluster_session, cluster, vector):
        # Get general gucs.
        custom_gucs = self.get_guc_dict_from_dimensions(vector)
        # If not 0, selective_dispatch_level may interact with lun test.
        # Disable tiered storage since this test currently fails with
        # tiered storage. Will follow up in TODO(DP-66276).
        custom_gucs.update({
            "selective_dispatch_level": 0,
            "tiered_storage_enabled": "false",
            "superblock_capacity_extended": 0
        })

        with cluster_session(gucs=custom_gucs,
                             clean_db_before=True, clean_db_after=True):
            with self.db.cursor() as cursor:
                # Prepare data
                self.create_table_insert_random_rows(cursor, MIN_ROWS)

                for node_num in NODE_NUM_TO_FAIL:
                    for disk_num in DISK_NUM_TO_FAIL:
                        assert self._check_disk_usage_data_existance_at_node(
                            cursor, node_num, disk_num, expect_data_exist=True),\
                            ASSERT_DATA_EXIST_ERROR_MSG.format(node_num, disk_num)

                        # Fail LUN, check if disk usage on that LUN is emptied.
                        self.fail_disk_on_node(cluster, node_num, disk_num)
                        try:
                            wait_for(self._check_disk_usage_data_existance_at_node,
                                     args=[cursor, node_num, disk_num, False],
                                     retries=6, delay_seconds=5)
                        except TimeoutError:
                            raise AssertionError(
                                ASSERT_EMPTY_ERROR_MSG.format(node_num, disk_num))

                        # Recover LUN, check disk usage on that LUN is restored.
                        self.recover_disk_on_node(cluster, node_num, disk_num)
                        try:
                            wait_for(self._check_disk_usage_data_existance_at_node,
                                     args=[cursor, node_num, disk_num, True],
                                     retries=6, delay_seconds=20)
                        except TimeoutError:
                            raise AssertionError(
                                ASSERT_DATA_EXIST_ERROR_MSG.format(node_num, disk_num))
