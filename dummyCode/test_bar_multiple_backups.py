# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import os
import pytest
from raff.common.dimensions import Dimensions
from raff.backup_restore.bar_test import BARTestSuite

MIN_ROWS = 100000
APPEND_ROWS = 100000
# This query will give us the number of backed up / non backed up blocks, order
# by backed_up flag value. So if backed_up = 0 block exists, it must be the
# first row. Output like:
# backed_up | count
# 0 | <COUNT_NON_BACKED_UP>
# 1 | <COUNT_BACKED_UP>
BLOCKLIST_QUERY = """
WITH tbl_id AS (
    SELECT oid AS id, relname AS name
    FROM pg_class
    WHERE relname ILIKE '%{}%'
)
SELECT backed_up, COUNT(1)
FROM stv_blocklist JOIN tbl_id ON (tbl_id.id = tbl)
WHERE tbl > 0 AND tombstone = 0
GROUP BY 1
ORDER BY 1;
"""
TBL_NAME = "bar_tests"


@pytest.mark.serial_only
class TestBARMultipleBackups(BARTestSuite):
    """
    Test to check if multiple backups are created correctly.
    1. Create a table and insert data.
    2. Verify that we have some non backed up blocks.
    3. Take a backup and wait for it to finish.
    4. Verify all blocks are backed up.
    5. Verify at least 1 superblock exist for each node in the backup directory.
    6. Add some more data.
    7. Verify that we have some non backed up blocks.
    8. Take another backup and wait for it to finish.
    9. Verify all blocks are backed up.
    10. Verify at least 1 superblock exist for each node in the backup directory.
    """

    @classmethod
    def modify_test_dimensions(cls):
        # Need only backup_restore format v2 and backup_sb_format v1.
        gucs = BARTestSuite.backup_restore_configs_v2()
        gucs['backup_sb_format_restore_full_sb'] = [('1', 'false')]

        return Dimensions(gucs)

    def _verify_backup_directory(self, cursor, backup_id):
        # Verify that superblock exists for each node in the backup directory.
        cursor.execute("SELECT count(distinct(node)) FROM stv_slices")
        num_nodes = cursor.fetch_scalar()
        for i in range(num_nodes):
            xen_root = os.environ.get('XEN_ROOT')
            backup_dir = xen_root + '/data/backup{}/superblocks/'.format(i)
            files = os.listdir(backup_dir)
            num_sb_per_node = 0
            for file in files:
                if "sb_{}".format(backup_id) in file:
                    num_sb_per_node += 1
            assert num_sb_per_node > 0,\
                "Should have superblock with backup id {} on each node in {}, "\
                "now have {} on node {}".format(backup_id, backup_dir,
                                                num_sb_per_node, i)

    def test_bar_multiple_backups(self, cluster_session, vector):
        # Get general gucs.
        custom_gucs = self.get_guc_dict_from_dimensions(vector)

        # Start session.
        with cluster_session(gucs=custom_gucs,
                             clean_db_before=True, clean_db_after=True):
            with self.db.cursor() as cursor:
                # Prepare data.
                self.create_table_insert_random_rows(cursor, MIN_ROWS)

                # Verify that we have non backed up blocks.
                cursor.execute(BLOCKLIST_QUERY.format(TBL_NAME))
                result = cursor.fetchall()
                assert len(result) == 1 and int(result[0][0]) == 0, \
                    "Should have only non backed "\
                    "up blocks, now either no block exists or backed up blocks"\
                    "exist."

                # Take backup and wait for it to finish.
                backup_id = self.take_backup()

                # Verify all blocks are backed up.
                cursor.execute(BLOCKLIST_QUERY.format(TBL_NAME))
                result = cursor.fetchall()
                assert int(result[0][0]) == 1, \
                    "All blocks should have been backed up, now have {} non "\
                    "backed up blocks."\
                    .format(int(result[0][1]))

                # Verify the backup have superblock for each node.
                self._verify_backup_directory(cursor, backup_id)

                # Add some more data.
                self.append_random_rows(cursor, APPEND_ROWS)

                # Verify that we have non backed up blocks.
                cursor.execute(BLOCKLIST_QUERY.format(TBL_NAME))
                result = cursor.fetchall()
                assert len(result) == 2 and int(result[0][0]) == 0, \
                    "Should have some non backed "\
                    "up blocks, now don't have any non backed up blocks."

                # Take another backup and wait for it to finish.
                backup_id = self.take_backup()

                # Verify all blocks are backed up.
                cursor.execute(BLOCKLIST_QUERY.format(TBL_NAME))
                result = cursor.fetchall()
                assert int(result[0][0]) == 1, \
                    "All blocks should have been backed up, now have {} non "\
                    "backed up blocks."\
                    .format(int(result[0][1]))

                # Verify the backup have superblock for each node.
                self._verify_backup_directory(cursor, backup_id)
