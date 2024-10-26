import os
import logging
import json
import subprocess
import pytest
from raff.common.simulated_helper import XEN_ROOT
from raff.backup_restore.bar_test import BARTestSuite
from raff.common.simulated_helper import (create_localhost_snapshot,
                                          restore_from_localhost_snapshot,
                                          is_restore_complete,
                                          is_rerepp_complete,
                                          wait_for,
                                          copy_snapshot_post_backup)
from raff.common.cluster.cluster_session import ClusterSession


BLOCK_BACKUP_QUERY = '''
SELECT count(*)
FROM stv_blocklist
WHERE backed_up=1;'''

log = logging.getLogger(__name__)


@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.session_ctx(user_type='bootstrap')
class TestVerifyColsInBlockManifest(BARTestSuite):
    def create_and_load_basic_table(self, cursor):
        '''
        This function executes a series of SQL queries, including:
        1. Dropping a table if it exists
        2. Creating a table
        3. Inserting data to the table

        Args:
            db_cursor: A database cursor object for executing queries.
        '''
        cursor.execute("drop table if exists bar_test1;")
        cursor.execute("create table bar_test1 (a bigint, b bigint);")
        cursor.execute("insert into bar_test1 values (1, 1), (1, 2), (1, 3);")

    def populate_bar_test_table(self, cursor, num_records):
        '''
        This function executes a series of SQL queries, including:
        1. Dropping a table if it exists
        2. Creating a table
        3. Inserting data to the table until

        Args:
            db_cursor: A database cursor object for executing queries.
            num_records(int): The number of records desired to add.
        '''
        cursor.execute("drop table if exists bar_tests;")
        cursor.execute("create table bar_tests(a float, b float, c float, d float);")
        cursor.execute("insert into bar_tests values (random(),  \
                        random(), random(), random());")
        table_size = 1
        while table_size < num_records:
            cursor.execute("insert into bar_tests select random(), random(), \
                           random(), random() from bar_tests;")
            table_size *= 2

    def get_num_nodes(self):
        '''
        This function retrieves the number of nodes.
        '''
        num_nodes = 0
        with open(f"{XEN_ROOT}/data/xen.topology.json", 'r') as f:
            data = json.load(f)
            for item in data:
                if "node_addr" in item:
                    num_nodes += 1
        return num_nodes

    def verify_cols_in_block_manifest(self, cluster):
        '''
        This function does the following verifications.
            1. Verify Number of columns in block manifest for each node
            2. Verify whether Block Manifest Cols is expected
            3. Verify whether First column is expected
            4. Verify whether Second column is expected
            5. Verify whether any column on any node is empty

        Args:
            cluster (RedshiftCluster): The Redshift cluster instance.
        '''
        expected_cols = 9
        s3_backup_key_prefix = cluster.get_guc_value('s3_backup_key_prefix')
        cluster_id = os.path.basename(s3_backup_key_prefix)
        num_nodes = self.get_num_nodes()
        for i in range(num_nodes):
            blklst_zip = f"{XEN_ROOT}/data/backup{i}/superblocks/blklst_*.gz"
            blklst_loc = f"{XEN_ROOT}/data/backup{i}/superblocks/blklst_*"
            subprocess.run(["gunzip", blklst_zip])

            log.info(f"Node {i}: Verify Number of columns in block manifest")
            with open(blklst_loc, 'r') as f:
                lines = f.readlines()
                first_row_cols = len(lines[0].split())
                first_col = f.readline().split()[0]
                second_col = f.readline().split()[1]

                if first_row_cols != expected_cols:
                    log.info(f"Node {i}: Block Manifest Cols expected: \
                             {expected_cols}, Actual: {first_row_cols}")
                    pytest.fail()

                if first_col != cluster_id:
                    log.info(f"Node {i}: First column expected: \
                             {cluster_id}, Actual: {first_col}")
                    pytest.fail()

                if second_col != str(i):
                    log.info(f"Node {i}: Second column expected: {i}, \
                             Actual: {second_col}")
                    pytest.fail()

                for line in f:
                    columns = line.split()
                    for j in range(2, len(columns)):
                        if not columns[j]:
                            log.info(f"Node {i}: Column {j} is empty")
                            pytest.fail()

    def restore_from_localhost_snapshot_wait(self, backup_id):
        '''
        This function does the following verifications.
            1. restore from localhost snapshot
            2. wait for restoring steps to complete

        Args:
            backup_id(str): A backup_id for checking.
        '''
        restore_from_localhost_snapshot(backup_id, do_restore_bootstrap=True)
        wait_for(is_restore_complete, retries=30, delay_seconds=10)
        wait_for(is_rerepp_complete, retries=30, delay_seconds=10)
        log.info('successfully restored {}'.format(backup_id))

    def create_localhost_snapshot_check(self, backup_id, cluster, cursor):
        '''
        This function does the following verifications.
            1. create local host snapshot
            2. verify enc_key_hash value
            3. verify cols_in_block_manifest value

        Args:
            backup_id(str): A backup_id for checking.
            cluster (RedshiftCluster): The Redshift cluster instance.
            cursor: A database cursor object for executing queries.
        '''
        create_localhost_snapshot(backup_id, wait=True)
        copy_snapshot_post_backup(backup_id)
        log.info('successfully backed up {}'.format(backup_id))
        self.verify_cols_in_block_manifest(cluster)

    def test_verify_cols_in_block_manifest(self, db_session, cluster):
        '''
        This test verifies enc_key_hash value and
        cols_in_block_manifest after creating backup
        and restoring backup for format 1 guc settings.
        backup_test_1: benchmark
        backup_test_2: has more data compared with backup_test_1
        backup_test_3: backup after restoring from backup_test_1
        backup_test_4: backup after restoring from backup_test_2

        Args:
            cluster (RedshiftCluster): The Redshift cluster instance.
            db_cursor: A database cursor object for executing queries.
            vector: A vector object for Dimensions.
        '''
        session = ClusterSession(cluster)
        with session(clean_db_before=True,
                     clean_db_after=True):
            with db_session.cursor() as cursor:
                # create tables and add data
                self.create_and_load_basic_table(cursor)
                log.info("create tables and add data")

                # create backup_test_1
                BACKUP_ID = 'backup_test_1'
                self.create_localhost_snapshot_check(BACKUP_ID, cluster, cursor)

                # restore snapshot backup_test_1
                self.restore_from_localhost_snapshot_wait(BACKUP_ID)

            with db_session.cursor() as cursor:
                # now try backuping and restoring the restored cluster.
                BACKUP_ID3 = 'backup_test_3'
                self.create_localhost_snapshot_check(BACKUP_ID3, cluster, cursor)

            with db_session.cursor() as cursor:
                # create tables and add data
                self.create_and_load_basic_table(cursor)

                # add some more data
                self.populate_bar_test_table(cursor, 16)

                # add more data to a table
                cursor.execute("insert into bar_test1 values (1, 4), (1, 5), (1, 6);")

                # create backup_test_2
                BACKUP_ID2 = 'backup_test_2'
                self.create_localhost_snapshot_check(BACKUP_ID2, cluster, cursor)

                # restore snapshot backup_test_2
                self.restore_from_localhost_snapshot_wait(BACKUP_ID2)

            with db_session.cursor() as cursor:
                # now try backuping and restoring the restored cluster.
                BACKUP_ID4 = 'backup_test_4'
                self.create_localhost_snapshot_check(BACKUP_ID4, cluster, cursor)
