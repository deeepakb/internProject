import logging

import pytest

from raff.backup_restore.bar_test import BARTestSuite
from raff.common.cluster.cluster_session import ClusterSession
from raff.common.simulated_helper import create_localhost_snapshot

BLOCK_BACKUP_QUERY = '''
SELECT count(*)
FROM stv_blocklist
WHERE backed_up=1;'''

log = logging.getLogger(__name__)


@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.session_ctx(user_type='bootstrap')
class TestClearInS3Bit(BARTestSuite):
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

    def get_number_blocks_backed_up(self, cursor):
        '''
        This function executes SQL query to extract
        the number of blocks already backed up.

        Args:
            db_cursor: A database cursor object for executing queries.

        Returns:
            int: The count of blocks already backed up.
        '''
        cursor.execute(BLOCK_BACKUP_QUERY)
        res = cursor.fetchall()
        return res[0][0]

    def test_clear_in_s3_bit(self, db_session, cluster):
        '''
        Test whether xpx 'clear_in_s3_bits -1 -1' can work to
        clear s3 bits.

        Args:
            cluster (RedshiftCluster): The Redshift cluster instance.
            db_session (DatabaseSession): The database session.
            vector: A vector object for Dimensions.

        Returns:
            None
        '''
        session = ClusterSession(cluster)
        with session(clean_db_before=True,
                     clean_db_after=True):
            with db_session.cursor() as cursor:
                # create and load data
                self.create_and_load_basic_table(cursor)
                log.info("create tables and add data")

                # check no blocks backed up in s3 currently
                res = self.get_number_blocks_backed_up(cursor)
                assert res == 0
                log.info("check no blocks backed up in s3 currently")

                # take backup and wait it to finish
                BACKUP_ID = 'uuidgen'
                create_localhost_snapshot(BACKUP_ID, wait=True)
                log.info("check no blocks backed up in s3 currently")

                # check number of blocks back up in s3
                res = self.get_number_blocks_backed_up(cursor)
                log.info(res)
                log.info("Found {} blocks which have been backed up to S3".format(res))
                log.info("check no blocks backed up in s3 currently")

                # clear bits for all blocks
                cluster.set_event("EtClearInS3Bits")
                cursor.execute("xpx 'clear_in_s3_bits -1 -1';")
                log.info("clear bit for all blocks")

                # verify bit has been cleared for all blocks
                res = self.get_number_blocks_backed_up(cursor)
                assert res == 0
                log.info("verify bit has been cleared for all blocks")
