import logging
import pytest
import os
from py_lib.common.xen_guard_control import XenGuardControl
from raff.backup_restore.bar_test import BARTestSuite
from raff.common.cluster.cluster_session import ClusterSession
from raff.common.simulated_helper import (create_localhost_snapshot,
                                          stop_sim_db,
                                          copy_snapshot_post_backup,
                                          is_backup_finished,
                                          is_restore_complete,
                                          is_rerepp_complete,
                                          wait_for,
                                          restore_from_localhost_snapshot)

DATA_DIR = os.path.dirname(os.environ["PGDATA"])
BAR_TEST_BLOCKLIST_BEFORE = '''select
case when count(1)>0
then 'pass'
else 'fail'
end as check_status
from stv_blocklist where tbl > 0 and backed_up = 0;'''

BAR_TEST_BLOCKLIST_AFTER_1 = '''
select case when count(1)=0
then 'pass one' else 'fail one'
end as check_status
from stv_blocklist
where tbl > 0 and tombstone = 0 and backed_up = 0;'''

BAR_TEST_BLOCKLIST_AFTER_2 = '''
select case when count(1)>0
then 'pass two' else 'fail two'
end as check_status
from stv_blocklist
where tbl > 0 and tombstone = 0 and backed_up = 1;'''

log = logging.getLogger(__name__)


@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.session_ctx(user_type='bootstrap')
class TestBlocklistandUnderrepedCheck(BARTestSuite):

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

    def num_underrepped_blocks(self, cluster, cursor):
        '''
        This function executes a SQL query to retrieve the number
        of underrepped blocks

        Args:
            cluster (RedshiftCluster): The Redshift cluster instance.
            db_cursor: A database cursor object for executing queries.
        '''
        query = "select count(*) from stv_underrepped_blocks"
        s3_mirror_guc = cluster.get_guc_value("enable_mirror_to_s3")
        disable_mirror_in_s3_block_guc = cluster.\
            get_guc_value("disable_mirror_in_s3_block")

        if s3_mirror_guc != "0" and disable_mirror_in_s3_block_guc == "off":
            query += " t2 where exists (select slice from stv_slices t1 where"
            query += " exists (select * from stl_disk_failures where"
            query += " node = t1.node) and slice=t2.slice)"

        if s3_mirror_guc != "0" and disable_mirror_in_s3_block_guc == "off":
            query += " and sb_pos not in (select sb_pos from stv_blocklist"
            query += " where tbl = -12 and tombstone > 0)"
        else:
            query += " where sb_pos not in (select sb_pos from stv_blocklist"
            query += " where tbl = -12 and tombstone > 0)"

        query += ";"

        cursor.execute(query)
        res = cursor.fetchall()[0][0]
        return res

    def test_blocklist_underreped_heck(self, db_session, cluster):
        session = ClusterSession(cluster)
        custom_gucs = {"xen_guard_enabled": "true"}
        with session(gucs=custom_gucs,
                     clean_db_before=True,
                     clean_db_after=True):
            with db_session.cursor() as cursor:
                # create tables and add data
                self.create_and_load_basic_table(cursor)

                # verify that we have user table un-backed-up blocks
                cursor.execute(BAR_TEST_BLOCKLIST_BEFORE)
                res = cursor.fetchall()
                assert res[0][0] == 'pass'

                # do a backup
                BACKUP_ID = 'backup_bar_test2'

                # Initialize the xen guard to pause backup in stage2
                xen_guard_name = (
                    "storage:storage_backup:upload_superblock_and_new_blocks")
                xen_guard = XenGuardControl(
                    host=None,
                    username=None,
                    guardname=xen_guard_name,
                    debug=True,
                    remote=False)

                # pause backup before it uploads the blocks to S3.
                cluster.set_event('EtBackupPauseAtStage2')
                xen_guard.enable()
                create_localhost_snapshot(BACKUP_ID)

                # Wait until backup is blocked on the guard
                log.debug('Waiting for PADB to block on guard {}'.format(
                    xen_guard_name))
                xen_guard.wait_until_process_blocks(timeout_secs=300)

                # reboot cluster
                cluster.reboot_cluster()

            with db_session.cursor() as cursor:
                # check whether backup finishes within the specified time
                wait_for(is_backup_finished,
                         args=[BACKUP_ID],
                         retries=30, delay_seconds=10)

                copy_snapshot_post_backup(BACKUP_ID)

                # check blocklist
                cursor.execute(BAR_TEST_BLOCKLIST_AFTER_1)
                res = cursor.fetchall()
                assert res[0][0] == 'pass one'

                # check blocklist
                cursor.execute(BAR_TEST_BLOCKLIST_AFTER_2)
                res = cursor.fetchall()
                assert res[0][0] == 'pass two'

                # add more data to the table
                cursor.execute("insert into bar_test1 values \
                               (1, 4), (1, 5), (1, 6);")

                # Kill PADB
                stop_sim_db()

                # restore from the backup
                restore_from_localhost_snapshot(BACKUP_ID, do_restore_bootstrap=True)
                wait_for(is_restore_complete, retries=30, delay_seconds=10)
                wait_for(is_rerepp_complete, retries=30, delay_seconds=10)

            with db_session.cursor() as cursor:
                cursor.execute("select * from bar_test1 values")

                # check number of underrepped blocks
                underreapped_blocks = self.num_underrepped_blocks(cluster, cursor)
                assert underreapped_blocks == 0
