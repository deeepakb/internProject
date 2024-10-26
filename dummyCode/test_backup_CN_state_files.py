
import subprocess
import logging

import pytest

from py_lib.common.xen_guard_control import XenGuardControl
from raff.backup_restore.bar_test import BARTestSuite
from raff.common.simulated_helper import XEN_ROOT
from raff.common.cluster.cluster_session import ClusterSession
from raff.common.simulated_helper import (stop_sim_db,
                                          start_sim_db,
                                          create_localhost_snapshot,
                                          expect_backup_fail_with_error_code,
                                          is_backup_finished,
                                          wait_for)

BAR_TEST_BLOCKLIST_BEFORE = '''select
case when count(1)>0
then 'pass'
else 'fail'
end as check_status
from stv_blocklist where tbl > 0 and backed_up = 0;'''


CHECK_BACKUP = '''select *
from stl_backup_leader
where backup_id = {}
and in_progress = 0
and tlr_metadata_successful = 1;"'''

log = logging.getLogger(__name__)


@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.session_ctx(user_type='bootstrap')
class TestCNStateCheck(BARTestSuite):
    def test_backuo_CN_state_files(self, db_session, cluster):
        '''
        This test does the following steps in sequence
        1. create a backup
        2. stop padb
        3. modify state.txt on each CN
        4. restart padb
        5. check whether backup finihsed
        6. check whether backup has BackupInvalidVersion error

        Args:
            cluster (RedshiftCluster): The Redshift cluster instance.
            db_cursor: A database cursor object for executing queries.
        '''
        custom_gucs = {"xen_guard_enabled": "true"}
        session = ClusterSession(cluster)
        with session(gucs=custom_gucs,
                     clean_db_before=True,
                     clean_db_after=True):
            with db_session.cursor() as cursor:
                # create and insert data into the table
                subprocess.run(["python",
                                "{}/test/leader_durability/populate_bar_test_table.py"
                                .format(XEN_ROOT),
                                "1000"])

                # verify that we have user table un-backed-up blocks
                cursor.execute(BAR_TEST_BLOCKLIST_BEFORE)
                res = cursor.fetchall()
                assert res[0][0] == 'pass'

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

                # create a backup
                BACKUP_ID = 'backup_bar_test_state'
                create_localhost_snapshot(BACKUP_ID,  wait=False)

                # Wait until backup is blocked on the guard
                log.debug('Waiting for PADB to block on guard {}'.format(
                    xen_guard_name))
                xen_guard.wait_until_process_blocks(timeout_secs=300)

                # Kill PADB
                stop_sim_db()

                file_path_node0 = "{}/data/backup0/state.txt".format(XEN_ROOT)
                file_path_node1 = "{}/data/backup1/state.txt".format(XEN_ROOT)
                file_path_node2 = "{}/data/backup2/state.txt".format(XEN_ROOT)

                # modify node0 state file as empty
                with open(file_path_node0, 'w'):
                    pass

                # modify node1 state file to contain a blank line
                with open(file_path_node1, 'w') as file:
                    file.write('\n')

                # remove last 2 characters from node2 state file
                with open(file_path_node2, 'r') as file:
                    content = file.read()
                modified_content = content[:-2]
                with open(file_path_node2, 'w') as file:
                    file.write(modified_content)

                # restart PADB
                start_sim_db()

            # check whether backup finished
            wait_for(is_backup_finished,
                     args=[BACKUP_ID],
                     retries=30, delay_seconds=10)

            # check backup fail error code
            expect_backup_fail_with_error_code(BACKUP_ID, 1066)
