# Copyright 2022 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import getpass
import logging
import uuid
import pytest

from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.simulated_helper import create_localhost_snapshot
from raff.common.dimensions import Dimensions
from raff.storage.storage_test import create_thread

__all__ = [super_simulated_mode]
log = logging.getLogger(__name__)


SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
CREATE_TABLE = "CREATE TABLE {} (col1 int) diststyle even"
INSERT = "INSERT INTO {} VALUES {}"
SELECT = "SELECT * FROM {}"
MY_TABLE = "test_table"


@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_burst_gucs(gucs={
        'enable_burst_s3_commit_based_refresh': 'true',
        'xen_guard_enabled': 'true'
})
@pytest.mark.custom_local_gucs(gucs={
        'enable_burst_s3_commit_based_refresh': 'true',
        'xen_guard_enabled': 'true'
})
class TestCommitBasedRefreshPhaseOne(BurstWriteTest):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                guard_pos=[
                    "storage:fdisk:process_message:commit_p1",
                    "storage:fdisk:process_message:commit_p2"
                ]))

    def _setup_table(self, cursor):
        cursor.execute(CREATE_TABLE.format(MY_TABLE))
        cursor.execute(INSERT.format(MY_TABLE, "(0)"))

    def _commit_thread(self, cluster):
        cluster.run_xpx("hello")

    def test_refresh_commit_phase_one(self, cluster, vector):
        """
        This test pauses commit before and after commit phase one and verifies
        the behavior of commit based refresh. When pausing before phase one
        of commit we expect burst not to refresh to the sb version of the
        commit. When pausing after phase one we expect burst will refresh to
        the sb version of the commit.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        snapshot_id = "burst-write-snapshot-{}".format(str(uuid.uuid4().hex))
        create_localhost_snapshot(snapshot_id, wait=True)
        log.info("Setting event for guard")
        # set event so the guard positions are hit in fdisk
        cluster.set_event('EtSimXRestoreFailureXenGuard')
        with db_session.cursor() as cursor, \
                self.db.cursor() as bootstrap_cursor:
            cursor.execute("set query_group to burst")
            self._setup_table(cursor)
            xen_guard = self._create_xen_guard(vector.guard_pos)
            with create_thread(self._commit_thread, (cluster, )) as thread, \
                    xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks(timeout_secs=180)
                cursor.execute(SELECT.format(MY_TABLE))
                self._start_and_wait_for_refresh(
                    cluster, bootstrap_cursor, no_commit=True)
                bootstrap_cursor.execute(
                    "select MAX(refresh_version) from stl_burst_manager_refresh")
                refresh_version = bootstrap_cursor.fetch_scalar()
            bootstrap_cursor.execute(
                "select distinct sb_version from stv_superblock")
            sb_version = bootstrap_cursor.fetch_scalar()
            # if pausing before phase one we expect refresh to be based
            # on the previous commit
            if vector.guard_pos == "storage:fdisk:process_message:commit_p1":
                assert refresh_version == sb_version - 1
            else:
                assert refresh_version == sb_version
