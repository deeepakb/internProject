# Copyright 2022 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import getpass
import logging
import uuid
import pytest

from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode,\
    prepare_burst
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.common.dimensions import Dimensions
from raff.storage.alter_table_suite import AlterTableSuite
from raff.common.result import SelectResult
from raff.common.simulated_helper import create_localhost_snapshot

__all__ = [super_simulated_mode]
log = logging.getLogger(__name__)

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
CREATE_TABLE = "CREATE TABLE {} (col1 int) diststyle even"
INSERT = "INSERT INTO {} VALUES {}"
SELECT = "SELECT * FROM {}"
MY_USER = "test_user"
MY_TABLE = "test_table"
BACKUP_IN_PROGRESS = "backup already in progress"


@pytest.mark.serial_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.encrypted_only
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
class TestBurstCommitRefreshCrash(BurstWriteTest, AlterTableSuite):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                crash_pos=[
                    'EtCrashCommitBeforeP1',
                    'EtCrashCommitAfterP1',
                    'EtCrashCommitAfterP2'
                ]))

    def _setup_table(self, cursor):
        cursor.execute(CREATE_TABLE.format(MY_TABLE))
        cursor.execute(INSERT.format(MY_TABLE, "(0)"))

    def _get_cluster_start_time(self, cluster):
        conn_params = cluster.get_conn_params()
        session_ctx = SessionContext(user_type='bootstrap')
        with DbSession(conn_params, session_ctx=session_ctx) as db_session:
            with db_session.cursor() as cursor:
                cursor.execute("select pg_postmaster_start_time()")
                return cursor.fetch_scalar()

    def test_burst_refresh_commit_crash(self, cluster, vector):
        db_session = DbSession(cluster.get_conn_params(user='master'))
        start_time = self._get_cluster_start_time(cluster)
        snapshot_id = "burst-write-snapshot-{}".format(str(uuid.uuid4().hex))
        create_localhost_snapshot(snapshot_id, wait=True)
        with db_session.cursor() as cursor:

            cursor.execute("set query_group to burst")
            self._setup_table(cursor)
            cursor.execute(SELECT.format(MY_TABLE))
            try:
                cluster.set_event(vector.crash_pos)
                cursor.execute(INSERT.format(MY_TABLE, "(1)"))
            except Exception as e:
                log.info("error message: {}".format(str(e)))
                assert "non-std exception" in str(e)

            conn_params = cluster.get_conn_params()
            self.wait_for_crash(conn_params, start_time)
            cluster.reboot_cluster()

        cluster.wait_for_cluster_available(180)
        log.info("Cluster has rebooted")
        prepare_burst({'enable_burst_s3_commit_based_refresh': 'true'})
        log.info("Restarted super simulated Burst cluster is ready.")
        with db_session.cursor() as cursor:
            self._start_and_wait_for_refresh(cluster)
            log.info("Burst cluster has been refreshed")
            cursor.execute("set query_group to burst")
            cursor.execute(SELECT.format(MY_TABLE))
            res = cursor.result_fetchall()
            self._check_last_query_bursted(cluster, cursor)
            # We don't expect any change to the committed
            # superblock on S3 if the crash occurs before
            # phase one of commit.
            if vector.crash_pos == 'EtCrashCommitBeforeP1':
                assert res == SelectResult(
                    rows=[(0,)],
                    column_types=['INT'])
            else:
                assert res == SelectResult(
                    rows=[(0,), (1,)],
                    column_types=['INT'])
