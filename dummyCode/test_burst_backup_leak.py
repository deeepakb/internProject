# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import logging
import time

from raff.burst.burst_test import BurstTest
from raff.common.ssh_user import SSHUser
from raff.util.utils import get_real_pid

log = logging.getLogger(__name__)


@pytest.mark.cluster_only
@pytest.mark.serial_only
@pytest.mark.session_ctx(user_type='super')
@pytest.mark.no_jdbc  # https://issues.amazon.com/RedshiftDP-21817
# Unfit for burst nightly because the EVR workflow will modify the gucs.
@pytest.mark.no_burst_nightly
class TestBurstBackupLeak(BurstTest):
    """
    Test to check that no connections are leaked after taking a backup.
    Marked as cluster only because we need to enable EVR.
    """

    def get_open_files_for_process(self, cluster, pid):
        pid = get_real_pid(cluster, self.db.cursor(), pid)

        sshc = cluster.get_leader_ssh_conn()
        _, stdout, _ = sshc.run_remote_cmd(
            "lsof -p {}".format(pid),
            user=SSHUser.RDSDB)

        if stdout != "":
            log.info("Lsof output: {}".format(stdout))
            return len(stdout.split('\n'))

    def run_double_xpx(self, cluster_session, db_session, cluster):
        # EVR works only on clusters and we need to trigger the xpx multiple
        # times from the same session. Given that bootstrap queries on
        # clusters run each one in their own session, we need to allow
        # superusers to run xpx too such that multiple xpx can be run in the
        # same session.
        with cluster_session(gucs={"enable_non_bootstrap_xpx": "true"}), \
            db_session.cursor() as cursor:
            cursor.execute("select pg_backend_pid()")
            pid = cursor.fetch_scalar()
            log.info("Session PID: {}".format(pid))
            open_files_b = self.get_open_files_for_process(cluster, pid)
            log.info("Open files before xpx: {}".format(open_files_b))

            # Run 2 xpx (one will trigger the backup, the other will fail
            # because of a backup in progress)
            cursor.execute("xpx 'burst_backup'")
            cursor.execute("xpx 'burst_backup'")

            timeout_s = 60
            timeout = time.time() + timeout_s
            while time.time() <= timeout:
                open_files_a = self.get_open_files_for_process(cluster, pid)
                log.info("Open files after xpx: {}".format(open_files_a))
                if open_files_b == open_files_a:
                    break
            assert open_files_b == open_files_a, (
                "Connections not closed after {} sec".format(timeout_s))

    def test_burst_backup_leak(self, cluster, cluster_session, db_session):
        self.run_double_xpx(cluster_session, db_session, cluster)

        with self.enable_disable_evr(cluster):
            self.run_double_xpx(cluster_session, db_session, cluster)
