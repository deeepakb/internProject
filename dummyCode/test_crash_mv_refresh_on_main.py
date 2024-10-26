# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.burst.burst_write import burst_write_basic_gucs
from raff.burst.burst_write import burst_write_mv_gucs
from raff.burst.burst_test import setup_teardown_burst
from raff.storage.storage_test import disable_all_autoworkers
from test_burst_mv_refresh import TestBurstWriteMVBase
from test_burst_mv_refresh import MV_QUERIES, MY_TABLE
from test_burst_mv_refresh import BURST_OWN
from raff.common.db.redshift_db import RedshiftDb
from raff.storage.alter_table_suite import AlterTableSuite

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst, disable_all_autoworkers]

REFRESH_MV = "refresh materialized view {0}"
MY_SCHEMA = "test_schema"
MY_USER = "test_user"

diststyle = ['distkey(c0)', 'diststyle even']
sortkey = ['sortkey(c0)', '']

crash_events = ['EtCrashCommitBeforeP1', 'EtCrashCommitAfterP1']


class TestBurstWriteCrashMVRefreshBase(TestBurstWriteMVBase, AlterTableSuite):
    def _crash_mv_refresh_on_main(self, cluster, mv_configs, crash_event):
        """
            Runs a given SQL after setting the crash event in vector.
            The crash event dictates whether the crash should happen on main
            cluster or burst cluster.

            Args:
                sql (str): SQL statement
                mv_configs (list):
                    List of MVs to run the SQL on and crash
                cluster (obj):
                    Cluster object
                crash_event (str):
                    Crash point in query code
        """
        session = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))
        crash_cursor = session.cursor()
        # Refresh all MVs in a txn and commit. Commit should crash.
        try:
            crash_cursor.execute("set query_group to metrics")
            cluster.run_xpx("event set {}".format(crash_event))
            crash_cursor.execute("BEGIN")
            for mv in mv_configs.mvs:
                crash = False
                sql = REFRESH_MV.format(mv)
                crash_cursor.execute(sql)
            crash_cursor.execute("COMMIT")
        except Exception as e:
            log.error("Error for MV-refresh crash = {}".format(e))

        conn_params = cluster.get_conn_params()
        control_conn = RedshiftDb(conn_params)
        self.wait_for_crash(conn_params, control_conn)
        cluster.reboot_cluster()
        cluster.wait_for_cluster_available(300)

    def _get_cursors(self, cluster):
        session1 = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))
        session2 = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))
        main_cur1 = session1.cursor()
        main_cur2 = session2.cursor()
        bs_cur = self.db.cursor()
        return [main_cur1, main_cur2, bs_cur]

    def base_test_crash_mv_refresh_on_main(self, cluster):
        relprefix = "crash_mv_refresh_on_main"
        main_cur1, main_cur2, bs_cur = cursors = self._get_cursors(cluster)

        for crash_event in crash_events:
            cluster.run_xpx("event unset {}".format(crash_event))
            self._reset_conn_to_main(cluster)

        with self.setup_views_with_shared_tables(
                cluster, main_cur2, relprefix, diststyle, sortkey, MV_QUERIES,
                False) as mv_configs:
            bs_cur.execute("set search_path to {}".format(MY_SCHEMA))
            shared_base_tables = mv_configs.tables
            # Crash mv_refresh on burst cluster
            for crash_event in crash_events:
                log.info("Testing crash event = {}".format(crash_event))
                main_cur1, main_cur2, bs_cur = cursors = self._get_cursors(
                    cluster)
                # This will force mv-refresh
                self._do_sql(
                    "insert",
                    False,
                    False,
                    shared_base_tables,
                    cluster, [main_cur1],
                    num_val=500)
                self._crash_mv_refresh_on_main(cluster, mv_configs,
                                               crash_event)

            # Cluster restart
            main_cur1, main_cur2, bs_cur = cursors = self._get_cursors(cluster)
            # This takes backup
            self._check_burst_cluster_health(cluster, None, MY_TABLE)
            # This will force mv-refresh
            self._do_sql("insert", True, True, shared_base_tables, cluster,
                         [main_cur1])
            # Refresh all MVs on burst
            self._refresh_all_mv(
                # try_burst
                True,
                # should_burst
                True,
                mv_configs,
                cluster,
                cursors,
                BURST_OWN)


@pytest.mark.cluster_only
@pytest.mark.custom_burst_gucs(
    gucs=dict(list(burst_write_mv_gucs.items()) + [(
        'burst_percent_threshold_to_trigger_backup',
        '100'), ('burst_cumulative_time_since_stale_backup_threshold_s',
                 '86400'), ('enable_burst_async_acquire', 'false'),
        ('burst_commit_refresh_check_frequency_seconds', '-1'),
        ('enable_mddl_conjunct_logging_task', 'false')]))
@pytest.mark.usefixtures("setup_teardown_burst")
class TestBurstWriteCrashMVRefreshCluster(TestBurstWriteCrashMVRefreshBase):
    @pytest.mark.skip(reason="rsqa-14831")
    def test_crash_mv_refresh_on_main_cluster(self, cluster):
        self.base_test_crash_mv_refresh_on_main(cluster)

# This test cannot be run in SS mode.
skip_reason = ("Unsuitable for SS mode since we restart main-padb"
               " and unlike cluster mode, we cannot acquire burst cluster"
               " on the fly in SS mode. There is no method right now to"
               " restart the docker container in the middle of the test")
