# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
import logging
import random
import pytest

from raff.common.dimensions import Dimensions
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

INSERT = "insert into {0} values {1}"
MY_SCHEMA = "test_schema"
MY_USER = "test_user"

diststyle = ['distkey(c0)', 'diststyle even']
sortkey = ['sortkey(c0)', '']

crash_events = ['EtCrashCommitBeforeP1', 'EtCrashCommitAfterP1']
dimensions = Dimensions(dict(sql=['insert', 'delete']))


class TestBurstWriteCrashMVBaseTableBase(TestBurstWriteMVBase,
                                         AlterTableSuite):
    def _crash_sql_on_main(self, cluster, sql, table, crash_event):
        """
            Runs a given SQL after setting the crash event in vector.
            The crash event dictates whether the crash should happen on main
            cluster or burst cluster.

            Args:
                sql (str): SQL statement
                table (str):
                    Table to run the SQL on and crash
                cluster (obj):
                    Cluster object
                crash_event (str):
                    Crash point in query code
        """
        # Make a new cursor for crashes. The cursor could be closed after a
        # crash and then its useless
        session = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))
        crash_cursor = session.cursor()
        crash = False
        try:
            cluster.run_xpx("event set {}".format(crash_event))
            self._do_sql(sql, False, False, [table], cluster, [crash_cursor])
        except Exception as e:
            "Query crashed as expected, query = {}".format(sql)
            log.error("Error msg for simulated crash = {}".format(e))
            crash = "non-std exception" in str(e)

        assert crash, "Simulated crash expected, query = {}".format(sql)

        conn_params = cluster.get_conn_params()
        control_conn = RedshiftDb(conn_params)
        self.wait_for_crash(conn_params, control_conn)
        # Poll cluster status to make sure the cluster is recovered. Ten minutes
        # of wait time should be sufficient.
        cluster.wait_for_padb_up(
            timeout_sec=600, retry_interval_sec=30, monitor_logs=False)

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

    def base_test_crash_mv_base_table_on_main(self, cluster, vector):
        relprefix = "crash_mv_base_table_on_main"
        main_cur1, main_cur2, bs_cur = cursors = self._get_cursors(cluster)

        # Unset any crash events on the burst cluster from previous runs
        # of this test. And then reset the test ssh connection to main cluster.
        for crash_event in crash_events:
            try:
                cluster.run_xpx("event unset {}".format(crash_event))
                self._reset_conn_to_main(cluster)
            except Exception as e:
                log.error(e)
                raise e

        with self.setup_views_with_shared_tables(
                cluster, main_cur2, relprefix, diststyle, sortkey, MV_QUERIES,
                False) as mv_configs:
            bs_cur.execute("set search_path to {}".format(MY_SCHEMA))
            shared_base_tables = mv_configs.tables

            for crash_event in crash_events:
                log.info("Testing {} & {}".format(vector.sql, crash_event))
                # Pick a random base table
                base_table = random.choice(shared_base_tables)
                log.info("Crashing {} on {} on main".format(
                    vector.sql, base_table))
                self._crash_sql_on_main(cluster, vector.sql, base_table,
                                        crash_event)
            # Cluster restart
            main_cur1, main_cur2, bs_cur = cursors = self._get_cursors(cluster)
            # This takes backup
            self._check_burst_cluster_health(cluster, None, MY_TABLE)
            # This will force mv-refresh
            self._do_sql("insert", True, True, shared_base_tables, cluster,
                         [main_cur1])
            # Refresh all MVs
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
        ('burst_commit_refresh_check_frequency_seconds', '-1')]))
@pytest.mark.usefixtures("setup_teardown_burst")
class TestBurstWriteCrashMVBaseTableCluster(
        TestBurstWriteCrashMVBaseTableBase):
    @classmethod
    def modify_test_dimensions(cls):
        return dimensions

    def test_crash_mv_base_table_on_main_cluster(self, cluster, vector):
        self.base_test_crash_mv_base_table_on_main(cluster, vector)


# This test cannot be run in SS mode.
skip_reason = ("Unsuitable for SS mode since we restart main-padb"
               " and unlike cluster mode, we cannot acquire burst cluster"
               " on the fly in SS mode. There is no method right now to"
               " restart the docker container in the middle of the test")
