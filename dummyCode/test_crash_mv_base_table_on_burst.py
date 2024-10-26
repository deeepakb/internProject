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

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst, disable_all_autoworkers]

INSERT = "insert into {0} values {1}"
MY_SCHEMA = "test_schema"
MY_USER = "test_user"

diststyle = ['distkey(c0)', 'diststyle even']
sortkey = ['sortkey(c0)', '']

crash_events = [
    'EtFakeBurstErrorBeforeStreamHeader',
    'EtFakeBurstErrorAfterStreamHeader',
    'EtFakeBurstErrorStepDelete',
    'EtFakeBurstErrorStepInsert',
]
dimensions = Dimensions(dict(sql=['insert', 'update', 'delete', 'copy']))
skip_dimensions = {
    ('insert', 'EtFakeBurstErrorStepDelete'),
    ('delete', 'EtFakeBurstErrorStepInsert'),
    ('copy', 'EtFakeBurstErrorStepDelete'),
}


class TestBurstWriteCrashMVBaseTableBase(TestBurstWriteMVBase):
    def _crash_sql_on_burst(self, cluster, sql, table, crash_event):
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
        crash_cursor.execute("set query_group to burst")
        crash = False
        try:
            self.run_directly_on_burst(
                cluster, ("xpx 'event set {}'").format(crash_event),
                new_conn=True)
            self._do_sql(sql, True, True, [table], cluster, [crash_cursor])
        except Exception as e:
            "Query crashed as expected, query = {}".format(sql)
            log.info(e)
            crash = "Simulate" in str(e)

        assert crash, "Simulated crash expected, query = {}".format(sql)

        try:
            self.run_directly_on_burst(
                cluster, ("xpx 'event unset {}'").format(crash_event),
                new_conn=True)
            self._reset_conn_to_main(cluster)
        except Exception as e:
            log.error(e)
            raise e

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

    def _should_skip(self, sql, crash_event):
        return (sql, crash_event) in skip_dimensions

    def base_test_crash_mv_base_table_on_burst(self, cluster, vector):
        relprefix = "crash_mv_base_table_on_burst"
        main_cur1, main_cur2, bs_cur = cursors = self._get_cursors(cluster)

        # Unset any crash events on the burst cluster from previous runs
        # of this test. And then reset the test ssh connection to main cluster.
        for crash_event in crash_events:
            self.run_directly_on_burst(
                cluster, ("xpx 'event unset {}'").format(crash_event),
                new_conn=True)
            self._reset_conn_to_main(cluster)

        with self.setup_views_with_shared_tables(
                cluster, main_cur2, relprefix, diststyle, sortkey, MV_QUERIES,
                False) as mv_configs:
            bs_cur.execute("set search_path to {}".format(MY_SCHEMA))
            shared_base_tables = mv_configs.tables

            for crash_event in crash_events:
                if self._should_skip(vector.sql, crash_event):
                    continue
                log.info("Testing {} & {}".format(vector.sql, crash_event))
                # Pick a random base table
                base_table = random.choice(shared_base_tables)
                # This will force mv-refresh
                self._do_sql(
                    "insert",
                    True,
                    True, [base_table],
                    cluster, [main_cur1],
                    num_val=500)
                log.info("Crashing {} on {} on burst".format(
                    vector.sql, base_table))
                self._crash_sql_on_burst(cluster, vector.sql, base_table,
                                         crash_event)
                self._refresh_all_mv(
                    # try_burst
                    True,
                    # should_burst
                    False,
                    mv_configs,
                    cluster,
                    cursors)
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


@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_no_stable_rc
@pytest.mark.custom_burst_gucs(
    gucs=list(burst_write_basic_gucs.items()) + [('slices_per_node', '3')])
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteCrashMVBaseTableSS(TestBurstWriteCrashMVBaseTableBase):
    @classmethod
    def modify_test_dimensions(cls):
        return dimensions

    def test_crash_mv_base_table_on_burst_ss(self, cluster, vector):
        self.base_test_crash_mv_base_table_on_burst(cluster, vector)
