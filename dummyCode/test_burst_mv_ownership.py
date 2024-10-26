# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
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
from test_burst_mv_refresh import MV_QUERIES, MAIN_UNDO, BURST_OWN
from test_burst_mv_refresh import MAIN_OWN, CLEAN, BURST_DIRTY

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst, disable_all_autoworkers]

IDCU = "insert delete copy update"
MY_SCHEMA = "test_schema"
MY_USER = "test_user"

diststyle = ['diststyle even', 'distkey(c0)']
sortkey = ['sortkey(c0)', '']


class TestBurstWriteMVOwnershipBase(TestBurstWriteMVBase):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(dict(test_case=[1, 2, 3, 4, 5]))

    def _read_all_mv(self, try_burst, should_burst, mv_configs, cluster,
                     cursors):
        """
            Issues a select cmd on an MV and checks if it bursts or not.
        """
        self._do_sql("select", try_burst, should_burst, mv_configs.mvs,
                     cluster, cursors)

    def _check_state(self, table_state, mv_state, mv_configs, cluster):
        """
            Checks if base tables of MV and the MV are in a given state.
            Fails the test if they are not in the given state.
        """
        base_tables = mv_configs.tables[:1]
        for tid, table in enumerate(base_tables):
            self._check_ownership(MY_SCHEMA, table, table_state, cluster)
            self._check_table(cluster, MY_SCHEMA, table,
                              mv_configs.t_dists[tid])
        for mvid, mv in enumerate(mv_configs.mvs):
            mv_internal_table = "mv_tbl__{}__0".format(mv)
            self._check_ownership(MY_SCHEMA, mv_internal_table, mv_state,
                                  cluster)
            self._check_table(cluster, MY_SCHEMA, mv_internal_table,
                              mv_configs.mv_dists[mvid])

    def _test_case_1(self, cluster, cursors, mv_configs):
        """
            This test takes the base tables and MV through these states
            <table state, mv-internal-table state>

            # a
                Begin
                Clean, clean
                burst-dml on base-tables
                Burst-own, clean
                Commit
                Burst-own, clean
                refresh mv on burst
                Burst-own, burst-own

            # b
                Begin
                Burst-own, burst-own
                dml on main on base-tables
                Main-own, burst-own
                Commit
                Clean, burst-own
                refresh mv on main
                Clean, clean

            # c
                begin
                clean, clean
                dml on main on base-tables
                main-own, clean
                abort
                main-undo, clean
                refresh mv on main
                main-undo, clean

            # d
                begin
                main undo, clean
                dml on main on base-tables
                main-own + main-undo, clean
                abort
                main-undo, clean
                refresh mv on main
                main-undo, clean
        """

        main_cur1, check_cur, bs_cur = cursors
        base_tables = mv_configs.tables[:1]

        # a
        main_cur1.execute('begin')
        self._read_all_mv(True, True, mv_configs, cluster, [main_cur1])
        self._check_state(CLEAN, CLEAN, mv_configs, cluster)
        self._do_sql(IDCU, True, True, base_tables, cluster, [main_cur1])
        self._check_state(BURST_OWN, CLEAN, mv_configs, cluster)
        main_cur1.execute('commit')
        self._check_state(BURST_OWN, CLEAN, mv_configs, cluster)
        # The base-tables of MV are BURST_OWN, so mv-refresh can burst
        self._refresh_all_mv(
            True, True, mv_configs, cluster, cursors, is_stale=True)
        self._check_state(BURST_OWN, BURST_OWN, mv_configs, cluster)

        # b
        main_cur1.execute('begin')
        self._read_all_mv(True, True, mv_configs, cluster, [main_cur1])
        self._check_state(BURST_OWN, BURST_OWN, mv_configs, cluster)
        self._do_sql(IDCU, False, False, base_tables, cluster, [main_cur1])
        self._check_state(MAIN_OWN, BURST_OWN, mv_configs, cluster)
        self._read_all_mv(True, True, mv_configs, cluster, [main_cur1])
        self._check_state(MAIN_OWN, BURST_OWN, mv_configs, cluster)
        main_cur1.execute('commit')
        self._check_state(CLEAN, BURST_OWN, mv_configs, cluster)
        # The base-tables of MV are MAIN_OWN, so mv-refresh cannot burst
        self._refresh_all_mv(
            True, False, mv_configs, cluster, cursors, is_stale=True)
        self._check_state(CLEAN, CLEAN, mv_configs, cluster)

        # c
        main_cur1.execute('begin')
        # SELECT from MV cannot burst as burst cluster lost ownership of MV
        self._read_all_mv(True, False, mv_configs, cluster, [main_cur1])
        self._check_state(CLEAN, CLEAN, mv_configs, cluster)
        self._do_sql(IDCU, True, False, base_tables, cluster, [main_cur1])
        self._check_state(MAIN_OWN, CLEAN, mv_configs, cluster)
        self._read_all_mv(True, False, mv_configs, cluster, [main_cur1])
        self._check_state(MAIN_OWN, CLEAN, mv_configs, cluster)
        main_cur1.execute('abort')
        self._check_state(MAIN_UNDO, CLEAN, mv_configs, cluster)
        # The base-tables of MV are MAIN_UNDO, so mv-refresh cannot burst
        # This mv-refresh is no-op since DML on base-tables aborted
        self._refresh_all_mv(
            True, False, mv_configs, cluster, cursors, is_stale=False)
        self._check_state(MAIN_UNDO, CLEAN, mv_configs, cluster)

        # d
        main_cur1.execute('begin')
        # SELECT from MV cannot burst as burst cluster lost ownership of MV
        self._read_all_mv(True, False, mv_configs, cluster, [main_cur1])
        self._check_state(MAIN_UNDO, CLEAN, mv_configs, cluster)
        self._do_sql(IDCU, True, False, base_tables, cluster, [main_cur1])
        self._check_state(MAIN_OWN + MAIN_UNDO, CLEAN, mv_configs, cluster)
        self._read_all_mv(True, False, mv_configs, cluster, [main_cur1])
        self._check_state(MAIN_OWN + MAIN_UNDO, CLEAN, mv_configs, cluster)
        main_cur1.execute('abort')
        self._check_state(MAIN_UNDO, CLEAN, mv_configs, cluster)
        # The base-tables of MV are MAIN_UNDO, so mv-refresh cannot burst
        # This mv-refresh is no-op since DML on base-tables aborted
        self._refresh_all_mv(
            True, False, mv_configs, cluster, cursors, is_stale=False)
        self._check_state(MAIN_UNDO, CLEAN, mv_configs, cluster)

    def _test_case_2(self, cluster, cursors, mv_configs):
        """
            This test takes the base tables and MV through these states
            <table state, mv-internal-table state>

            # a
                begin
                clean, clean
                burst-dml on base-tables
                burst-own, clean
                commit
                burst-own, clean
                refresh mv on burst
                burst-own, burst-own

            # b
                begin
                burst-own, burst-own
                dml on main on base-tables
                burst-own, clean
                main-own, burst-own
                abort
                main-undo, burst-own
                refresh mv on main
                main-undo, burst-own

            # c
                begin
                main-undo, burst-own
                dml on main on base-tables
                main-own + main-undo, burst-own
                commit
                main-undo, burst-own
                refresh mv on main
                main-undo, clean
        """
        main_cur1, check_cur, bs_cur = cursors
        base_tables = mv_configs.tables[:1]

        # a
        main_cur1.execute('begin')
        self._read_all_mv(True, True, mv_configs, cluster, [main_cur1])
        self._check_state(CLEAN, CLEAN, mv_configs, cluster)
        self._do_sql(IDCU, True, True, base_tables, cluster, [main_cur1])
        self._check_state(BURST_OWN, CLEAN, mv_configs, cluster)
        self._read_all_mv(True, True, mv_configs, cluster, [main_cur1])
        self._check_state(BURST_OWN, CLEAN, mv_configs, cluster)
        main_cur1.execute('commit')
        self._check_state(BURST_OWN, CLEAN, mv_configs, cluster)
        # The base-tables of MV are BURST_OWN, so mv-refresh can burst
        self._refresh_all_mv(
            True, True, mv_configs, cluster, cursors, is_stale=True)
        self._check_state(BURST_OWN, BURST_OWN, mv_configs, cluster)

        # b
        main_cur1.execute('begin')
        self._read_all_mv(True, True, mv_configs, cluster, [main_cur1])
        self._check_state(BURST_OWN, BURST_OWN, mv_configs, cluster)
        self._do_sql(IDCU, False, False, base_tables, cluster, [main_cur1])
        self._check_state(MAIN_OWN, BURST_OWN, mv_configs, cluster)
        self._read_all_mv(True, True, mv_configs, cluster, [main_cur1])
        self._check_state(MAIN_OWN, BURST_OWN, mv_configs, cluster)
        main_cur1.execute('abort')
        self._check_state(MAIN_UNDO, BURST_OWN, mv_configs, cluster)
        # The base-tables of MV are MAIN_UNDO, so mv-refresh cannot burst
        # This mv-refresh is no-op since DML on base-tables aborted
        self._refresh_all_mv(
            True, False, mv_configs, cluster, cursors, is_stale=False)
        self._check_state(MAIN_UNDO, BURST_OWN, mv_configs, cluster)

        # c
        main_cur1.execute('begin')
        self._read_all_mv(True, True, mv_configs, cluster, [main_cur1])
        self._check_state(MAIN_UNDO, BURST_OWN, mv_configs, cluster)
        self._do_sql(IDCU, True, False, base_tables, cluster, [main_cur1])
        self._check_state(MAIN_OWN + MAIN_UNDO, BURST_OWN, mv_configs, cluster)
        self._read_all_mv(True, True, mv_configs, cluster, [main_cur1])
        self._check_state(MAIN_OWN + MAIN_UNDO, BURST_OWN, mv_configs, cluster)
        main_cur1.execute('commit')
        self._check_state(MAIN_UNDO, BURST_OWN, mv_configs, cluster)
        # The base-tables of MV are MAIN_UNDO, so mv-refresh cannot burst
        self._refresh_all_mv(
            True, False, mv_configs, cluster, cursors, is_stale=True)
        self._check_state(MAIN_UNDO, CLEAN, mv_configs, cluster)

    def _test_case_3(self, cluster, cursors, mv_configs):
        """
            This test takes the base tables and MV through these states
            <table state, mv-internal-table state>

            # a
                begin
                clean, clean
                burst-dml on base-tables
                burst-own, clean
                commit
                burst-own, clean
                refresh mv on burst
                burst-own, burst-own

            # b
                begin
                burst-own, burst-own
                burst-dml on base-tables
                burst-own, burst-own
                commit
                burst-own, burst-own
                refresh mv on main
                burst-own, clean
                burst-dml on base-tables
                refresh mv on main
                burst-own, clean

            # c
                begin
                burst-own, clean
                dml on main on base-tables
                main-own, clean
                commit
                clean, clean
                refresh mv on main
                clean, clean
        """
        main_cur1, check_cur, bs_cur = cursors
        base_tables = mv_configs.tables[:1]

        # a
        main_cur1.execute('begin')
        self._read_all_mv(True, True, mv_configs, cluster, [main_cur1])
        self._check_state(CLEAN, CLEAN, mv_configs, cluster)
        self._do_sql(IDCU, True, True, base_tables, cluster, [main_cur1])
        self._check_state(BURST_OWN, CLEAN, mv_configs, cluster)
        self._read_all_mv(True, True, mv_configs, cluster, [main_cur1])
        self._check_state(BURST_OWN, CLEAN, mv_configs, cluster)
        main_cur1.execute('commit')
        self._check_state(BURST_OWN, CLEAN, mv_configs, cluster)
        # The base-tables of MV are BURST_OWN, so mv-refresh can burst
        self._refresh_all_mv(
            True, True, mv_configs, cluster, cursors, is_stale=True)
        self._check_state(BURST_OWN, BURST_OWN, mv_configs, cluster)

        # b
        main_cur1.execute('begin')
        self._read_all_mv(True, True, mv_configs, cluster, [main_cur1])
        self._check_state(BURST_OWN, BURST_OWN, mv_configs, cluster)
        self._do_sql(IDCU, True, True, base_tables, cluster, [main_cur1])
        self._check_state(BURST_OWN, BURST_OWN, mv_configs, cluster)
        self._read_all_mv(True, True, mv_configs, cluster, [main_cur1])
        self._check_state(BURST_OWN, BURST_OWN, mv_configs, cluster)
        main_cur1.execute('commit')
        self._check_state(BURST_OWN, BURST_OWN, mv_configs, cluster)
        # mv-refresh on main
        self._refresh_all_mv(
            False, False, mv_configs, cluster, cursors, is_stale=True)
        self._check_state(BURST_OWN, CLEAN, mv_configs, cluster)
        self._do_sql(IDCU, True, True, base_tables, cluster, [main_cur1])
        # mv-refresh cannot burst since previous mv-refresh was on main and the
        # burst cluster lost ownership
        self._refresh_all_mv(
            True, False, mv_configs, cluster, cursors, is_stale=True)
        self._check_state(BURST_OWN, CLEAN, mv_configs, cluster)

        # c
        main_cur1.execute('begin')
        # SELECT from MV cannot burst as burst cluster lost ownership of MV
        self._read_all_mv(True, False, mv_configs, cluster, [main_cur1])
        self._check_state(BURST_OWN, CLEAN, mv_configs, cluster)
        self._do_sql(IDCU, False, False, base_tables, cluster, [main_cur1])
        self._check_state(MAIN_OWN, CLEAN, mv_configs, cluster)
        self._read_all_mv(True, False, mv_configs, cluster, [main_cur1])
        self._check_state(MAIN_OWN, CLEAN, mv_configs, cluster)
        main_cur1.execute('commit')
        self._check_state(CLEAN, CLEAN, mv_configs, cluster)
        # mv-refresh cannot burst as burst-cluster lost ownership of both MV &
        # its base tables
        self._refresh_all_mv(
            True, False, mv_configs, cluster, cursors, is_stale=True)
        self._check_state(CLEAN, CLEAN, mv_configs, cluster)

    def _test_case_4(self, cluster, cursors, mv_configs):
        """
            This test takes the base tables and MV through these states
            <table state, mv-internal-table state>

            # a
                Begin
                burst-dml on base-tables
                Burst-own, clean
                Commit
                Burst-own, clean
                refresh mv on burst
                Burst-own, burst-own

            # b
                Begin
                Burst-own, burst-own
                dml on main on base-tables
                Burst-dirty + main-own, clean
                commit
                Burst-dirty, burst-own
                refresh mv on main
                Burst-dirty, clean

            # c
                Begin
                Burst-dirty, clean
                dml on main on base-tables
                Burst-dirty + main-own, clean
                abort
                main-undo, clean
                refresh mv on main
                main-undo, clean
        """
        main_cur1, check_cur, bs_cur = cursors
        base_tables = mv_configs.tables[:1]

        # a
        main_cur1.execute('begin')
        self._read_all_mv(True, True, mv_configs, cluster, [main_cur1])
        self._check_state(CLEAN, CLEAN, mv_configs, cluster)
        self._do_sql(IDCU, True, True, base_tables, cluster, [main_cur1])
        self._check_state(BURST_OWN, CLEAN, mv_configs, cluster)
        self._read_all_mv(True, True, mv_configs, cluster, [main_cur1])
        self._check_state(BURST_OWN, CLEAN, mv_configs, cluster)
        main_cur1.execute('commit')
        self._check_state(BURST_OWN, CLEAN, mv_configs, cluster)
        # The base-tables of MV are BURST_OWN, so mv-refresh can burst
        self._refresh_all_mv(
            True, True, mv_configs, cluster, cursors, is_stale=True)
        self._check_state(BURST_OWN, BURST_OWN, mv_configs, cluster)

        # b
        main_cur1.execute('begin')
        self._read_all_mv(True, True, mv_configs, cluster, [main_cur1])
        self._check_state(BURST_OWN, BURST_OWN, mv_configs, cluster)
        self._do_sql(IDCU, True, True, base_tables, cluster, [main_cur1])
        self._check_state(BURST_OWN, BURST_OWN, mv_configs, cluster)
        self._do_sql(IDCU, False, False, base_tables, cluster, [main_cur1])
        self._check_state(BURST_DIRTY + MAIN_OWN, BURST_OWN, mv_configs,
                          cluster)
        self._read_all_mv(True, True, mv_configs, cluster, [main_cur1])
        self._check_state(BURST_DIRTY + MAIN_OWN, BURST_OWN, mv_configs,
                          cluster)
        main_cur1.execute('commit')
        self._check_state(BURST_DIRTY, BURST_OWN, mv_configs, cluster)
        # mv-refresh cannot burst as base-tables are BURST_DIRTY
        self._refresh_all_mv(
            True, False, mv_configs, cluster, cursors, is_stale=True)
        self._check_state(BURST_DIRTY, CLEAN, mv_configs, cluster)

        # c
        main_cur1.execute('begin')
        # SELECT from MV cannot burst as burst cluster lost ownership of MV due
        # to previous MV-refresh on main
        self._read_all_mv(True, False, mv_configs, cluster, [main_cur1])
        self._check_state(BURST_DIRTY, CLEAN, mv_configs, cluster)
        self._do_sql(IDCU, True, False, base_tables, cluster, [main_cur1])
        self._check_state(BURST_DIRTY + MAIN_OWN, CLEAN, mv_configs, cluster)
        self._read_all_mv(True, False, mv_configs, cluster, [main_cur1])
        self._check_state(BURST_DIRTY + MAIN_OWN, CLEAN, mv_configs, cluster)
        main_cur1.execute('abort')
        self._check_state(MAIN_UNDO, CLEAN, mv_configs, cluster)
        # MV-refresh cannot burst since base tables are MAIN_UNDO and burst
        # cluster lost ownership of MV
        # This mv-refresh is no-op since DML on base-tables aborted
        self._refresh_all_mv(
            True, False, mv_configs, cluster, cursors, is_stale=False)
        self._check_state(MAIN_UNDO, CLEAN, mv_configs, cluster)

    def _test_case_5(self, cluster, cursors, mv_configs):
        """
            This test takes the base tables and MV through these states
            <table state, mv-internal-table state>

            # a
                Begin
                Clean, clean
                burst-dml on base tables
                Burst-own, clean
                refresh mv on burst - no-op
                Burst-own, clean
                Commit
                Burst-own, clean
                refresh mv on burst
                Burst-own, burst-own

            # b
                Begin
                Burst-own, burst-own
                burst-dml on base tables
                refresh mv on burst - no-op
                Abort
                main-undo, burst-own
                refresh mv on burst - no-op
                main-undo, burst-own

            # c
                Begin
                main-undo, burst-own
                dml on base-tables on main
                main-own + main-undo, burst-own
                refresh mv on burst - no-op
                Abort
                main-undo, burst-own
                refresh mv on burst - no-op
                main-undo, burst-own

        """
        main_cur1, check_cur, bs_cur = cursors
        base_tables = mv_configs.tables[:1]

        # a
        main_cur1.execute('begin')
        self._read_all_mv(True, True, mv_configs, cluster, [main_cur1])
        self._check_state(CLEAN, CLEAN, mv_configs, cluster)
        self._do_sql(IDCU, True, True, base_tables, cluster, [main_cur1])
        self._check_state(BURST_OWN, CLEAN, mv_configs, cluster)
        # This mv-refresh is no-op as txn on base-table hasn't committed
        self._refresh_all_mv(
            True, False, mv_configs, cluster, cursors, is_stale=False)
        self._check_state(BURST_OWN, CLEAN, mv_configs, cluster)
        main_cur1.execute('commit')
        # MV-refresh can burst since DML on base tables bursted and committed
        self._refresh_all_mv(
            True, True, mv_configs, cluster, cursors, is_stale=True)
        self._check_state(BURST_OWN, BURST_OWN, mv_configs, cluster)

        # b
        main_cur1.execute('begin')
        self._read_all_mv(True, True, mv_configs, cluster, [main_cur1])
        self._check_state(BURST_OWN, BURST_OWN, mv_configs, cluster)
        self._do_sql(IDCU, True, True, base_tables, cluster, [main_cur1])
        self._check_state(BURST_OWN, BURST_OWN, mv_configs, cluster)
        # This mv-refresh is no-op as txn on base-table hasn't committed
        self._refresh_all_mv(
            True, False, mv_configs, cluster, cursors, is_stale=False)
        self._check_state(BURST_OWN, BURST_OWN, mv_configs, cluster)
        main_cur1.execute('abort')
        # MV-refresh cannot burst as base-tables are MAIN_UNDO
        self._refresh_all_mv(
            True, False, mv_configs, cluster, cursors, is_stale=False)
        self._check_state(MAIN_UNDO, BURST_OWN, mv_configs, cluster)

        # c
        main_cur1.execute('begin')
        self._read_all_mv(True, True, mv_configs, cluster, [main_cur1])
        self._check_state(MAIN_UNDO, BURST_OWN, mv_configs, cluster)
        self._do_sql(IDCU, True, False, base_tables, cluster, [main_cur1])
        self._check_state(MAIN_OWN + MAIN_UNDO, BURST_OWN, mv_configs, cluster)
        # This mv-refresh is no-op as txn on base-table hasn't committed
        self._refresh_all_mv(
            True, False, mv_configs, cluster, cursors, is_stale=False)
        self._check_state(MAIN_OWN + MAIN_UNDO, BURST_OWN, mv_configs, cluster)
        main_cur1.execute('commit')
        # MV-refresh cannot burst as base-tables are MAIN_UNDO
        self._refresh_all_mv(
            True, False, mv_configs, cluster, cursors, is_stale=True)
        self._check_state(MAIN_UNDO, CLEAN, mv_configs, cluster)

    def base_test_burst_mv_ownership(self, cluster, vector):
        relprefix = "ownership"
        session1 = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))
        session2 = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))

        main_cur1 = session1.cursor()
        check_cur = session2.cursor()
        bs_cur = self.db.cursor()
        test_cases = [
            self._test_case_1,
            self._test_case_2,
            self._test_case_3,
            self._test_case_4,
            self._test_case_5,
        ]
        with self.setup_views_with_shared_tables(
                cluster, check_cur, relprefix, diststyle, sortkey, MV_QUERIES,
                False, 'noburst') as mv_configs:
            bs_cur.execute("set query_group to burst;")
            bs_cur.execute("set search_path to {}".format(MY_SCHEMA))
            cursors = [main_cur1, check_cur, bs_cur]
            test_cases[vector.test_case - 1](cluster, cursors, mv_configs)


@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(
    gucs=list(burst_write_basic_gucs.items()) + [('slices_per_node', '3')])
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.serial_only
@pytest.mark.skip_load_data
class TestBurstWriteMVOwnershipSS(TestBurstWriteMVOwnershipBase):
    def test_burst_mv_ownership_ss(self, cluster, vector):
        self.base_test_burst_mv_ownership(cluster, vector)
