# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.common.dimensions import Dimensions
from raff.storage.storage_test import create_thread
from psycopg2.extensions import QueryCanceledError

__all__ = [super_simulated_mode]
log = logging.getLogger(__name__)
INSERT_STMT_T1 = "insert into {}.bw_t1 values(10,10);"
INSERT_STMT_T2 = "insert into {}.bw_t2 values(10,10);"
READ_STMT = ("select sum(t1.c0), sum(t2.c0) "
             "from bw_t1 t1, bw_t2 t2 where t1.c0 = t2.c0;")
INSERT_SELECT_STMT = ("insert into bw_t1 select * from bw_t2 t2;")
MAIN_OWNED = [('Main', 'Owned')]
MAIN_UNDO = [('Main', 'Undo')]
BURST_OWNED = [('Burst', 'Owned')]


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.custom_burst_gucs(
        gucs={'slices_per_node': '3', 'burst_enable_write':'true'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteConcurrentQuery(BurstWriteTest):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions({
            "guard": ['burst:check_backup', 'burst:find_cluster',
                      'burst:found_cluster', 'burst_write:pre_write_lock'],
            "is_owned_table": [True, False]
        })

    def _setup_tables(self, db_session, schema):
        with db_session.cursor() as cursor:
            cursor.execute("begin;")
            cursor.execute(
                    "create table bw_t1(c0 int, c1 bigint) diststyle even;")
            cursor.execute(
                    "create table bw_t2(c0 int, c1 bigint) distkey(c0);")
            cursor.execute("select * from bw_t1;");
            cursor.execute("select * from bw_t2;");
            cursor.execute("insert into bw_t1 values(0,0),(1,1),(2,2),(3,3)")
            cursor.execute("insert into bw_t2 values(0,0),(1,1),(2,2),(3,3)")
            for i in range(12):
                cursor.execute("insert into bw_t1 select * from bw_t1;")
                cursor.execute("insert into bw_t2 select * from bw_t2;")
            cursor.execute("commit;")

    def _init_check(self, cluster, cursor, schema):
        cursor.execute("select sum(c0), sum(c1), count(*) from bw_t1;")
        assert cursor.fetchall() == [(24576, 24576, 16384)]
        self._check_last_query_bursted(cluster, cursor)
        cursor.execute("select sum(c0), sum(c1), count(*) from bw_t2;")
        assert cursor.fetchall() == [(24576, 24576, 16384)]
        self._check_last_query_bursted(cluster, cursor)
        self._validate_ownership_state(schema, 'bw_t1', [])
        self._validate_ownership_state(schema, 'bw_t2', [])
        # make burst cluster owns bw_t1 and bw_t2
        cursor.execute("begin;")
        cursor.execute("insert into bw_t1 values(0,0),(1,1),(2,2),(3,3)")
        self._check_last_query_bursted(cluster, cursor)
        self._validate_ownership_state(schema, 'bw_t1', BURST_OWNED)
        cursor.execute("insert into bw_t2 values(0,0),(1,1),(2,2),(3,3)")
        self._check_last_query_bursted(cluster, cursor)
        self._validate_ownership_state(schema, 'bw_t2', BURST_OWNED)
        cursor.execute("commit")
        self._validate_ownership_state(schema, 'bw_t1', BURST_OWNED)
        self._validate_ownership_state(schema, 'bw_t2', BURST_OWNED)

    def _do_background_query(self, cluster, cursor, schema, query, is_bursted):
        cursor.execute('set query_group to burst;')
        cursor.execute(query)
        if is_bursted:
            self._check_last_query_bursted(cluster, cursor)
        else:
            self._check_last_query_didnt_burst(cluster, cursor)

    def _close_test(self, cluster, cursor, schema):
        cursor.execute("insert into bw_t1 values(10,10);")
        self._check_last_query_bursted(cluster, cursor)
        cursor.execute("insert into bw_t2 values(10,10);")
        self._check_last_query_bursted(cluster, cursor)
        self._validate_ownership_state(schema, 'bw_t1', BURST_OWNED)
        self._validate_ownership_state(schema, 'bw_t2', BURST_OWNED)
        self._validate_table(cluster, schema, 'bw_t1', 'even')
        self._validate_table(cluster, schema, 'bw_t2', 'distkey')
        cursor.execute("drop table bw_t1;")
        cursor.execute("drop table bw_t2;")

    def _should_burst(self, vector, is_commit):
        return (vector.guard == 'burst:found_cluster' or \
                not vector.is_owned_table) and \
               (is_commit or vector.guard != 'burst_write:pre_write_lock');

    def _abort_dml_txn(self, schema):
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute("xpx 'event set EtBurstWriteGuard'")
            bootstrap_cursor.execute("begin;")
            bootstrap_cursor.execute(INSERT_STMT_T2.format(schema))
            self._validate_ownership_state(schema, 'bw_t2', MAIN_OWNED)
            bootstrap_cursor.execute("abort;")
            self._validate_ownership_state(schema, 'bw_t2', MAIN_UNDO)
            bootstrap_cursor.execute("xpx 'event unset EtBurstWriteGuard'")

    def test_disqualify_burst_write_by_concurrent_undo(
            self, cluster, vector, db_session):
        """
        Test: Block write query on different burst qualification position and
              undo dml on table referred by write query in background. Check
              the burst qualification works correctly.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, schema)
            self._start_and_wait_for_refresh(cluster)
            if vector.is_owned_table:
                self._init_check(cluster, cursor, schema)
            is_bursted = self._should_burst(vector, False)
            params = (cluster, cursor, schema, INSERT_SELECT_STMT, is_bursted)
            with create_thread(self._do_background_query, params) as thread, \
                    self._create_xen_guard(vector.guard) as xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks()
                self._abort_dml_txn(schema)
                xen_guard.disable()
            # Since it is write query, bw_t1 is in BURST_OWNED state only if
            # the write query is burted.
            bw_t1_state = BURST_OWNED if is_bursted else []
            self._validate_ownership_state(schema, 'bw_t1', bw_t1_state)
            # Since write query on bw_t2 is aborted, bw_t2 must be MAIN_UNDO
            # state.
            self._validate_ownership_state(schema, 'bw_t2', MAIN_UNDO)
            self._start_and_wait_for_refresh(cluster)
            # check table ownership after backup and refresh
            self._validate_ownership_state(schema, 'bw_t1', bw_t1_state)
            self._validate_ownership_state(schema, 'bw_t2', [])
            content_t1 = [(49164, 49164, 32776)] if vector.is_owned_table else \
                    [(49152, 49152, 32768)]
            content_t2 = [(24582, 24582, 16388)] if vector.is_owned_table else \
                    [(24576, 24576, 16384)]
            # check burst cluster table content
            cursor.execute("set query_group to burst;")
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t1;")
            assert cursor.fetchall() == content_t1
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t2;")
            assert cursor.fetchall() == content_t2
            self._check_last_query_bursted(cluster, cursor)
            # check main cluster table content
            cursor.execute("set query_group to metrics;")
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t1;")
            assert cursor.fetchall() == content_t1
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t2;")
            assert cursor.fetchall() == content_t2
            cursor.execute("set query_group to burst;")
            self._close_test(cluster, cursor, schema)

    def test_disqualify_burst_read_by_concurrent_undo(
            self, cluster, vector, db_session):
        """
        Test: Block read query on different burst qualification position and
              undo dml on table referred by read query in background. Check
              the burst qualification works correctly.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, schema)
            self._start_and_wait_for_refresh(cluster)
            if vector.is_owned_table:
                self._init_check(cluster, cursor, schema)
            is_bursted = self._should_burst(vector, False)
            params = (cluster, cursor, schema, READ_STMT, is_bursted)
            with create_thread(self._do_background_query, params) as thread, \
                    self._create_xen_guard(vector.guard) as xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks()
                self._abort_dml_txn(schema)
                xen_guard.disable()
            # Since it is read query, bw_t1 is determined by the initial state
            # of this test.
            bw_t1_state = BURST_OWNED if vector.is_owned_table else []
            self._validate_ownership_state(schema, 'bw_t1', bw_t1_state)
            # Since write query on bw_t2 is aborted, bw_t2 must be MAIN_UNDO
            # state.
            self._validate_ownership_state(schema, 'bw_t2', MAIN_UNDO)
            self._start_and_wait_for_refresh(cluster)
            # check table ownership after backup and refresh
            self._validate_ownership_state(schema, 'bw_t1', bw_t1_state)
            self._validate_ownership_state(schema, 'bw_t2', [])
            content = [(24582, 24582, 16388)] if vector.is_owned_table else \
                    [(24576, 24576, 16384)]
            # check burst cluster table content
            cursor.execute("set query_group to burst;")
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t1;")
            assert cursor.fetchall() == content
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t2;")
            assert cursor.fetchall() == content
            self._check_last_query_bursted(cluster, cursor)
            # check main cluster table content
            cursor.execute("set query_group to metrics;")
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t1;")
            assert cursor.fetchall() == content
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t2;")
            assert cursor.fetchall() == content
            cursor.execute("set query_group to burst;")
            self._close_test(cluster, cursor, schema)

    def _abort_dml_target_table_txn(self, schema):
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute("xpx 'event set EtBurstWriteGuard'")
            bootstrap_cursor.execute("begin;")
            bootstrap_cursor.execute(INSERT_STMT_T1.format(schema))
            self._validate_ownership_state(schema, 'bw_t1', MAIN_OWNED)
            bootstrap_cursor.execute("abort;")
            self._validate_ownership_state(schema, 'bw_t1', MAIN_UNDO)
            bootstrap_cursor.execute("xpx 'event unset EtBurstWriteGuard'")

    def test_disqualify_burst_read_by_concurrent_undo_target_table(
            self, cluster, vector, db_session):
        """
        Test: Block read query on different burst qualification position and
              undo dml on target table in background. Check the burst
              qualification works correctly.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, schema)
            self._start_and_wait_for_refresh(cluster)
            if vector.is_owned_table:
                self._init_check(cluster, cursor, schema)
            is_bursted = self._should_burst(vector, False)
            params = (cluster, cursor, schema, READ_STMT, is_bursted)
            with create_thread(self._do_background_query, params) as thread, \
                    self._create_xen_guard(vector.guard) as xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks()
                self._abort_dml_target_table_txn(schema)
                xen_guard.disable()
            # Since write query on bw_t1 is aborted, bw_t1 must be MAIN_UNDO
            # state.
            self._validate_ownership_state(schema, 'bw_t1', MAIN_UNDO)
            # Since it is read query, bw_t2 is determined by the initial state
            # of this test.
            bw_t2_state = BURST_OWNED if vector.is_owned_table else []
            self._validate_ownership_state(schema, 'bw_t2', bw_t2_state)
            self._start_and_wait_for_refresh(cluster)
            # check table ownership after backup and refresh
            self._validate_ownership_state(schema, 'bw_t1', [])
            self._validate_ownership_state(schema, 'bw_t2', bw_t2_state)
            content = [(24582, 24582, 16388)] if vector.is_owned_table else \
                    [(24576, 24576, 16384)]
            # check burst cluster table content
            cursor.execute("set query_group to burst;")
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t1;")
            assert cursor.fetchall() == content
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t2;")
            assert cursor.fetchall() == content
            self._check_last_query_bursted(cluster, cursor)
            # check main cluster table content
            cursor.execute("set query_group to metrics;")
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t1;")
            assert cursor.fetchall() == content
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t2;")
            assert cursor.fetchall() == content
            cursor.execute("set query_group to burst;")
            self._close_test(cluster, cursor, schema)

    def _commit_dml(self, schema, xen_guard, concurrent_thread):
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute("xpx 'event set EtBurstWriteGuard'")
            bootstrap_cursor.execute("begin;")
            bootstrap_cursor.execute(INSERT_STMT_T2.format(schema))
            self._validate_ownership_state(schema, 'bw_t2', MAIN_OWNED)
            xen_guard.disable()
            concurrent_thread.join()
            bootstrap_cursor.execute("commit;")
            self._validate_ownership_state(schema, 'bw_t2', [])
            bootstrap_cursor.execute("xpx 'event unset EtBurstWriteGuard'")

    def test_disqualify_burst_write_tables_by_concurrent_dml(
            self, cluster, vector, db_session):
        """
        Test: Block write query on different burst qualification position and
              commit dml on owned tables in background. Check the burst
              qualification works correctly.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, schema)
            self._start_and_wait_for_refresh(cluster)
            if vector.is_owned_table:
                self._init_check(cluster, cursor, schema)
            is_bursted = self._should_burst(vector, True)
            params = (cluster, cursor, schema, INSERT_SELECT_STMT, is_bursted)
            with create_thread(self._do_background_query, params) as thread, \
                    self._create_xen_guard(vector.guard) as xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks()
                self._commit_dml(schema, xen_guard, thread)
            # Since it is write query, bw_t1 is in BURST_OWNED state only if
            # the write query is burted.
            bw_t1_state = BURST_OWNED if is_bursted else []
            self._validate_ownership_state(schema, 'bw_t1', bw_t1_state)
            # Since write query on bw_t2 is ran on main cluster, then bw_t2's
            # state is empty.
            self._validate_ownership_state(schema, 'bw_t2', [])
            self._start_and_wait_for_refresh(cluster)
            # check table ownership after backup and refresh.
            self._validate_ownership_state(schema, 'bw_t1', bw_t1_state)
            self._validate_ownership_state(schema, 'bw_t2', [])
            content_t1 = [(49164, 49164, 32776)] if vector.is_owned_table else \
                    [(49152, 49152, 32768)]
            content_t2 = [(24592, 24592, 16389)] if vector.is_owned_table else \
                    [(24586, 24586, 16385)]
            # check burst cluster table content
            cursor.execute("set query_group to burst;")
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t1;")
            assert cursor.fetchall() == content_t1
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t2;")
            assert cursor.fetchall() == content_t2
            self._check_last_query_bursted(cluster, cursor)
            # check main cluster table content
            cursor.execute("set query_group to metrics;")
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t1;")
            assert cursor.fetchall() == content_t1
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t2;")
            assert cursor.fetchall() == content_t2
            cursor.execute("set query_group to burst;")
            self._close_test(cluster, cursor, schema)

    def test_disqualify_burst_read_tables_by_concurrent_dml(
            self, cluster, vector, db_session):
        """
        Test: Block read query on different burst qualification position and
              commit dml on owned tables in background. Check the burst
              qualification works correctly.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, schema)
            self._start_and_wait_for_refresh(cluster)
            if vector.is_owned_table:
                self._init_check(cluster, cursor, schema)
            is_bursted = self._should_burst(vector, True)
            params = (cluster, cursor, schema, READ_STMT, is_bursted)
            with create_thread(self._do_background_query, params) as thread, \
                    self._create_xen_guard(vector.guard) as xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks()
                self._commit_dml(schema, xen_guard, thread)
            # Since it is read query, bw_t1 is determined by the initial state
            # of this test.
            bw_t1_state = BURST_OWNED if vector.is_owned_table else []
            self._validate_ownership_state(schema, 'bw_t1', bw_t1_state)
            # Since write query on bw_t2 is ran on main cluster, then bw_t2's
            # state is empty.
            self._validate_ownership_state(schema, 'bw_t2', [])
            self._start_and_wait_for_refresh(cluster)
            # check table ownership after backup and refresh.
            self._validate_ownership_state(schema, 'bw_t1', bw_t1_state)
            self._validate_ownership_state(schema, 'bw_t2', [])
            content_t1 = [(24582, 24582, 16388)] if vector.is_owned_table else \
                    [(24576, 24576, 16384)]
            content_t2 = [(24592, 24592, 16389)] if vector.is_owned_table else \
                    [(24586, 24586, 16385)]
            cursor.execute("set query_group to burst;")
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t1;")
            assert cursor.fetchall() == content_t1
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t2;")
            assert cursor.fetchall() == content_t2
            self._check_last_query_bursted(cluster, cursor)
            # check main cluster table content
            cursor.execute("set query_group to metrics;")
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t1;")
            assert cursor.fetchall() == content_t1
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t2;")
            assert cursor.fetchall() == content_t2
            cursor.execute("set query_group to burst;")
            self._close_test(cluster, cursor, schema)
