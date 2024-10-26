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
insert_select = "insert into {} select * from {};"
insert_values = "insert into {} values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5);"
read_stmt = "select count(*) from {};"
delete_stmt = "delete from {} where c0 = {};"
update_stmt = "update {} set c0 = c0 + 1, c1 = c1 + 100;"


class BaseBurstWriteTableRefresh(BurstWriteTest):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions({
            "query": ['delete', 'update', 'insert value', 'insert select'],
            "sortkey": [""],
        })

    def _setup_tables(self, db_session, schema, sortkey):
        with db_session.cursor() as cursor:
            t1_def = "create table bw_t1(c0 int, c1 bigint) diststyle even {};"
            t2_def = "create table bw_t2(c0 int, c1 bigint) distkey(c0) {};"
            t3_def = "create table bw_t3(c0 int, c1 bigint) diststyle even {};"
            t4_def = "create table bw_t4(c0 int, c1 bigint) distkey(c1) {};"
            t5_def = "create table bw_t5(c0 int, c1 bigint) distkey(c1) {};"
            cursor.execute("begin;")
            cursor.execute(t1_def.format(sortkey))
            cursor.execute(t2_def.format(sortkey))
            cursor.execute(t3_def.format(sortkey))
            cursor.execute(t4_def.format(sortkey))
            cursor.execute(t5_def.format(sortkey))
            cursor.execute("select * from bw_t1;")
            cursor.execute("select * from bw_t2;")
            cursor.execute("select * from bw_t3;")
            cursor.execute("select * from bw_t4;")
            cursor.execute("insert into bw_t1 values(0,0),(1,1),(2,2),(3,3)")
            cursor.execute("insert into bw_t2 values(0,0),(1,1),(2,2),(3,3)")
            cursor.execute("insert into bw_t3 values(0,0),(1,1),(2,2),(3,3)")
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
        cursor.execute("select sum(c0), sum(c1), count(*) from bw_t3;")
        assert cursor.fetchall() == [(6, 6, 4)]
        self._check_last_query_bursted(cluster, cursor)
        cursor.execute("select sum(c0), sum(c1), count(*) from bw_t4;")
        assert cursor.fetchall() == [(None, None, 0)]
        self._check_last_query_bursted(cluster, cursor)

        self._validate_ownership_state(schema, 'bw_t1', [])
        self._validate_ownership_state(schema, 'bw_t2', [])
        self._validate_ownership_state(schema, 'bw_t3', [])
        self._validate_ownership_state(schema, 'bw_t4', [])
        cursor.execute("begin;")
        # make burst cluster owns bw_t1 and bw_t2
        cursor.execute("insert into bw_t1 values(0,0),(1,1),(2,2),(3,3)")
        self._validate_ownership_state(schema, 'bw_t1', [('Burst', 'Owned')])
        self._check_last_query_bursted(cluster, cursor)
        cursor.execute("insert into bw_t2 values(0,0),(1,1),(2,2),(3,3)")
        self._validate_ownership_state(schema, 'bw_t2', [('Burst', 'Owned')])
        self._check_last_query_bursted(cluster, cursor)
        cursor.execute("commit")
        self._validate_ownership_state(schema, 'bw_t1', [('Burst', 'Owned')])
        self._validate_ownership_state(schema, 'bw_t2', [('Burst', 'Owned')])
        self._validate_ownership_state(schema, 'bw_t3', [])
        self._validate_ownership_state(schema, 'bw_t4', [])
        # Make bw_t5 stale on burst clusters.
        cursor.execute("set query_group to 'noburst';")
        cursor.execute("insert into bw_t5 values(0,0),(1,1),(2,2),(3,3)")
        self._check_last_query_didnt_burst(cluster, cursor)
        cursor.execute("set query_group to burst;")

    def _run_query(self, cursor, query, counter, table):
        if query == 'insert select':
            stmt = insert_select.format(table, table)
        elif query == 'insert value':
            stmt = insert_values.format(table)
        elif query == 'update':
            stmt = update_stmt.format(table)
        elif query == 'delete':
            stmt = delete_stmt.format(table, counter)
        else:
            stmt = read_stmt.format(table)
        cursor.execute(stmt)

    def _validate_tables(self, cluster, schema):
        self._validate_table(cluster, schema, 'bw_t1', 'even')
        self._validate_table(cluster, schema, 'bw_t2', 'distkey')
        self._validate_table(cluster, schema, 'bw_t3', 'even')
        self._validate_table(cluster, schema, 'bw_t4', 'distkey')

    def _validate_all_can_burst(
            self, cluster, cursor, query, schema, is_structure_changed=False):
        # After refreshing is completed, all table should be able to burst.
        if is_structure_changed:
            state = [('Burst', 'Owned'), ('Main', 'Structure Changed')]
        else:
            state = [('Burst', 'Owned')]
        cursor.execute("begin;")
        self._run_query(cursor, query, 1, 'bw_t1')
        self._check_last_query_bursted(cluster, cursor)
        self._validate_ownership_state(schema, 'bw_t1', state)

        self._run_query(cursor, query, 2, 'bw_t2')
        self._validate_ownership_state(schema, 'bw_t2', state)
        self._check_last_query_bursted(cluster, cursor)

        self._run_query(cursor, query, 3, 'bw_t3')
        self._validate_ownership_state(schema, 'bw_t3', state)
        self._check_last_query_bursted(cluster, cursor)

        self._run_query(cursor, query, 4, 'bw_t4')
        self._validate_ownership_state(schema, 'bw_t4', state)
        self._check_last_query_bursted(cluster, cursor)

        self._run_query(cursor, 'read-only', 1, 'bw_t1')
        self._check_last_query_bursted(cluster, cursor)
        self._run_query(cursor, 'read-only', 1, 'bw_t2')
        self._check_last_query_bursted(cluster, cursor)
        self._run_query(cursor, 'read-only', 1, 'bw_t3')
        self._check_last_query_bursted(cluster, cursor)
        self._run_query(cursor, 'read-only', 1, 'bw_t4')
        self._check_last_query_bursted(cluster, cursor)
        cursor.execute("commit;")

        self._validate_ownership_state(schema, 'bw_t1', state)
        self._validate_ownership_state(schema, 'bw_t2', state)
        self._validate_ownership_state(schema, 'bw_t3', state)
        self._validate_ownership_state(schema, 'bw_t4', state)

    def _validate_all_cant_burst(self, cluster, cursor, query, commit=True):
        cursor.execute("begin;")
        self._run_query(cursor, 'read-only', 1, 'bw_t1')
        self._check_last_query_didnt_burst(cluster, cursor)
        self._run_query(cursor, 'read-only', 1, 'bw_t2')
        self._check_last_query_didnt_burst(cluster, cursor)
        self._run_query(cursor, 'read-only', 1, 'bw_t3')
        self._check_last_query_didnt_burst(cluster, cursor)
        self._run_query(cursor, 'read-only', 1, 'bw_t4')
        self._check_last_query_didnt_burst(cluster, cursor)

        self._run_query(cursor, query, 1, 'bw_t1')
        self._check_last_query_didnt_burst(cluster, cursor)
        self._run_query(cursor, query, 2, 'bw_t2')
        self._check_last_query_didnt_burst(cluster, cursor)
        self._run_query(cursor, query, 3, 'bw_t3')
        self._check_last_query_didnt_burst(cluster, cursor)
        self._run_query(cursor, query, 4, 'bw_t4')
        self._check_last_query_didnt_burst(cluster, cursor)
        if commit:
            cursor.execute("commit;")
        else:
            cursor.execute("abort;")


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '3',
        'burst_enable_write': 'true',
        'enable_burst_refresh_changed_tables': 'true'
    })
@pytest.mark.custom_local_gucs(
    gucs={
        'enable_burst_refresh_changed_tables': 'true',
        'burst_enable_write_on_skipped_tables_during_refresh': 'false'
    })
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteTableRefresh(BaseBurstWriteTableRefresh):
    def test_burst_dml_on_cluster_refreshing(self, cluster, db_session, vector):
        """
        Test: Run DMLs on tables owned/unowned by burst cluster, when burst
              cluster is refreshing and after burst cluster is refreshed.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, schema, vector.sortkey)
            self._start_and_wait_for_refresh(cluster)
            self._init_check(cluster, cursor, schema)
            # Block burst cluster from completing refresh.
            xen_guard = self._create_xen_guard(
                    "check_refresh_status:mark_successful")
            finish_xen_guard = self._create_xen_guard(
                    "check_refresh_status:complete")
            with xen_guard, finish_xen_guard:
                finish_xen_guard.disable()
                self._start_and_wait_for_refresh(cluster, early_return=True)
                xen_guard.wait_until_process_blocks()
                finish_xen_guard.enable()
                # Table bw_t1 and bw_t2 are owned by burst cluster. Although the
                # cluster is refreshing, bw_t1 and bw_t2 can still be bursted.
                self._run_query(cursor, vector.query, 1, 'bw_t1')
                self._validate_ownership_state(
                        schema, 'bw_t1', [('Burst', 'Owned')])
                self._check_last_query_bursted(cluster, cursor)
                self._run_query(cursor, vector.query, 2, 'bw_t2')
                self._validate_ownership_state(
                        schema, 'bw_t2', [('Burst', 'Owned')])
                self._check_last_query_bursted(cluster, cursor)
                # Table bw_t3 and bw_t4 are not owned by burst cluster and the
                # cluster is refreshing, thus they can not be bursted.
                self._run_query(cursor, vector.query, 3, 'bw_t3')
                self._validate_ownership_state(schema, 'bw_t3', [])
                self._check_last_query_didnt_burst(cluster, cursor)
                self._run_query(cursor, vector.query, 4, 'bw_t4')
                self._validate_ownership_state(schema, 'bw_t4', [])
                self._check_last_query_didnt_burst(cluster, cursor)
                xen_guard.disable()
                finish_xen_guard.wait_until_process_blocks()

            self._start_and_wait_for_refresh(cluster)
            self._validate_ownership_state(
                    schema, 'bw_t1', [('Burst', 'Owned')])
            self._validate_ownership_state(
                    schema, 'bw_t2', [('Burst', 'Owned')])
            self._validate_ownership_state(schema, 'bw_t3', [])
            self._validate_ownership_state(schema, 'bw_t4', [])
            # After refreshing is completed, all table should be able to burst.
            self._validate_all_can_burst(cluster, cursor, vector.query, schema)
            self._validate_tables(cluster, schema)

    def test_burst_dml_abort_on_cluster_refreshing(self, cluster, db_session, vector):
        """
        Test: Run DMLs on tables owned/unowned by burst cluster and abort
              transaction, when burst cluster is refreshing and after burst
              cluster is refreshed.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, schema, vector.sortkey)
            self._start_and_wait_for_refresh(cluster)
            self._init_check(cluster, cursor, schema)
            # Block burst cluster from completing refresh.
            xen_guard = self._create_xen_guard(
                    "check_refresh_status:mark_successful")
            finish_xen_guard = self._create_xen_guard(
                    "check_refresh_status:complete")
            with xen_guard, finish_xen_guard:
                finish_xen_guard.disable()
                self._start_and_wait_for_refresh(cluster, early_return=True)
                xen_guard.wait_until_process_blocks()
                cursor.execute("begin;")
                # Table bw_t1 and bw_t2 are owned by burst cluster. Although the
                # cluster is refreshing, bw_t1 and bw_t2 can still be bursted.
                self._run_query(cursor, vector.query, 1, 'bw_t1')
                self._validate_ownership_state(
                        schema, 'bw_t1', [('Burst', 'Owned')])
                self._check_last_query_bursted(cluster, cursor)
                self._run_query(cursor, vector.query, 2, 'bw_t2')
                self._validate_ownership_state(
                        schema, 'bw_t2', [('Burst', 'Owned')])
                self._check_last_query_bursted(cluster, cursor)
                # Table bw_t3 and bw_t4 are not owned by burst cluster and the
                # cluster is refreshing, thus they can not be bursted.
                self._run_query(cursor, vector.query, 3, 'bw_t3')
                self._validate_ownership_state(
                        schema, 'bw_t3', [('Main', 'Owned')])
                self._check_last_query_didnt_burst(cluster, cursor)
                self._run_query(cursor, vector.query, 4, 'bw_t4')
                self._validate_ownership_state(
                        schema, 'bw_t4', [('Main', 'Owned')])
                self._check_last_query_didnt_burst(cluster, cursor)
                cursor.execute("abort;")
                self._validate_ownership_state(
                        schema, 'bw_t1', [('Main', 'Undo')])
                self._validate_ownership_state(
                        schema, 'bw_t2', [('Main', 'Undo')])
                self._validate_ownership_state(
                        schema, 'bw_t3', [('Main', 'Undo')])
                self._validate_ownership_state(
                        schema, 'bw_t4', [('Main', 'Undo')])
                # Both read-only and write query can not burst now, because it is in
                # no burst state.
                self._validate_all_cant_burst(cluster, cursor, vector.query)
                finish_xen_guard.enable()
                xen_guard.disable()
                finish_xen_guard.wait_until_process_blocks()

            # Both read-only and write query can not burst now, because it is in
            # no burst state.
            self._validate_all_cant_burst(cluster, cursor, vector.query)
            self._start_and_wait_for_refresh(cluster)
            self._validate_ownership_state(schema, 'bw_t1', [])
            self._validate_ownership_state(schema, 'bw_t2', [])
            self._validate_ownership_state(schema, 'bw_t3', [])
            self._validate_ownership_state(schema, 'bw_t4', [])
            self._validate_all_can_burst(cluster, cursor, vector.query, schema)
            self._validate_tables(cluster, schema)

    def test_burst_dml_vacuum_cluster_refreshing(
            self, cluster, db_session, vector):
        """
        Test: Run vaccum on tables owned/unowned by burst cluster and
              commit. Then, run DMLs on these table to ensure only read-only
              queries can burst. After backup and refresh, both write and
              read-only queries can burst.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, schema, vector.sortkey)
            self._start_and_wait_for_refresh(cluster)
            self._init_check(cluster, cursor, schema)
            # Block burst cluster from completing refresh.
            xen_guard = self._create_xen_guard(
                    "check_refresh_status:mark_successful")
            finish_xen_guard = self._create_xen_guard(
                    "check_refresh_status:complete")
            with xen_guard, finish_xen_guard:
                finish_xen_guard.disable()
                self._start_and_wait_for_refresh(cluster, early_return=True)
                xen_guard.wait_until_process_blocks()
                # Only read-only queries can burst.
                cursor.execute("begin;")
                # Table bw_t1 and bw_t2 are owned by burst cluster. Although the
                # cluster is refreshing, bw_t1 and bw_t2 can still be bursted.
                self._run_query(cursor, vector.query, 1, 'bw_t1')
                self._check_last_query_bursted(cluster, cursor)
                self._validate_ownership_state(
                        schema, 'bw_t1', [('Burst', 'Owned')])

                self._run_query(cursor, vector.query, 2, 'bw_t2')
                self._check_last_query_bursted(cluster, cursor)
                self._validate_ownership_state(
                        schema, 'bw_t2', [('Burst', 'Owned')])
                # Table bw_t3 and bw_t4 are not owned by burst cluster and the
                # cluster is refreshing, thus they can not be bursted.
                self._run_query(cursor, vector.query, 3, 'bw_t3')
                self._check_last_query_didnt_burst(cluster, cursor)
                self._validate_ownership_state(
                        schema, 'bw_t3', [('Main', 'Owned')])
                self._run_query(cursor, vector.query, 4, 'bw_t4')
                self._check_last_query_didnt_burst(cluster, cursor)
                self._validate_ownership_state(
                        schema, 'bw_t4', [('Main', 'Owned')])
                cursor.execute("commit;")
                self._validate_ownership_state(
                        schema, 'bw_t1', [('Burst', 'Owned')])
                self._validate_ownership_state(
                        schema, 'bw_t2', [('Burst', 'Owned')])
                self._validate_ownership_state(schema, 'bw_t3', [])
                self._validate_ownership_state(schema, 'bw_t4', [])

                cursor.execute("begin;")
                cursor.execute("delete from bw_t1;")
                self._check_last_query_bursted(cluster, cursor)
                self._validate_ownership_state(
                        schema, 'bw_t1', [('Burst', 'Owned')])

                cursor.execute("delete from bw_t2;")
                self._check_last_query_bursted(cluster, cursor)
                self._validate_ownership_state(
                        schema, 'bw_t2', [('Burst', 'Owned')])

                cursor.execute("delete from bw_t3;")
                self._check_last_query_didnt_burst(cluster, cursor)
                self._validate_ownership_state(
                        schema, 'bw_t3', [('Main', 'Owned')])

                cursor.execute(
                        "insert into bw_t4 values(1,1),(2,2),(3,3),(4,4);")
                cursor.execute(
                        "insert into bw_t4 values(11,11),(12,12),(13,13);")
                cursor.execute(
                        "insert into bw_t4 values(21,21),(22,22),(23,23);")
                cursor.execute(
                        "insert into bw_t4 values(31,31),(32,32),(33,33);")
                cursor.execute(
                        "insert into bw_t4 values(41,41),(42,42),(43,43);")
                cursor.execute("delete from bw_t4;")
                self._check_last_query_didnt_burst(cluster, cursor)
                self._validate_ownership_state(
                        schema, 'bw_t4', [('Main', 'Owned')])
                cursor.execute("commit")

                cursor.execute("vacuum bw_t1 to 100 percent;")
                self._validate_ownership_state(schema, 'bw_t1',
                                               [('Burst', 'Owned'),
                                                ('Main', 'Structure Changed')])
                cursor.execute("vacuum bw_t2 to 100 percent;")
                self._validate_ownership_state(schema, 'bw_t2',
                                               [('Burst', 'Owned'),
                                                ('Main', 'Structure Changed')])
                cursor.execute("vacuum bw_t3 to 100 percent;")
                self._validate_ownership_state(
                        schema, 'bw_t3', [('Main', 'Structure Changed')])
                cursor.execute("vacuum bw_t4 to 100 percent;")
                self._validate_ownership_state(
                        schema, 'bw_t4', [('Main', 'Structure Changed')])

                cursor.execute("begin;")
                self._run_query(cursor, vector.query, 1, 'bw_t1')
                self._check_last_query_didnt_burst(cluster, cursor)
                self._validate_ownership_state(schema, 'bw_t1',
                                               [('Main', 'Owned'),
                                                ('Main', 'Structure Changed')])

                self._run_query(cursor, vector.query, 2, 'bw_t2')
                self._check_last_query_didnt_burst(cluster, cursor)
                self._validate_ownership_state(schema, 'bw_t2',
                                               [('Main', 'Owned'),
                                                ('Main', 'Structure Changed')])

                self._run_query(cursor, vector.query, 3, 'bw_t3')
                self._check_last_query_didnt_burst(cluster, cursor)
                self._validate_ownership_state(
                        schema, 'bw_t3', [('Main', 'Owned'),
                                          ('Main', 'Structure Changed')])

                self._run_query(cursor, vector.query, 4, 'bw_t4')
                self._check_last_query_didnt_burst(cluster, cursor)
                self._validate_ownership_state(
                        schema, 'bw_t4', [('Main', 'Owned'),
                                          ('Main', 'Structure Changed')])
                cursor.execute("commit;")
                finish_xen_guard.enable()
                xen_guard.disable()
                finish_xen_guard.wait_until_process_blocks()

            self._validate_ownership_state(
                    schema, 'bw_t1', [('Main', 'Structure Changed')])
            self._validate_ownership_state(
                    schema, 'bw_t2', [('Main', 'Structure Changed')])
            self._validate_ownership_state(
                    schema, 'bw_t3', [('Main', 'Structure Changed')])
            self._validate_ownership_state(
                    schema, 'bw_t4', [('Main', 'Structure Changed')])
            # Only read queries can burst.
            self._validate_all_cant_burst(cluster, cursor, vector.query)
            # After refreshing is completed, all table should be able to burst.
            self._start_and_wait_for_refresh(cluster)
            self._validate_all_can_burst(
                    cluster, cursor, vector.query, schema, True)
            self._validate_tables(cluster, schema)

    def test_burst_dml_transfer_to_main_during_refreshing(
            self, cluster, db_session, vector):
        """
        Test: Run DMLs on main cluster for tables owned/unowned by burst
              cluster and commit. Then, run DMLs on these table to ensure both
              read-only and write queries can not burst. After backup and
              refresh, both write and read-only queries can burst.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, schema, vector.sortkey)
            self._start_and_wait_for_refresh(cluster)
            self._init_check(cluster, cursor, schema)
            # Block burst cluster from completing refresh.
            xen_guard = self._create_xen_guard(
                    "check_refresh_status:mark_successful")
            finish_xen_guard = self._create_xen_guard(
                    "check_refresh_status:complete")
            with xen_guard, finish_xen_guard:
                finish_xen_guard.disable()
                self._start_and_wait_for_refresh(cluster, early_return=True)
                xen_guard.wait_until_process_blocks()
                # transfer ownership to main
                cursor.execute("set query_group to metrics;")
                cursor.execute("begin;")
                self._run_query(cursor, 'read-only', 1, 'bw_t1')
                self._check_last_query_didnt_burst(cluster, cursor)
                self._run_query(cursor, 'read-only', 1, 'bw_t2')
                self._check_last_query_didnt_burst(cluster, cursor)
                self._run_query(cursor, 'read-only', 1, 'bw_t3')
                self._check_last_query_didnt_burst(cluster, cursor)
                self._run_query(cursor, 'read-only', 1, 'bw_t4')
                self._check_last_query_didnt_burst(cluster, cursor)

                self._run_query(cursor, vector.query, 1, 'bw_t1')
                self._check_last_query_didnt_burst(cluster, cursor)
                self._validate_ownership_state(
                        schema, 'bw_t1', [('Main', 'Owned')])

                self._run_query(cursor, vector.query, 2, 'bw_t2')
                self._check_last_query_didnt_burst(cluster, cursor)
                self._validate_ownership_state(
                        schema, 'bw_t2', [('Main', 'Owned')])

                self._run_query(cursor, vector.query, 3, 'bw_t3')
                self._check_last_query_didnt_burst(cluster, cursor)
                self._validate_ownership_state(
                        schema, 'bw_t3', [('Main', 'Owned')])

                self._run_query(cursor, vector.query, 4, 'bw_t4')
                self._check_last_query_didnt_burst(cluster, cursor)
                self._validate_ownership_state(
                        schema, 'bw_t4', [('Main', 'Owned')])
                cursor.execute("commit;")
                finish_xen_guard.enable()
                xen_guard.disable()
                finish_xen_guard.wait_until_process_blocks()
                self._validate_ownership_state(schema, 'bw_t1', [])
                self._validate_ownership_state(schema, 'bw_t2', [])
                self._validate_ownership_state(schema, 'bw_t3', [])
                self._validate_ownership_state(schema, 'bw_t4', [])

            cursor.execute("set query_group to burst;")
            self._validate_all_cant_burst(cluster, cursor, vector.query)
            self._start_and_wait_for_refresh(cluster)
            self._validate_all_can_burst(cluster, cursor, vector.query, schema)
            self._validate_tables(cluster, schema)

    def test_burst_dml_transfer_to_main_abort_during_refreshing(
            self, cluster, db_session, vector):
        """
        Test: Run DMLs on main cluster for tables owned/unowned by burst
              cluster and abort. Then, run DMLs on these table to ensure both
              read-only and write queries can not burst. After backup and
              refresh, both write and read-only queries can burst.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, schema, vector.sortkey)
            self._start_and_wait_for_refresh(cluster)
            self._init_check(cluster, cursor, schema)
            # Block burst cluster from completing refresh.
            xen_guard = self._create_xen_guard(
                    "check_refresh_status:mark_successful")
            finish_xen_guard = self._create_xen_guard(
                    "check_refresh_status:complete")
            with xen_guard, finish_xen_guard:
                finish_xen_guard.disable()
                self._start_and_wait_for_refresh(cluster, early_return=True)
                xen_guard.wait_until_process_blocks()
                # transfer ownership to main
                cursor.execute("set query_group to metrics;")
                cursor.execute("begin;")
                self._run_query(cursor, 'read-only', 1, 'bw_t1')
                self._check_last_query_didnt_burst(cluster, cursor)
                self._run_query(cursor, 'read-only', 1, 'bw_t2')
                self._check_last_query_didnt_burst(cluster, cursor)
                self._run_query(cursor, 'read-only', 1, 'bw_t3')
                self._check_last_query_didnt_burst(cluster, cursor)
                self._run_query(cursor, 'read-only', 1, 'bw_t4')
                self._check_last_query_didnt_burst(cluster, cursor)

                self._run_query(cursor, vector.query, 1, 'bw_t1')
                self._check_last_query_didnt_burst(cluster, cursor)
                self._validate_ownership_state(
                        schema, 'bw_t1', [('Main', 'Owned')])

                self._run_query(cursor, vector.query, 2, 'bw_t2')
                self._check_last_query_didnt_burst(cluster, cursor)
                self._validate_ownership_state(
                        schema, 'bw_t2', [('Main', 'Owned')])

                self._run_query(cursor, vector.query, 3, 'bw_t3')
                self._check_last_query_didnt_burst(cluster, cursor)
                self._validate_ownership_state(
                        schema, 'bw_t3', [('Main', 'Owned')])

                self._run_query(cursor, vector.query, 4, 'bw_t4')
                self._check_last_query_didnt_burst(cluster, cursor)
                self._validate_ownership_state(
                        schema, 'bw_t4', [('Main', 'Owned')])
                cursor.execute("abort;")
                self._validate_ownership_state(
                        schema, 'bw_t1', [('Main', 'Undo')])
                self._validate_ownership_state(
                        schema, 'bw_t2', [('Main', 'Undo')])
                self._validate_ownership_state(
                        schema, 'bw_t3', [('Main', 'Undo')])
                self._validate_ownership_state(
                        schema, 'bw_t4', [('Main', 'Undo')])
                finish_xen_guard.enable()
                xen_guard.disable()
                finish_xen_guard.wait_until_process_blocks()

            cursor.execute("set query_group to burst;")
            self._validate_ownership_state(
                    schema, 'bw_t1', [('Main', 'Undo')])
            self._validate_ownership_state(
                    schema, 'bw_t2', [('Main', 'Undo')])
            self._validate_ownership_state(
                    schema, 'bw_t3', [('Main', 'Undo')])
            self._validate_ownership_state(
                    schema, 'bw_t4', [('Main', 'Undo')])
            self._validate_all_cant_burst(cluster, cursor, vector.query)
            self._start_and_wait_for_refresh(cluster)
            self._validate_all_can_burst(cluster, cursor, vector.query, schema)
            self._validate_tables(cluster, schema)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '3',
        'burst_enable_write': 'true',
        'enable_burst_refresh_changed_tables': 'true'
    })
@pytest.mark.custom_local_gucs(
    gucs={
        'enable_burst_refresh_changed_tables': 'true',
        'burst_enable_write_on_skipped_tables_during_refresh': 'true'
    })
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteOnSkippedTablesDuringRefresh(BaseBurstWriteTableRefresh):
    def test_burst_write_on_skipped_tables_during_refresh(
            self, cluster, db_session, vector):
        """
        Test: Run burst write queries to burst cluster during refresh.
              Verify that they can burst if the target table is skipped during
              refresh, and they can't if the target table is not skipped.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, schema, vector.sortkey)
            self._start_and_wait_for_refresh(cluster)
            self._init_check(cluster, cursor, schema)
            # Block burst cluster from completing refresh.
            xen_guard = self._create_xen_guard(
                "check_refresh_status:mark_successful")
            with xen_guard:
                self._start_and_wait_for_refresh(cluster, early_return=True)
                xen_guard.wait_until_process_blocks()
                # table bw_t3 is not owned by burst cluster but also isn't
                # changed since last refresh. When cluster is refreshing
                # it can be bursted.
                self._run_query(cursor, vector.query, 3, 'bw_t3')
                self._validate_ownership_state(schema, 'bw_t3',
                                               [('Burst', 'Owned')])
                self._check_last_query_bursted(cluster, cursor)
                # Change bw_t4 on Main so that it becomes stale to the burst
                # cluster.
                cursor.execute("set query_group to 'noburst';")
                self._run_query(cursor, vector.query, 4, 'bw_t4')
                self._validate_ownership_state(schema, 'bw_t4', [])
                self._check_last_query_didnt_burst(cluster, cursor)
                # table bw_t4 is changed on Main. When cluster is refreshing
                # it can't be bursted.
                cursor.execute("set query_group to burst;")
                self._run_query(cursor, vector.query, 4, 'bw_t4')
                self._validate_ownership_state(schema, 'bw_t4', [])
                self._check_last_query_didnt_burst(cluster, cursor)
