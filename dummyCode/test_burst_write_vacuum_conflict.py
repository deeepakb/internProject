# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import time

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest
from raff.storage.alter_table_suite import AlterTableSuite
from raff.common.dimensions import Dimensions
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.storage.storage_test import create_thread
from psycopg2 import InternalError
from psycopg2.extensions import QueryCanceledError

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

INSERT_QUERY = (
    "insert into {}.{} values (5,'Customer#000000005',"
    "'hwBtxkoBF qSW4KrIk5U 2B1AU7H',3,'13-750-942-6364',794.47,'HOUSEHOLD',"
    "'blithely final ins');")
DELETE_QUERY = "DELETE FROM {}.{} where c_custkey = 5"
UPDATE_QUERY = "UPDATE {}.{} SET c_custkey = 10 where c_custkey < 5;"
SELECT_QUERY = ("SELECT c_custkey from {}.{} where c_custkey = 5;")


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_no_stable_rc
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(gucs={'burst_enable_write': 'true'})
class TestBurstWriteWithVacuum(BurstWriteTest, AlterTableSuite):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                vacuum_cmd=['SORT ONLY', 'DELETE ONLY', ''],
                query=['copy', 'insert', 'delete', 'update']))

    def _do_burst_write_query(self,
                              cursor,
                              query,
                              schema,
                              tbl='customer_burst'):
        cursor.execute('select pg_backend_pid()')
        res = cursor.fetch_scalar()
        log.info("pid: {}".format(res))
        if 'copy' in query:
            cursor.run_copy(
                '{}.{}'.format(schema, tbl),
                's3://tpc-h/1/customer.tbl',
                gzip=True,
                delimiter="|")
        elif 'insert' in query:
            cursor.execute(INSERT_QUERY.format(schema, tbl))
        elif 'delete' in query:
            cursor.execute(DELETE_QUERY.format(schema, tbl))
        elif 'update' in query:
            cursor.execute(UPDATE_QUERY.format(schema, tbl))
        else:
            cursor.execute(SELECT_QUERY.format(schema, tbl))

    def _get_vacuum_query(self, vacuum_cmd, schema, tbl):
        return "VACUUM {} {}.{} to 100 percent".format(vacuum_cmd, schema, tbl)

    def _do_vacuum_partial_without_cancel(self, cursor, schema, tbl):
        cursor.execute('select pg_backend_pid()')
        res = cursor.fetch_scalar()
        log.info("vac pid: {}".format(res))
        cursor.execute("vacuum {}.{};".format(schema, tbl))

    def _do_vacuum_query_without_cancel(self, cursor, vacuum_cmd, schema, tbl):
        cursor.execute(self._get_vacuum_query(vacuum_cmd, schema, tbl))

    def _release_xen_guard(self, guard, time_out=15):
        time.sleep(time_out)
        guard.disable()

    def _do_vacuum_query(self, cursor, vacuum_cmd, schema, tbl):
        try:
            cursor.execute(self._get_vacuum_query(vacuum_cmd, schema, tbl))
        except QueryCanceledError as e:
            assert "cancelled" in str(e) or 'canceled' in str(
                e) or 'abort' in str(e)
            pass
        except InternalError as e:
            assert "cancelled" in str(e) or 'canceled' in str(
                e) or 'abort' in str(e)
            pass
        except InternalError as e:
            assert 'abort' in str(e)
            pass

    def _do_burst_write_query_bg(self,
                                 cluster,
                                 query,
                                 schema,
                                 tbl='customer_burst'):
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor:
            self._do_burst_write_query(cursor, query, schema, tbl)

    def verify_table_content(self, cursor, res):
        cmd = "select count(*), sum(c_custkey), sum(c_nationkey) from customer_burst;"
        cursor.execute(cmd)
        assert cursor.fetchall() == res

    def test_vacuum_wait_burst_write_query(self, cluster, vector):
        """
        This test pause burst query right after it gets wrlock, so that
        when vacuum comes in, vacuum has to wait until burst finishes.
        Vacuum got cancel because of timeout thus makes the table dirty
        Therefore following burst query can not burst.
        For unrelated tables, vacuum does not need to wait.(no lock issue)
        """

        db_session = DbSession(cluster.get_conn_params(user='master'))
        guard_pos = 'burst_write:write_lock'
        # Setup test tables.
        self.execute_test_file("create_burst_write_tables", session=db_session)
        self._start_and_wait_for_refresh(cluster)
        xen_guard = self._create_xen_guard(guard_pos)
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            args = (cursor, vector.query, schema)
            with create_thread(self._do_burst_write_query, args) as thread, \
                    xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks()
                with self.db.cursor() as bootstrap_cursor:
                    try:
                        bootstrap_cursor.execute('select pg_backend_pid()')
                        res = bootstrap_cursor.fetch_scalar()
                        log.info("vac pid: {}".format(res))
                        # background vacuum will be blocked and fail
                        bootstrap_cursor.execute(
                            "set statement_timeout to 10000;")
                        self._do_vacuum_query_without_cancel(
                            bootstrap_cursor, vector.vacuum_cmd, schema,
                            'customer_burst')
                        assert False, "Time out"
                    except InternalError as e:
                        assert 'abort' in str(e)
                        pass
                    except QueryCanceledError:
                        pass
                    # unrelated table should not be affected
                    bootstrap_cursor.execute("reset statement_timeout;")
                    self._do_vacuum_query_without_cancel(
                        bootstrap_cursor, vector.vacuum_cmd, schema,
                        'dp24016_t1')
                xen_guard.disable()
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned')])
            # burst write query bursted.(cursor here is different)
            self._do_vacuum_query_without_cancel(cursor, vector.vacuum_cmd,
                                                 schema, 'customer_burst')
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            # aborted vacuum marked the table dirty, can not burst
            self._do_burst_write_query(cursor, vector.query, schema)
            self._check_last_query_didnt_burst(cluster, cursor)
            self._start_and_wait_for_refresh(cluster)
            self._do_burst_write_query(cursor, vector.query, schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._validate_table(cluster, schema, "customer_burst",
                                 'diststyle auto')

    def _do_bg_vac(self, cluster, vacuum_cmd, schema, tbl):
        with self.db.cursor() as cursor:
            cursor.execute(
                'SET SEARCH_PATH TO "{}", "$user", public;'.format(schema))
            cmd = self._get_vacuum_query(vacuum_cmd, schema, tbl)
            cursor.execute(cmd)

    def test_burst_write_wait_vacuum(self, cluster, vector):
        """
        This test pauses vacuum after getting the wrlock, burst query
        has to wait, and can not burst after vacuum runnning on the target
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        # Setup test tables.
        self.execute_test_file("create_burst_write_tables", session=db_session)
        self._start_and_wait_for_refresh(cluster)
        xen_guard = self._create_xen_guard('vacuum:get_write_lock')
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            args = (cluster, vector.vacuum_cmd, schema, 'customer_burst')
            with create_thread(self._do_bg_vac, args) as thread, xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks()
                with create_thread(self._release_xen_guard,
                                   (xen_guard, 20)) as thread_one:
                    # timeout 15 seconds
                    thread_one.start()
                    try:
                        # background insert will be blocked and fail
                        cursor.execute("set statement_timeout to 10000;")
                        self._do_burst_write_query(cursor, vector.query,
                                                   schema)
                        assert False, "Time out"
                    except InternalError as e:
                        log.info("internal error msg is: {}".format(str(e)))
                        assert 'abort' in str(e)
                        pass
                    except QueryCanceledError:
                        pass
                    cursor.execute("reset statement_timeout;")

            # Vacuum and undo marks the table to be structure changed and
            # undo state.
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Main', 'Structure Changed'),
                                            ('Main', 'Undo')])
            # Next read and write query can not burst.
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_didnt_burst(cluster, cursor)
            self._do_burst_write_query(cursor, vector.query, schema)
            self._check_last_query_didnt_burst(cluster, cursor)
            self._start_and_wait_for_refresh(cluster)
            # After refresh, both read and write query can burst.
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._start_and_wait_for_refresh(cluster)
            self._do_burst_write_query(cursor, vector.query, schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._validate_table(cluster, schema, "customer_burst",
                                 'diststyle auto')

    def test_no_burst_after_vacuum_reset(self, cluster, vector):
        """
        For vacuum, we pause at vacuum:start where we have not reset
        the ownership. Therefore when burst query comes in, it can grab
        necessary locks and complete burst.
        After release vacuum, it resets the ownership, any burst query
        after this can no longer burst.
        Verify burst table afterwards.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        # Setup test tables.
        self.execute_test_file("create_burst_write_tables", session=db_session)
        self._start_and_wait_for_refresh(cluster)
        xen_guard = self._create_xen_guard('vacuum:start')
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            args = (cluster, vector.vacuum_cmd, schema, 'customer_burst')
            with create_thread(self._do_bg_vac, args) as thread, xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks()
                cursor.execute("set query_group to burst;")
                # before thread reset query ownership, burst query can burst
                self._do_burst_write_query(cursor, vector.query, schema)
                self._check_last_query_bursted(cluster, cursor)
                xen_guard.disable()
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._do_burst_write_query(cursor, vector.query, schema)
            self._check_last_query_didnt_burst(cluster, cursor)
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_didnt_burst(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Main', 'Structure Changed')])
            # Backup and refresh cluster.
            self._start_and_wait_for_refresh(cluster)
            # Both read and write query can burst.
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._do_burst_write_query(cursor, vector.query, schema)
            self._check_last_query_bursted(cluster, cursor)
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._validate_table(cluster, schema, "customer_burst",
                                 'diststyle auto')

    @pytest.mark.super_simulated_precommit
    @pytest.mark.parametrize("vacuum", [''])
    def test_sequential_vacuum_undo(self, cluster, vacuum):
        """
        Test sequential vacuum and undo, after both are done, the ownership
        of the table should be [('Main', 'Structure Changed'),
                                                ('Main', 'Undo')]
        Undo shouldn't erase the structure changed status added by vacuum.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session.session_ctx.schema
        session_ctx2 = SessionContext(user_type='bootstrap')
        db_session2 = DbSession(
            cluster.get_conn_params(), session_ctx=session_ctx2)
        # Setup test tables.
        self.execute_test_file("create_burst_write_tables", session=db_session)
        with self.db.cursor() as db_cur:
            db_cur.execute("set search_path to {}".format(schema))
            self.verify_table_content_and_properties(
                db_cur, schema, "customer_burst", [(156, 11392, 1709)],
                'diststyle auto')

        self._start_and_wait_for_refresh(cluster)
        with db_session.cursor() as cursor, db_session2.cursor() as cursor2:
            cursor2.execute("set search_path to {}".format(schema))
            self._do_vacuum_query_without_cancel(cursor, vacuum, schema,
                                                 'customer_burst')
            # vacuum query does not burst
            self._check_last_query_didnt_burst(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Main', 'Structure Changed')])
            # conduct an undo
            cursor.execute("set query_group to metrics;")
            cursor.execute("begin;")

            cursor.execute(INSERT_QUERY.format(schema, 'customer_burst'))
            cursor.execute("abort;")
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Main', 'Structure Changed'),
                                            ('Main', 'Undo')])

            self.verify_table_content_and_properties(
                cursor2, schema, "customer_burst", [(156, 11392, 1709)],
                'diststyle auto')
            # Back and refresh cluster
            self._start_and_wait_for_refresh(cluster)
            # Both read and write can burst.
            cursor.execute("set query_group to burst;")
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._do_burst_write_query(cursor, 'delete', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self.verify_table_content_and_properties(
                cursor2, schema, "customer_burst", [(154, 11382, 1703)],
                'diststyle auto')

    @pytest.mark.super_simulated_precommit
    def test_burst_write_concurrent_vacuum_undo(self, cluster):
        """
        This test pauses vacuum before incremental_sort, at this stage,
        the vacuum has released inslock and dellock after InitVacuum, and
        dml can conduct on the same target table. We issue an undo while
        pausing vacuum at this stage, undo shouldn't erase the structure
        changed status added by vacuum.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        session_ctx2 = SessionContext(user_type='bootstrap')
        db_session2 = DbSession(
            cluster.get_conn_params(), session_ctx=session_ctx2)
        schema = db_session.session_ctx.schema
        # Setup test tables.
        self.execute_test_file("create_burst_write_tables", session=db_session)
        self._start_and_wait_for_refresh(cluster)
        xen_guard = self._create_xen_guard('vacuum:begin_incremental_sort')

        with db_session.cursor() as cursor, db_session2.cursor() as cursor2:
            cursor2.execute("set search_path to {}".format(schema))
            cursor.execute('set query_group to burst;')
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(INSERT_QUERY.format(schema, 'customer_burst'))
            self._check_last_query_bursted(cluster, cursor)

            with create_thread(
                    self._do_vacuum_query,
                (cursor2, '', schema, 'customer_burst')) as thread, xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks()

                # Conduct undo
                cursor.execute("set query_group to metrics;")
                cursor.execute("begin;")
                cursor.execute(INSERT_QUERY.format(schema, 'customer_burst'))
                cursor.execute("abort;")
                self._validate_ownership_state(schema, 'customer_burst',
                                               [('Main', 'Structure Changed'),
                                                ('Main', 'Undo')])
                # Refresh when the vacuum is still running
                self._start_and_wait_for_refresh(cluster)
                # After the refresh, the write query cannot be burst, as
                # vacuum is still running on the same table.
                cursor.execute('set query_group to burst;')
                cursor.execute(INSERT_QUERY.format(schema, 'customer_burst'))
                self._check_last_query_didnt_burst(cluster, cursor)

            self.verify_table_content_and_properties(
                cursor2, schema, "customer_burst", [(158, 11402, 1715)],
                'diststyle auto')
            # Back and refresh cluster
            self._start_and_wait_for_refresh(cluster)
            # Both read and write can burst.
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._do_burst_write_query(cursor, 'delete', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])

            self.verify_table_content_and_properties(
                cursor2, schema, "customer_burst", [(154, 11382, 1703)],
                'diststyle auto')

    @pytest.mark.parametrize("vacuum", ['SORT ONLY', 'DELETE ONLY', ''])
    def test_read_query_no_block_vacuum(self, cluster, vacuum):
        """
        This test issues read query and then vacuum on it, make sure read query
        does not block vacuum, after vacuum, no read and write can be bursted.
        After refresh, read and write should burst.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        # Setup test tables.
        self.execute_test_file("create_burst_write_tables", session=db_session)
        self._start_and_wait_for_refresh(cluster)
        xen_guard = self._create_xen_guard('burst_write:write_lock')
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            args = (cursor, 'select', schema)
            with create_thread(self._do_burst_write_query, args) as thread, \
                    xen_guard, self.db.cursor() as bootstrap_cursor:
                thread.start()
                xen_guard.wait_until_process_blocks()
                bootstrap_cursor.execute("set query_group to burst;")
                # vacuum can still pass even select query has write lock
                # NO blocking here
                self._do_vacuum_query_without_cancel(bootstrap_cursor, vacuum,
                                                     schema, 'customer_burst')
                xen_guard.disable()
            # The query should be bursted.
            self._check_last_query_bursted(cluster, cursor)
            # Table should be in 'Structure Changed'.
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Main', 'Structure Changed')])
            self._do_vacuum_query_without_cancel(cursor, vacuum, schema,
                                                 'customer_burst')
            # Vacuum query does not burst
            self._check_last_query_didnt_burst(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Main', 'Structure Changed')])
            # Read query can still burst
            cursor.execute("select count(*) from customer_burst;")
            self._check_last_query_bursted(cluster, cursor)
            # Insert query after vacuum does not burst.
            self._do_burst_write_query(cursor, 'insert', schema)
            self._check_last_query_didnt_burst(cluster, cursor)
            self._validate_table(cluster, schema, "customer_burst",
                                 'diststyle auto')
            # Back and refresh cluster
            self._start_and_wait_for_refresh(cluster)
            # Both read and write can burst.
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._do_burst_write_query(cursor, 'delete', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._validate_table(cluster, schema, "customer_burst",
                                 'diststyle auto')

    @pytest.mark.parametrize("vacuum", ['SORT ONLY', 'DELETE ONLY', ''])
    def test_read_query_interleave_vacuum(self, cluster, vacuum):
        """
        This test squentially issues read(burst), vacuum(no burst),
        read(no burst), write(no burst)
        and then refresh, after the refresh both read and write can burst.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        guard_pos = 'burst_write:write_lock'
        # Setup test tables.
        self.execute_test_file("create_burst_write_tables", session=db_session)
        self._start_and_wait_for_refresh(cluster)
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst', [])
            self._do_vacuum_query_without_cancel(cursor, vacuum, schema,
                                                 'customer_burst')
            # vacuum query does not burst
            self._check_last_query_didnt_burst(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Main', 'Structure Changed')])
            # read query after vacuum can still burst
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Main', 'Structure Changed')])
            # insert after vacuum query does not burst
            self._do_burst_write_query(cursor, 'insert', schema)
            self._check_last_query_didnt_burst(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Main', 'Structure Changed')])
            self._validate_table(cluster, schema, "customer_burst",
                                 'diststyle auto')
            # new round fresh
            self._start_and_wait_for_refresh(cluster)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Main', 'Structure Changed')])
            # dummy commit does not affect structure change
            cursor.execute("create table t1(c0 int);")
            cursor.execute("create table t2(c0 int);")
            self._do_burst_write_query(cursor, 'delete', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._validate_table(cluster, schema, "customer_burst",
                                 'diststyle auto')

    def _get_pid(self, cursor):
        cursor.execute('select pg_backend_pid()')
        res = cursor.fetch_scalar()
        return res

    def do_background_cancel(self, schema, cmd, pid):
        with self.db.cursor() as cursor:
            cursor.execute(cmd.format(pid))

    @pytest.mark.parametrize("guard",
                             ['vacuum:prepare', 'vacuum:get_write_lock'])
    def test_bg_cancel_vacuum_with_burst(self, cluster, guard):
        """
        This test runs a read query first: can burst
        1. run vacuum on table then cancel
        2. read query can always burst after
        3. before vacuum mark structure change, read and write query can burst
        4. after vacuum mark structure change, only read burst
        5. after refresh, read and write can burst
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        xen_guard = self._create_xen_guard(guard)
        # Setup test tables.
        self.execute_test_file("create_burst_write_tables", session=db_session)
        self._start_and_wait_for_refresh(cluster)
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            pid = self._get_pid(cursor)
            with create_thread(
                    self._do_vacuum_query,
                (cursor, '', schema, 'customer_burst')) as thread, xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks()
                self.do_background_cancel(schema,
                                          'select pg_cancel_backend({})', pid)
            # read query can burst
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            # write query after vacuum does not burst
            self._do_burst_write_query(cursor, 'insert', schema)
            if 'prepare' in guard:
                # did not make structure change yet nor write lock
                self._check_last_query_bursted(cluster, cursor)
                self._validate_ownership_state(schema, 'customer_burst',
                                               [('Burst', 'Owned')])
            else:
                self._check_last_query_didnt_burst(cluster, cursor)
                self._validate_ownership_state(schema, 'customer_burst',
                                               [('Main', 'Structure Changed')])
            self._validate_table(cluster, schema, "customer_burst",
                                 'diststyle auto')
            # new round fresh
            self._start_and_wait_for_refresh(cluster)
            self._do_burst_write_query(cursor, 'delete', schema)
            self._check_last_query_bursted(cluster, cursor)
            if 'prepare' in guard:
                self._validate_ownership_state(schema, 'customer_burst',
                                               [('Burst', 'Owned')])
            else:
                self._validate_ownership_state(schema, 'customer_burst',
                                               [('Burst', 'Owned'),
                                                ('Main', 'Structure Changed')])
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._do_burst_write_query(cursor, 'copy', schema)
            self._check_last_query_bursted(cluster, cursor)
            if 'prepare' in guard:
                self._validate_ownership_state(schema, 'customer_burst',
                                               [('Burst', 'Owned')])
            else:
                self._validate_ownership_state(schema, 'customer_burst',
                                               [('Burst', 'Owned'),
                                                ('Main', 'Structure Changed')])
            self._validate_table(cluster, schema, "customer_burst",
                                 'diststyle auto')

    @pytest.mark.super_simulated_precommit
    @pytest.mark.parametrize("vacuum", ['SORT ONLY', 'DELETE ONLY', ''])
    def test_burst_write_vacuum_sequential_commit(self, cluster, vacuum):
        """
        In a transaction, writes table on burst cluster and commits.
        Runs vacuum on the table:
        a. The subsequent read-only and write query can not burst.
        b. After backup and refresh, both read-only and write query can burst.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        # Setup test tables.
        self.execute_test_file("create_burst_write_tables", session=db_session)
        self._start_and_wait_for_refresh(cluster)
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute('begin')
            self._do_burst_write_query(cursor, 'insert', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned')])
            self._do_burst_write_query(cursor, 'delete', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned')])
            self._do_burst_write_query(cursor, 'update', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned')])
            cursor.execute('commit')

            self._do_vacuum_query_without_cancel(cursor, vacuum, schema,
                                                 'customer_burst')
            # vacuum query does not burst
            self._check_last_query_didnt_burst(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._do_burst_write_query(cursor, 'select', schema)
            # if table owned by cluster then ready query can burst after vacuum
            self._check_last_query_bursted(cluster, cursor)
            # insert after vacuum query does not burst
            self._do_burst_write_query(cursor, 'insert', schema)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Main', 'Structure Changed')])
            self._check_last_query_didnt_burst(cluster, cursor)
            self._validate_table(cluster, schema, "customer_burst",
                                 'diststyle auto')
            # backup and refresh
            self._start_and_wait_for_refresh(cluster)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Main', 'Structure Changed')])
            self._do_burst_write_query(cursor, 'delete', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._do_burst_write_query(cursor, 'update', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._validate_table(cluster, schema, "customer_burst",
                                 'diststyle auto')
            with self.db.cursor() as cursor:
                self.verify_table_content_and_properties(
                    cursor, schema, "customer_burst", [(154, 11430, 1703)],
                    'diststyle auto')

    @pytest.mark.super_simulated_precommit
    @pytest.mark.parametrize("vacuum", ['SORT ONLY', 'DELETE ONLY', ''])
    def test_burst_write_vacuum_sequential_commit_immediate_refresh(
            self, cluster, vacuum):
        """
        In a transaction, writes table on burst cluster and commits.
        Runs vacuum on the table. Immediately takes backup and refresh cluster.
        The subsequent read-only and write query can burst.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        # Setup test tables.
        self.execute_test_file("create_burst_write_tables", session=db_session)
        self._start_and_wait_for_refresh(cluster)
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute('begin')
            self._do_burst_write_query(cursor, 'insert', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned')])
            self._do_burst_write_query(cursor, 'delete', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned')])
            self._do_burst_write_query(cursor, 'update', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned')])
            cursor.execute('commit')
            self._do_vacuum_query_without_cancel(cursor, vacuum, schema,
                                                 'customer_burst')
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            # backup and refresh
            self._start_and_wait_for_refresh(cluster)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._do_burst_write_query(cursor, 'insert', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._do_burst_write_query(cursor, 'delete', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._do_burst_write_query(cursor, 'update', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._validate_table(cluster, schema, "customer_burst",
                                 'diststyle auto')
            with self.db.cursor() as cursor:
                self.verify_table_content_and_properties(
                    cursor, schema, "customer_burst", [(154, 11430, 1703)],
                    'diststyle auto')

    @pytest.mark.super_simulated_precommit
    @pytest.mark.parametrize("vacuum", ['SORT ONLY', 'DELETE ONLY', ''])
    def test_burst_write_vacuum_sequential_abort(self, cluster, vacuum):
        """
        In a transaction, writes table on burst cluster and aborts.
        Runs vacuum on the table:
        a. The subsequent read-only and write query can not burst.
        b. After backup and refresh, both read-only and write query can burst.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        # Setup test tables.
        self.execute_test_file("create_burst_write_tables", session=db_session)
        self._start_and_wait_for_refresh(cluster)
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute('begin')
            self._do_burst_write_query(cursor, 'insert', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned')])
            self._do_burst_write_query(cursor, 'delete', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned')])
            self._do_burst_write_query(cursor, 'update', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned')])
            cursor.execute('abort')

            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Main', 'Undo')])
            self._do_vacuum_query_without_cancel(cursor, vacuum, schema,
                                                 'customer_burst')
            # vacuum query does not burst
            self._check_last_query_didnt_burst(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Main', 'Structure Changed'),
                                            ('Main', 'Undo')])
            # read query cannot be bursted
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_didnt_burst(cluster, cursor)
            # write after vacuum query does not burst
            self._do_burst_write_query(cursor, 'insert', schema)
            self._check_last_query_didnt_burst(cluster, cursor)
            self._do_burst_write_query(cursor, 'delete', schema)
            self._check_last_query_didnt_burst(cluster, cursor)
            self._do_burst_write_query(cursor, 'update', schema)
            self._check_last_query_didnt_burst(cluster, cursor)
            self._validate_table(cluster, schema, "customer_burst",
                                 'diststyle auto')
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Main', 'Structure Changed'),
                                            ('Main', 'Undo')])
            # backup and refresh
            self._start_and_wait_for_refresh(cluster)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Main', 'Structure Changed')])
            self._do_burst_write_query(cursor, 'insert', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._do_burst_write_query(cursor, 'delete', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._do_burst_write_query(cursor, 'update', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._validate_table(cluster, schema, "customer_burst",
                                 'diststyle auto')
            with self.db.cursor() as cursor:
                self.verify_table_content_and_properties(
                    cursor, schema, "customer_burst", [(154, 11430, 1703)],
                    'diststyle auto')

    @pytest.mark.super_simulated_precommit
    @pytest.mark.parametrize("vacuum", ['SORT ONLY', 'DELETE ONLY', ''])
    def test_burst_write_vacuum_sequential_abort_immediate_refresh(
            self, cluster, vacuum):
        """
        In a transaction, writes table on burst cluster and aborts.
        Runs vacuum on the table. Immediately takes backup and refresh cluster.
        The subsequent read-only and write query can burst.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        # Setup test tables.
        self.execute_test_file("create_burst_write_tables", session=db_session)
        self._start_and_wait_for_refresh(cluster)
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute('begin')
            self._do_burst_write_query(cursor, 'insert', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned')])
            self._do_burst_write_query(cursor, 'delete', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned')])
            self._do_burst_write_query(cursor, 'update', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned')])
            cursor.execute('abort')
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Main', 'Undo')])
            self._do_vacuum_query_without_cancel(cursor, vacuum, schema,
                                                 'customer_burst')
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Main', 'Structure Changed'),
                                            ('Main', 'Undo')])
            # backup and refresh
            self._start_and_wait_for_refresh(cluster)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Main', 'Structure Changed')])
            self._do_burst_write_query(cursor, 'delete', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._do_burst_write_query(cursor, 'update', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._do_burst_write_query(cursor, 'insert', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._validate_table(cluster, schema, "customer_burst",
                                 'diststyle auto')
            with self.db.cursor() as cursor:
                self.verify_table_content_and_properties(
                    cursor, schema, "customer_burst", [(155, 11435, 1706)],
                    'diststyle auto')

    @pytest.mark.super_simulated_precommit
    def test_burst_write_vacuum_internal_commit(self, cluster):
        """
        set up a table and do burst write query, pause vacuum after an internal
        commit. start refresh with pausing the vacuum.
        burst write query t1
        vacuum t1 txn 100
          internal commit 101
          start refresh
          internal commit 102..
        let vacuum finish
        read can burst, write can not
        start refresh
        all can burst.
        """
        xen_guard = self.create_xen_guard("vacuum:internal_commit")
        db_session = DbSession(cluster.get_conn_params(user='master'))
        # Setup test tables. enable xen_guard to pause after
        self.execute_test_file("create_burst_write_tables", session=db_session)
        self._start_and_wait_for_refresh(cluster)
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            self._do_burst_write_query(cursor, 'insert', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned')])
            self._do_burst_write_query(cursor, 'delete', schema)
            with create_thread(
                    self._do_vacuum_partial_without_cancel,
                (cursor, schema, 'customer_burst')) as thread, xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks()
                # pause at internal commit and let refresh going
                # Refresh in the middle of internal commit
                self._start_and_wait_for_refresh(cluster)
            self._validate_ownership_state(schema, 'customer_burst',
                                           [('Burst', 'Owned'),
                                            ('Main', 'Structure Changed')])
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._do_burst_write_query(cursor, 'update', schema)
            self._check_last_query_didnt_burst(cluster, cursor)
            self._do_burst_write_query(cursor, 'copy', schema)
            self._check_last_query_didnt_burst(cluster, cursor)
            self._do_burst_write_query(cursor, 'insert', schema)
            self._check_last_query_didnt_burst(cluster, cursor)
            self._do_burst_write_query(cursor, 'delete', schema)
            self._check_last_query_didnt_burst(cluster, cursor)
            # new round
            self._start_and_wait_for_refresh(cluster)
            self._do_burst_write_query(cursor, 'select', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._do_burst_write_query(cursor, 'update', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._do_burst_write_query(cursor, 'copy', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._do_burst_write_query(cursor, 'insert', schema)
            self._check_last_query_bursted(cluster, cursor)
            self._do_burst_write_query(cursor, 'delete', schema)
            self._check_last_query_bursted(cluster, cursor)
