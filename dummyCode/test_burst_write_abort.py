# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import time
import uuid

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest
from raff.common.dimensions import Dimensions
from raff.common.db.session import DbSession
from raff.storage.storage_test import create_thread
from psycopg2 import InternalError
from psycopg2.extensions import QueryCanceledError
from psycopg2 import OperationalError

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

INSERT_QUERY = (
    "insert into {}.{} values (5,'Customer#000000005',"
    "'hwBtxkoBF qSW4KrIk5U 2B1AU7H',3,'13-750-942-6364',794.47,'HOUSEHOLD',"
    "'blithely final ins');")
DELETE_QUERY = "DELETE FROM {}.{} where c_custkey = 5"
UPDATE_QUERY = "UPDATE {}.{} SET c_custkey = 10 where c_custkey < 5;"
SELECT_QUERY = ("SELECT c_custkey from {}.{} where c_custkey = 5;")
SNAPSHOT_IDENTIFIER = "burst-init-{}".format(str(uuid.uuid4().hex))


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_burst_gucs(gucs={
    'burst_enable_write': 'true',
    'enable_burst_failure_handling': 'true'
})
class TestBurstWriteTxnAbort(BurstWriteTest):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                guard_pos=[
                    'burst_write:write_lock',
                    'burst:internal_fetch:perform_request',
                    'burst_write:finish_dispatch_headers',
                    'burst_write:send_snap_in', 'burst_write:finish_snap_in'
                ],
                cmd=[
                    'select pg_cancel_backend({})',
                    'select pg_terminate_backend({})'
                ],
                write_query=['insert', 'delete', 'update', 'copy']))

    def _do_burst_write_query(self,
                              cursor,
                              query,
                              schema,
                              cluster,
                              tbl='customer_burst',
                              burst=None):
        try:
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
        except QueryCanceledError as e:
            assert "cancelled" in str(e) or 'canceled' in str(e)
            pass
        except InternalError as e:
            assert "cancelled" in str(e) or 'canceled' in str(e)
            pass
        except OperationalError as e:
            pass
        if burst is True:
            self._check_last_query_bursted(cluster, cursor)
            self._check_ownership_state(schema, tbl, 'Owned')
        elif burst is False:
            self._check_last_query_didnt_burst(cluster, cursor)
            self._check_ownership_state(schema, tbl, None)

    def do_background_cancel(self, schema, cmd, pid):
        with self.db.cursor() as cursor:
            cursor.execute(cmd.format(pid))

    def _execute_write_queries(self,
                               cluster,
                               cursor,
                               schema,
                               tbl,
                               bursted=False):
        cursor.execute("begin;")
        self._do_burst_write_query(
            cursor, 'insert', schema, cluster, tbl, burst=bursted)
        self._do_burst_write_query(
            cursor, 'delete', schema, cluster, tbl, burst=bursted)
        self._do_burst_write_query(
            cursor, 'copy', schema, cluster, tbl, burst=bursted)

    def _get_pid(self, cursor):
        cursor.execute('select pg_real_backend_pid();')
        res = cursor.fetch_scalar()
        return res

    def test_burst_write_txn_abort(self, cluster):
        """
        This test set up 3 tables, first two tables get write query
        and expected to burst before txn abort. After txn abort, any
        write query can not burst on these 2 tables. The last table
        did not get any operations on.
        Verify 3 tables content after.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        # Setup test tables.
        self.execute_test_file(
            "create_burst_write_txn_abort", session=db_session)
        self._start_and_wait_for_refresh(cluster)
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            self._execute_write_queries(cluster, cursor, schema,
                                        'customer_burst_one', True)
            self._execute_write_queries(cluster, cursor, schema,
                                        'customer_burst', True)
            cursor.execute("abort;")
            self._execute_write_queries(cluster, cursor, schema,
                                        'customer_burst')
            self._execute_write_queries(cluster, cursor, schema,
                                        'customer_burst_one')
            cursor.execute("rollback;")
        self._validate_table(cluster, schema, "customer_burst", 'diststyle key')
        self._validate_table(cluster, schema, "customer_burst_one", 'diststyle even')
        self._validate_table(cluster, schema, "dp28554_t1", 'diststyle all')
        self.execute_test_file(
            "verify_burst_write_txn_abort", session=db_session)

    def test_burstwrite_cancel_txn(self, cluster, vector):
        """
        This test set up 3 tables, table customer_burst_one gets normal
        burst write query. Table customer_burst gets various burst write query
        and cancel in different position. make sure no query can burst on
        customer_burst after until refresh
        Verify 3 tables content after.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        # Setup test tables.
        self.execute_test_file(
            "create_burst_write_txn_abort", session=db_session)
        self._start_and_wait_for_refresh(cluster)
        schema = db_session.session_ctx.schema
        pos = vector.guard_pos
        xen_guard = self._create_xen_guard(pos)
        with db_session.cursor() as cursor:
            self._do_burst_write_query(cursor, vector.write_query, schema,
                                       cluster, 'customer_burst_one', True)
            with create_thread(self._do_burst_write_query,
                               (cursor, vector.write_query, schema,
                                'customer_burst')) as thread, xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks()
                vpid = self._get_blocked_vpid(cluster, xen_guard)
                self.do_background_cancel(schema, vector.cmd, vpid)
        self._validate_table(cluster, schema, "customer_burst", 'diststyle key')
        # After abort query, write query can not burst
        with db_session.cursor() as cursor:
            self._do_burst_write_query(
                cursor,
                vector.write_query,
                schema,
                cluster,
                'customer_burst',
                burst=False)
            self._do_burst_write_query(
                cursor,
                vector.write_query,
                schema,
                cluster,
                'customer_burst_one',
                burst=True)
            # new round refresh can burst
            self._start_and_wait_for_refresh(cluster)
            self._do_burst_write_query(
                cursor,
                vector.write_query,
                schema,
                cluster,
                'customer_burst',
                burst=True)
            self._do_burst_write_query(
                cursor,
                vector.write_query,
                schema,
                cluster,
                'customer_burst_one',
                burst=True)
        self._validate_table(cluster, schema, "customer_burst", 'diststyle key')
        self._validate_table(cluster, schema, "customer_burst_one", 'diststyle even')
        self._validate_table(cluster, schema, "dp28554_t1", 'diststyle all')

    def _background_enable_process(self, pid, xen_guard, timeout=30):
        while (timeout > 0):
            xen_guard.process_unblock(pid)
            timeout -= 1
            time.sleep(1)

    @pytest.mark.parametrize(
        "guard_pos",
        ['select:constructor_querydesc', 'burst_write:query_desc_constructor'])
    def test_burst_rw_query_txn_abort(self, cluster, guard_pos):
        """
        This test issues a select query and pauses it before and after
        sb version in querydesc constructed, doing burst write query
        in the background and abort. The select query can not burst
        in either case.
        Validate all tables content after.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        xen_guard = self._create_xen_guard(guard_pos)
        # Setup test tables.
        self.execute_test_file(
            "create_burst_write_txn_abort", session=db_session)
        self._start_and_wait_for_refresh(cluster)
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            with create_thread(self._do_burst_write_query,
                               (cursor, 'select', schema, cluster,
                                'customer_burst', False)) as thread:
                with self.db.cursor() as bootstrap_cursor:
                    pid = self._get_pid(bootstrap_cursor)
                    xen_guard.enable()
                    thread.start()
                    xen_guard.wait_until_process_blocks()
                    with create_thread(self._background_enable_process,
                                       (pid, xen_guard, 20)) as thread_one:
                        # only unblock bootstrap_cursor pid which doing write
                        thread_one.start()
                        # bootstrap write does not burst
                        bootstrap_cursor.execute("begin;")
                        self._do_burst_write_query(bootstrap_cursor, 'insert',
                                                   schema, cluster,
                                                   'customer_burst_one')
                        self._do_burst_write_query(bootstrap_cursor, 'delete',
                                                   schema, cluster,
                                                   'customer_burst')
                        bootstrap_cursor.execute("abort;")
                        xen_guard.disable()
        self._validate_table(cluster, schema, "customer_burst", 'diststyle key')
        self._validate_table(cluster, schema, "customer_burst_one", 'diststyle even')
        self._validate_table(cluster, schema, "dp28554_t1", 'diststyle all')
        self.execute_test_file(
            "verify_burst_write_txn_abort", session=db_session)
