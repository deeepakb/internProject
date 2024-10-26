# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
import time
import uuid

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.dimensions import Dimensions
from raff.common.db.session import DbSession
from raff.storage.storage_test import create_thread
from raff.common.db.session_context import SessionContext
from psycopg2 import InternalError
from psycopg2.extensions import QueryCanceledError
from psycopg2 import OperationalError
from raff.burst.burst_temp_write import burst_user_temp_support_gucs, BurstTempWrite

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

DROP_TABLE = "DROP TABLE IF EXISTS {schema}.{table};"
INSERT_QUERY = (
    "insert into {schema}.{table} select * from {schema}.{table} where c0 > 10;"
)
INSERT_QUERY_ALL = (
    "insert into {schema}.{table} select * from {schema}.{table};")
DELETE_QUERY = "DELETE FROM {schema}.{table} where c0 > 10"
UPDATE_QUERY = "UPDATE {schema}.{table} SET c0 = 10 where c0 > 10;"
SELECT_QUERY = ("SELECT c0 from {schema}.{table} where c0 = 10;")
INSERT_QUERY_OP = (
    "insert into {schema}.{table} select * from {schema}.{table} where c0 < 5;"
)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.ssm_perm_or_temp_config
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.super_simulated_no_stable_rc
@pytest.mark.custom_burst_gucs(gucs=dict(list(burst_user_temp_support_gucs.items()) +
                                         [('burst_enable_write', 'true')]))
class TestBurstWriteNoOpQuery(BurstTempWrite):
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
                write_query=['insert', 'delete', 'update']))

    def test_burst_write_noop_query(self, cluster, vector, is_temp):
        """
        basic testing for no-op query, here insert/delete/update/select
        are all no-op queries, make sure they all bursted
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        # Setup test tables.
        setup_file = "create_burst_write_noop" if not is_temp \
            else "create_burst_write_temp_noop"
        self.execute_test_file(setup_file, session=db_session)
        self._start_and_wait_for_refresh(cluster)
        with db_session.cursor() as cursor:
            schema1 = self._get_temp_table_schema(cursor, "dp28555_t1") if is_temp \
                else db_session.session_ctx.schema
            schema2 = self._get_temp_table_schema(cursor, "dp28555_t2") if is_temp \
                else db_session.session_ctx.schema
            cursor.execute(
                INSERT_QUERY.format(schema=schema1, table="dp28555_t1"))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(
                DELETE_QUERY.format(schema=schema1, table="dp28555_t1"))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(
                UPDATE_QUERY.format(schema=schema1, table="dp28555_t1"))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(
                SELECT_QUERY.format(schema=schema1, table="dp28555_t1"))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(
                INSERT_QUERY.format(schema=schema2, table="dp28555_t2"))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(
                DELETE_QUERY.format(schema=schema2, table="dp28555_t2"))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(
                UPDATE_QUERY.format(schema=schema2, table="dp28555_t2"))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(
                SELECT_QUERY.format(schema=schema2, table="dp28555_t2"))
            self._check_last_query_bursted(cluster, cursor)
        self._validate_table(cluster, schema1, "dp28555_t1", 'diststyle even')
        self._validate_table(cluster, schema2, "dp28555_t2", 'diststyle key')
        self.execute_test_file(
            "test_burst_write_noop_verify", session=db_session)

    def _do_burst_write_query(self, cursor, query, schema, tbl='dp28555_t1'):
        try:
            if 'insert' in query:
                cursor.execute(INSERT_QUERY.format(schema=schema, table=tbl))
            elif 'delete' in query:
                cursor.execute(DELETE_QUERY.format(schema=schema, table=tbl))
            elif 'update' in query:
                cursor.execute(UPDATE_QUERY.format(schema=schema, table=tbl))
            else:
                cursor.execute(SELECT_QUERY.format(schema=schema, table=tbl))
        except QueryCanceledError as e:
            assert "cancelled" in str(e) or 'canceled' in str(e)
            pass
        except InternalError as e:
            assert "cancelled" in str(e) or 'canceled' in str(e)
            pass
        except OperationalError as e:
            pass

    def _get_pid(self, cursor):
        cursor.execute('select pg_backend_pid()')
        res = cursor.fetch_scalar()
        return res

    def test_noop_query_negative(self, cluster, vector, is_temp):
        """
        Pause no-op write query at various place, cancel query in the bg
        make sure all other read/write(op or no-op) can not burst
        after new around of refresh, all query can burst.
        """
        if is_temp and vector.cmd == "select pg_terminate_backend({})":
            pytest.skip("Temp tables are wiped out when we terminate the \
                        backend; unsupported test dimension")
        schema = 'test_schema'

        session_ctx = SessionContext(user_type='super', schema=schema)
        db_session = DbSession(cluster.get_conn_params(),
                               session_ctx=session_ctx)
        with db_session.cursor() as cursor:
            # We need to ensure that no tables are left over from previous iterations,
            # perm and temp tables need to be dropped if applicable
            cursor.execute(
                "SELECT RELNAMESPACE FROM PG_CLASS WHERE RELNAME = \'{}\'".format(
                    "dp28555_t1"))
            log.info(cursor.fetchall())
            cursor.execute(
                DROP_TABLE.format(schema=schema, table="dp28555_t1"))
            cursor.execute(
                DROP_TABLE.format(schema=schema, table="dp28555_t2"))
        # Setup test tables.
        setup_file = "create_burst_write_noop" if not is_temp \
            else "create_burst_write_temp_noop"
        self.execute_test_file(setup_file, session=db_session)
        self._start_and_wait_for_refresh(cluster)
        pos = vector.guard_pos
        xen_guard = self._create_xen_guard(pos)
        with db_session.cursor() as cursor:
            schema1 = self._get_temp_table_schema(cursor, "dp28555_t1") if is_temp \
                else db_session.session_ctx.schema
            schema2 = self._get_temp_table_schema(cursor, "dp28555_t2") if is_temp \
                else db_session.session_ctx.schema
            pid = self._get_pid(cursor)
            cursor.execute(
                DELETE_QUERY.format(schema=schema1, table="dp28555_t1"))
            self._check_last_query_bursted(cluster, cursor)
            with create_thread(
                    self._do_burst_write_query,
                (cursor, vector.write_query, schema1)) as thread, xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks()
                with self.db.cursor() as cursor_one:
                    cursor_one.execute(vector.cmd.format(pid))
        self._validate_table(cluster, schema1, "dp28555_t1", 'diststyle even')
        with db_session.cursor() as cursor:
            # operation can not burst after abort on perm table
            schema1 = self._get_temp_table_schema(cursor, "dp28555_t1") if is_temp \
                else db_session.session_ctx.schema
            schema2 = self._get_temp_table_schema(cursor, "dp28555_t2") if is_temp \
                else db_session.session_ctx.schema
            cursor.execute(
                INSERT_QUERY_OP.format(schema=schema1, table="dp28555_t1"))
            if is_temp:
                self._check_last_query_bursted(cluster, cursor)
            else:
                self._check_last_query_didnt_burst(cluster, cursor)
            cursor.execute(
                DELETE_QUERY.format(schema=schema2, table="dp28555_t2"))
            self._check_last_query_bursted(cluster, cursor)
            pid = self._get_pid(cursor)
            with create_thread(self._do_burst_write_query,
                               (cursor, vector.write_query, schema2,
                                'dp28555_t2')) as thread, xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks()
                with self.db.cursor() as cursor_one:
                    cursor_one.execute(vector.cmd.format(pid))
        if vector.cmd == 'select pg_terminate_backend({})':
            db_session = DbSession(cluster.get_conn_params(),
                                   session_ctx=session_ctx)
        with db_session.cursor() as cursor:
            schema1 = self._get_temp_table_schema(cursor, "dp28555_t1") if is_temp \
                else db_session.session_ctx.schema
            schema2 = self._get_temp_table_schema(cursor, "dp28555_t2") if is_temp \
                else db_session.session_ctx.schema
            # operation can not burst after abort
            cursor.execute(
                INSERT_QUERY_OP.format(schema=schema2, table="dp28555_t2"))
            if is_temp:
                self._check_last_query_bursted(cluster, cursor)
            else:
                self._check_last_query_didnt_burst(cluster, cursor)
        self._validate_table(cluster, schema2, "dp28555_t2", 'diststyle key')
        with db_session.cursor() as cursor:
            # New round refresh
            self._start_and_wait_for_refresh(cluster)
            schema1 = self._get_temp_table_schema(cursor, "dp28555_t1") if is_temp \
                else db_session.session_ctx.schema
            schema2 = self._get_temp_table_schema(cursor, "dp28555_t2") if is_temp \
                else db_session.session_ctx.schema
            # operation insert can burst
            cursor.execute(
                INSERT_QUERY_OP.format(schema=schema1, table="dp28555_t1"))
            #  no-op can burst
            self._check_last_query_bursted(cluster, cursor)
            # operation insert can burst
            cursor.execute(
                INSERT_QUERY_OP.format(schema=schema2, table="dp28555_t2"))
            #  no-op can burst
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(
                DELETE_QUERY.format(schema=schema2, table="dp28555_t2"))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(
                UPDATE_QUERY.format(schema=schema1, table="dp28555_t1"))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(
                SELECT_QUERY.format(schema=schema1, table="dp28555_t1"))
            self._check_last_query_bursted(cluster, cursor)
            self._validate_table(cluster, schema1, "dp28555_t1", 'diststyle even')
            self._validate_table(cluster, schema2, "dp28555_t2", 'diststyle key')
            self.execute_test_file(
                "create_burst_write_after_dml", session=db_session)
            # Clean-up all the tables
            cursor.execute(
                DROP_TABLE.format(schema=schema1, table="dp28555_t1"))
            cursor.execute(
                DROP_TABLE.format(schema=schema2, table="dp28555_t2"))
