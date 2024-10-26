# Copyright 2020 Amazon.com, Inc. or its affiliates
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

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]


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
class TestBurstWriteTableRefresh(BurstWriteTest):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions({
            "sortkey": ["", "sortkey(c0)", "sortkey(c1, c0)"],
        })

    def _setup_tables(self, db_session, schema, sortkey):
        with db_session.cursor() as cursor:
            t1_def = "create table bw_t1(c0 int, c1 bigint) diststyle even {};"
            t2_def = "create table bw_t2(c0 int, c1 bigint) distkey(c0) {};"
            t3_def = "create table bw_t3(c0 int, c1 bigint) diststyle even {};"
            t4_def = "create table bw_t4(c0 int, c1 bigint) distkey(c1) {};"
            t5_def = "create table bw_t5(c0 int, c1 bigint) distkey(c1) {};"
            cursor.execute(t1_def.format(sortkey))
            cursor.execute(t2_def.format(sortkey))
            cursor.execute(t3_def.format(sortkey))
            cursor.execute(t4_def.format(sortkey))
            cursor.execute(t5_def.format(sortkey))
            cursor.execute("begin;")
            cursor.execute("insert into bw_t1 values(0,0),(1,1),(2,2),(3,3)")
            cursor.execute("insert into bw_t2 values(0,0),(1,1),(2,2),(3,3)")
            cursor.execute("insert into bw_t3 values(0,0),(1,1),(2,2),(3,3)")
            for i in range(16):
                cursor.execute("insert into bw_t1 select * from bw_t1;")
                cursor.execute("insert into bw_t2 select * from bw_t2;")
            cursor.execute("commit;")

    def _init_check(self, cluster, cursor):
        cursor.execute("set query_group to burst;")
        cursor.execute("select sum(c0), sum(c1), count(*) from bw_t1;")
        assert cursor.fetchall() == [(393216, 393216, 262144)]
        self._check_last_query_bursted(cluster, cursor)
        cursor.execute("select sum(c0), sum(c1), count(*) from bw_t2;")
        assert cursor.fetchall() == [(393216, 393216, 262144)]
        self._check_last_query_bursted(cluster, cursor)
        cursor.execute("select sum(c0), sum(c1), count(*) from bw_t3;")
        assert cursor.fetchall() == [(6, 6, 4)]
        self._check_last_query_bursted(cluster, cursor)
        cursor.execute("select sum(c0), sum(c1), count(*) from bw_t4;")
        assert cursor.fetchall() == [(None, None, 0)]
        self._check_last_query_bursted(cluster, cursor)
        # make burst cluster owns bw_t1 and bw_t2
        cursor.execute("insert into bw_t1 values(0,0),(1,1),(2,2),(3,3)")
        self._check_last_query_bursted(cluster, cursor)
        cursor.execute("insert into bw_t2 values(0,0),(1,1),(2,2),(3,3)")
        self._check_last_query_bursted(cluster, cursor)
        # Make bw_t5 stale on burst clusters.
        cursor.execute("set query_group to 'noburst';")
        cursor.execute("insert into bw_t5 values(0,0),(1,1),(2,2),(3,3)")
        self._check_last_query_didnt_burst(cluster, cursor)
        cursor.execute("set query_group to burst;")

    def _validate_tables(self, cluster, schema):
        self._validate_table(cluster, schema, 'bw_t1', 'even')
        self._validate_table(cluster, schema, 'bw_t2', 'distkey')
        self._validate_table(cluster, schema, 'bw_t3', 'even')
        self._validate_table(cluster, schema, 'bw_t4', 'distkey')

    def test_burst_insert_ss_mode(self, cluster, vector):
        """
        Test: Run insert statement when burst cluster is refreshing and
              after burst cluster is refreshed.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            # Prevent insert query bursted in _setup_tables and
            # prevent burst from owning bw_3 and bw_4 table
            # when CBC is on.
            # The condition above can fail L129 and L132 check.
            cursor.execute("set query_group to noburst;")
            self._setup_tables(db_session, schema, vector.sortkey)
            self._start_and_wait_for_refresh(cluster)
            self._init_check(cluster, cursor)
            # Block burst cluster from completing refresh.
            xen_guard = self._create_xen_guard(
                "check_refresh_status:mark_successful")
            with xen_guard:
                self._start_and_wait_for_refresh(cluster,
                                                 throw_on_timeout=False)
                xen_guard.wait_until_process_blocks()
                # Table bw_t1 and bw_t2 are owned by burst cluster. Although the
                # cluster is refreshing, bw_t1 and bw_t2 can still be bursted.
                cursor.execute(
                    "insert into bw_t1 values(0,0),(1,1),(2,2),(3,3)")
                self._check_last_query_bursted(cluster, cursor)
                cursor.execute(
                    "insert into bw_t2 values(0,0),(1,1),(2,2),(3,3)")
                self._check_last_query_bursted(cluster, cursor)
                # table bw_t3 and bw_t4 are not owned by burst cluster and the
                # cluster is refreshing, thus they can not be bursted.
                cursor.execute(
                    "insert into bw_t3 values(0,0),(1,1),(2,2),(3,3)")
                self._check_last_query_didnt_burst(cluster, cursor)
                cursor.execute(
                    "insert into bw_t4 values(0,0),(1,1),(2,2),(3,3)")
                self._check_last_query_didnt_burst(cluster, cursor)

            self._start_and_wait_for_refresh(cluster)
            # After refreshing is completed, all table should be able to burst.
            cursor.execute("insert into bw_t1 values(0,0),(1,1),(2,2),(3,3)")
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("insert into bw_t2 values(0,0),(1,1),(2,2),(3,3)")
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("insert into bw_t3 values(0,0),(1,1),(2,2),(3,3)")
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("insert into bw_t4 values(0,0),(1,1),(2,2),(3,3)")
            self._check_last_query_bursted(cluster, cursor)
            # Check all tables' content.
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t1;")
            assert cursor.fetchall() == [(393234, 393234, 262156)]
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t2;")
            assert cursor.fetchall() == [(393234, 393234, 262156)]
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t3;")
            assert cursor.fetchall() == [(18, 18, 12)]
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t4;")
            assert cursor.fetchall() == [(12, 12, 8)]
            self._check_last_query_bursted(cluster, cursor)
            self._validate_tables(cluster, schema)

    def test_burst_insert_on_dirty_table(self, cluster, vector):
        """
        Test: Run insert statement to dirty table when burst cluster is
              refreshing and after burst cluster is refreshed.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session.session_ctx.schema

        bt_context = SessionContext(user_type='bootstrap')
        bt_conn = cluster.get_conn_params()
        bt_db_session = DbSession(bt_conn, session_ctx=bt_context)
        with db_session.cursor() as cursor, \
                bt_db_session.cursor() as bt_cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, schema, vector.sortkey)
            self._start_and_wait_for_refresh(cluster)
            self._init_check(cluster, cursor)
            # Abort the transaction with burst write to mark table to be dirty
            # on burst cluster.
            cursor.execute("begin;")
            cursor.execute("insert into bw_t2 values(0,0),(1,1),(2,2),(3,3)")
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("abort;")
            # Block burst cluster from completing refresh.
            xen_guard = self._create_xen_guard(
                "check_refresh_status:mark_successful")
            with xen_guard:
                self._start_and_wait_for_refresh(cluster,
                                                 throw_on_timeout=False)
                xen_guard.wait_until_process_blocks()
                # Table bw_t1 is owned by burst cluster. Although the cluster is
                # refreshing, bw_t1 can still be bursted.
                cursor.execute(
                    "insert into bw_t1 values(0,0),(1,1),(2,2),(3,3)")
                self._check_last_query_bursted(cluster, cursor)
                # Table bw_t2 is dirty, thus it can not be bursted.
                cursor.execute(
                    "insert into bw_t2 values(0,0),(1,1),(2,2),(3,3)")
                self._check_last_query_didnt_burst(cluster, cursor)
                # Table bw_t3 and bw_t4 are not owned by burst cluster and the
                # cluster is refreshing, thus they can not be bursted.
                cursor.execute(
                    "insert into bw_t3 values(0,0),(1,1),(2,2),(3,3)")
                self._check_last_query_didnt_burst(cluster, cursor)
                cursor.execute(
                    "insert into bw_t4 values(0,0),(1,1),(2,2),(3,3)")
                self._check_last_query_didnt_burst(cluster, cursor)

            self._start_and_wait_for_refresh(cluster)
            # After refreshing is completed, all table should be able to burst.
            cursor.execute("insert into bw_t1 values(0,0),(1,1),(2,2),(3,3)")
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("insert into bw_t3 values(0,0),(1,1),(2,2),(3,3)")
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("insert into bw_t4 values(0,0),(1,1),(2,2),(3,3)")
            self._check_last_query_bursted(cluster, cursor)
            # Check tables' content.
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t1;")
            assert cursor.fetchall() == [(393234, 393234, 262156)]
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t3;")
            assert cursor.fetchall() == [(18, 18, 12)]
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t4;")
            assert cursor.fetchall() == [(12, 12, 8)]
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("insert into bw_t2 values(0,0),(1,1),(2,2),(3,3)")
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("select sum(c0), sum(c1), count(*) from bw_t2;")
            assert cursor.fetchall() == [(393234, 393234, 262156)]
            self._check_last_query_bursted(cluster, cursor)
            self._validate_tables(cluster, schema)
