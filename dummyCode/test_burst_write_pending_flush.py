# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import time
import pytest

from raff.burst.burst_super_simulated_mode_helper import \
    super_simulated_mode, get_burst_conn_params, touch_spinfile, rm_spinfile
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.redshift_db import RedshiftDb
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.storage.storage_test import create_thread

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

CREATE = ("create table {}.dp32120(c0 bigint, c1 int, "
          "c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, "
          "c10 int, c11 int, c12 int, c13 int, c14 int, c15 int, c16 int, "
          "c17 int, c18 int, c19 int, c20 int, c21 int, c22 int, c23 int, "
          "c24 int, c25 int, c26 int, c27 int, c28 int, c29 int, c30 int, "
          "c31 int, c32 int, c33 int, c34 int, c35 int, c36 int, c37 int, "
          "c38 int, c39 int, c40 int, c41 int, c42 int, c43 int, c44 int, "
          "c45 int, c46 int, c47 int, c48 int, c49 int, c50 int, c51 int, "
          "c52 int, c53 int, c54 int, c55 int, c56 int, c57 int, c58 int, "
          "c59 int) diststyle even;")

INSERT = (
    "insert into {}.dp32120 values"
    "(1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1),"
    "(2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2),"
    "(3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3),"
    "(4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4),"
    "(5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5),"
    "(6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6),"
    "(7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7),"
    "(8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8),"
    "(9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9),"
    "(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);"
)

INSERT_SELECT = "insert into {0}.dp32120 select * from {0}.dp32120;"

SELECT = (
    "select max(c0), max(c1), max(c2), max(c3), max(c4), max(c5), "
    "max(c6), max(c7), max(c8), max(c9), max(c10), max(c11), max(c12), "
    "max(c13), max(c14), max(c15), max(c16), max(c17), max(c18), max(c19)"
    " from {}.dp32120;")

RESULT = [(9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9)]

MY_SCHEMA = "test_schema"

MY_USER = "test_user"


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_no_stable_rc
@pytest.mark.custom_burst_gucs(
    gucs={
        'burst_enable_write': True,
        'skew_row_threshold': 0,
        'rr_dist_num_rows': 1,
        'ingestion_delay_block_allocs': False,
        'gconf_disk_cache_size': 100,
        'slices_per_node': 4
    })
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.skip(reason="DP-37227")
class TestBurstWritePendingFlush(BurstWriteTest):
    def do_background(self, stmt, cursor):
        cursor.execute(stmt)

    def test_burst_cluster_pending_flush(self, cluster):
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        main_write_session = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))
        main_read_session = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))
        with main_write_session.cursor() as write_cursor, \
                main_read_session.cursor() as read_cursor, \
                burst_session.cursor() as burst_cursor:
            schema = main_read_session.session_ctx.schema
            insert_stmt = INSERT.format(schema)
            insert_select = INSERT_SELECT.format(schema)
            select_stmt = SELECT.format(schema)

            write_cursor.execute("begin;")
            write_cursor.execute(CREATE.format(schema))
            write_cursor.execute(insert_stmt)
            for i in range(6):
                write_cursor.execute(insert_select)
            write_cursor.execute("commit;")

            self._start_and_wait_for_refresh(cluster)
            write_cursor.execute("set query_group to burst;")
            write_cursor.execute(insert_select)
            self._check_last_query_bursted(cluster, write_cursor)

            read_cursor.execute("set query_group to burst;")
            read_cursor.execute(select_stmt)
            result = read_cursor.fetchall()
            assert result == RESULT
            self._check_last_query_bursted(cluster, read_cursor)

            # Pause insert statement after block copy
            write_args = (insert_select, write_cursor)
            read_args = (select_stmt, read_cursor)
            burst_cursor.execute("xpx 'event set EtTossCacheSkipWait'")
            expected = 0
            for i in range(4):
                write_cursor.execute("set query_group to burst;")
                read_cursor.execute("set query_group to burst;")
                with create_thread(self.do_background, write_args) as write_thread, \
                        create_thread(self.do_background, read_args) as read_thread:
                    touch_spinfile('block_copy_pause')
                    write_thread.start()
                    # Wait for burst cluster to process and paused on block copy
                    time.sleep(20)
                    # Toss cache and delay the write IO handling.
                    touch_spinfile('delay_handle_write')
                    burst_cursor.execute("xpx 'toss_cache'")
                    read_thread.start()
                    # Unpause write query and allow block io filter to process write
                    # request.
                    rm_spinfile("block_copy_pause")
                    time.sleep(20)
                    rm_spinfile("delay_handle_write")
                    write_thread.join()
                    read_thread.join()
                self._check_last_query_bursted(cluster, write_cursor)
                self._check_last_query_bursted(cluster, read_cursor)
                # Check query in main cluster is correct.
                read_cursor.execute("set query_group to metrics;")
                read_cursor.execute(select_stmt)
                assert read_cursor.fetchall() == RESULT
                # Check the pending write is handled.
                burst_cursor.execute(
                    "select count(*) from stl_print where message ilike 'Flush%';"
                )
                result = burst_cursor.fetch_scalar()
                assert result > expected
                expected = result

            # Unpause write IO handling.
            burst_cursor.execute("xpx 'event unset EtTossCacheSkipWait'")
