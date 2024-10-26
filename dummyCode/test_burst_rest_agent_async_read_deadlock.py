# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import time
import threading
import uuid

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode,\
                                                         get_burst_conn_params
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.common.db.redshift_db import RedshiftDb

__all__ = [super_simulated_mode, get_burst_conn_params]

CHECK_ABORT_INTERVAL = 5
CREATE_TABLE_TEMPLATE = "CREATE TABLE {} (                 \
      ss_sold_date_sk int4 encode RAW,                          \
      ss_sold_time_sk int4 encode delta32k,                     \
      ss_item_sk int4 NOT NULL encode lzo,                      \
      ss_customer_sk int4 encode RAW,                           \
      ss_cdemo_sk int4 encode lzo,                              \
      ss_hdemo_sk int4 encode delta32k,                         \
      ss_addr_sk int4 encode lzo,                               \
      ss_store_sk int4 encode RAW,                              \
      ss_promo_sk int4 encode RAW,                              \
      ss_ticket_number int4 NOT NULL,                           \
      ss_quantity int4 encode RAW,                              \
      ss_wholesale_cost numeric(7,2) encode delta32k,           \
      ss_list_price numeric(7,2) encode delta32k,               \
      ss_sales_price numeric(7,2) encode RAW,                   \
      ss_ext_discount_amt numeric(7,2) encode lzo,              \
      ss_ext_sales_price numeric(7,2) encode lzo,               \
      ss_ext_wholesale_cost numeric(7,2) encode lzo,            \
      ss_ext_list_price numeric(7,2) encode lzo,                \
      ss_ext_tax numeric(7,2) encode mostly16,                  \
      ss_coupon_amt numeric(7,2) encode lzo,                    \
      ss_net_paid numeric(7,2) encode lzo,                      \
      ss_net_paid_inc_tax numeric(7,2) encode lzo,              \
      ss_net_profit numeric(7,2) encode RAW,                    \
      PRIMARY KEY (ss_item_sk, ss_ticket_number))               \
      distkey(ss_item_sk) sortkey(ss_sold_date_sk,ss_store_sk,  \
      ss_promo_sk,ss_customer_sk,ss_net_profit,ss_sales_price,  \
      ss_quantity, ss_item_sk,ss_ticket_number);"

COPY_TEMPLATE = "COPY {} FROM 's3://tpc-h/tpc-ds/1/store_sales.'      \
                IAM_ROLE 'arn:aws:iam::467896856988:role/Redshift-S3' \
                gzip DELIMITER '|';"

SIMULATE_DEADLOCK_EVENT = "EtSimulateRestAgentAsyncReadDeadLock"
SIMULATE_DEADLOCK_READY_EVENT = "EtSimulateRestAgentAsyncReadDeadLockReady"


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_local_gucs(gucs={
    "burst_conn_query_abort_check_interval":
    CHECK_ABORT_INTERVAL
})
class TestBurstRestAgentAsyncReadDeadlock(BurstWriteTest):
    def _setup_table(self, db_session, table_name):
        with db_session.cursor() as cursor:
            cursor.execute(CREATE_TABLE_TEMPLATE.format(table_name))
            cursor.execute(COPY_TEMPLATE.format(table_name))

    def _run_deadlock_query(self, cursor, table_name):
        try:
            cursor.execute("SELECT * FROM {}".format(table_name))
        except:
            pass

    def _check_query_read_hang(self, burst_cursor, table_name):
        burst_cursor.execute("SELECT COUNT(*) FROM stl_query WHERE querytxt" +
                             " LIKE 'SELECT * FROM {}%'".format(table_name))
        assert int(burst_cursor.fetch_scalar()) == 0

    def _check_query_aborted(self, table_name):
        with self.db.cursor() as cursor:
            # Query completed on main cluster.
            cursor.execute("SELECT aborted FROM stl_query WHERE querytxt" +
                           " LIKE 'SELECT * FROM {}%'".format(table_name))
            assert int(cursor.fetch_scalar()) == 1

    def test_break_rest_agent_async_read_deadlock(self, cluster):
        table_name = 'store_sales_{}'.format(uuid.uuid4().hex[:8])
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        thread_db_session = DbSession(cluster.get_conn_params())
        with burst_session.cursor() as burst_cursor, \
             thread_db_session.cursor() as thread_cursor:
            thread_cursor.execute("select pg_backend_pid()")
            deadlock_query_pid = int(thread_cursor.fetch_scalar())
            assert deadlock_query_pid != 0
            self._setup_table(thread_db_session, table_name)
            self._start_and_wait_for_refresh(cluster)

            thread_cursor.execute("SET enable_result_cache_for_session to off")
            thread_cursor.execute("SET query_group to burst")
            cluster.set_event(SIMULATE_DEADLOCK_EVENT)
            run_query_thread = threading.Thread(
                target=self._run_deadlock_query,
                args=(thread_cursor, table_name))
            run_query_thread.start()
            # Wait at most one minute for main process to get ready for simulated
            # deadlock.
            main_ready = False
            for i in range(60):
                if cluster.is_event_set(SIMULATE_DEADLOCK_READY_EVENT):
                    main_ready = True
                    break
                time.sleep(1)
            assert main_ready
            # Main is ready, set event on burst to block request read.
            burst_cursor.execute(
                "xpx 'event set {}'".format(SIMULATE_DEADLOCK_EVENT))
            # Unblock main so that it can get stuck in later run_one() while loop.
            cluster.unset_event(SIMULATE_DEADLOCK_EVENT)
            cluster.unset_event(SIMULATE_DEADLOCK_READY_EVENT)

            # Wait for one minute, verify that the query is still
            # in simulated deadlock.
            for i in range(60):
                assert run_query_thread.is_alive()
                self._check_query_read_hang(burst_cursor, table_name)
                time.sleep(1)

            # Unset burst event so that cancel request can be unblocked.
            burst_cursor.execute(
                "xpx 'event unset {}'".format(SIMULATE_DEADLOCK_EVENT))

            # Wait for another one minute, verify that the query is still
            # in simulated deadlock.
            for i in range(60):
                assert run_query_thread.is_alive()
                self._check_query_read_hang(burst_cursor, table_name)
                time.sleep(1)

            # Break the deadlock by aborting the query.
            self._run_bootstrap_sql(cluster, "SELECT pg_cancel_backend({})"
                                    .format(deadlock_query_pid))
            # Wait for sufficient time for query abort on burst.
            time.sleep(CHECK_ABORT_INTERVAL + 5)
            # Verify that the query has been aborted on main.
            assert not run_query_thread.is_alive()
            self._check_query_aborted(table_name)
