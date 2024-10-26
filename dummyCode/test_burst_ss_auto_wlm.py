# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import getpass
import logging
import uuid
import pytest
import threading
from time import sleep

from raff.burst.burst_super_simulated_mode_helper \
    import super_simulated_mode, get_burst_conn_params, cold_start_ss_mode
from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst
from raff.common.db.session import DbSession
from raff.common.db.redshift_db import RedshiftDb

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
TPCDS_Q1_SQL = """
      WITH /* TPC-DS query1.tpl 0.12 */ customer_total_return AS
          (SELECT sr_customer_sk AS ctr_customer_sk,
                  sr_store_sk AS ctr_store_sk,
                  sum(SR_STORE_CREDIT) AS ctr_total_return
           FROM store_returns,
                date_dim
           WHERE sr_returned_date_sk = d_date_sk
             AND d_year =2000
           GROUP BY sr_customer_sk,
                    sr_store_sk)
        SELECT /* TPC-DS query1.tpl 0.12 */ top 100 c_customer_id
        FROM customer_total_return ctr1,
             store,
             customer
        WHERE ctr1.ctr_total_return >
            (SELECT avg(ctr_total_return)*1.2
             FROM customer_total_return ctr2
             WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
          AND s_store_sk = ctr1.ctr_store_sk
          AND s_state = 'MI'
          AND ctr1.ctr_customer_sk = c_customer_sk
        ORDER BY c_customer_id;
"""

VOLT_TT_CS_VERIFY_SQL = (
    'select count(*) from stl_query '
    'where concurrency_scaling_status = 1 and aborted = 0 '
    'and xid = {}'
)

WAITING_TIMEOUT = 720

FORCED_WLM_CONC_MSG = "Forced auto wlm concurrency, heavy: {}, on_demand {}."
FORCED_FLEXIBLE_MEM_MSG = "Forced flexible memory allocation: {}."
AUTO_WLM_SYNC_MSG = "Auto wlm burst sync request completed."
AUTO_WLM_NOT_SYNC_MSG = "Auto wlm burst sync request was not sent, sage " \
                        "scaling and unifed memory prediction are both disabled."


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.load_tpcds('store_returns', 'date_dim', 'store', 'customer')
@pytest.mark.custom_local_gucs(gucs={
    'wlm_json_configuration': (
            '[{"auto_wlm": true, "concurrency_scaling":"auto"}]'),
    'autowlm_max_heavy_slots': 20,
    'enable_burst_failure_handling': 'true',
    'burst_sum_queue_time_limit_s': '2',
    'try_burst_first': 'false',
    'enable_short_query_bias': 'false',
    'enable_sqa_by_default': 'false',
    'enable_elastic_sqa': 'false',
    'enable_predictor': 'true',
    'enable_burst_auto_wlm': 'true',
    "vacuum_auto_enable": "false",
    "enable_advisor_client_proc": "false",
    "burst_enable_ctas_failure_handling": "true",
    # Both following GUC and sage mode can enable or disable
    # flexible memory synchronization with burst independently.
    # Some test enables/disables sage mode to test this feature on
    # and off. In order to turn off synchronization correctly,
    # we should set this GUC to off by default for the whole test suite.
    "enable_unified_memory_prediction_for_main_and_burst": "false"
})
@pytest.mark.custom_burst_gucs(gucs={
    'autowlm_max_heavy_slots': 20,
    'enable_burst_auto_wlm': 'true',
    "vacuum_auto_enable": "false",
    "enable_advisor_client_proc": "false",
    "burst_enable_ctas_failure_handling": "true",
})
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBurstSSAutoWlm(BurstTest):
    def run_long_query(self, db_session):
        with db_session.cursor() as cursor:
            try:
                cursor.execute(
                    "set query_group to burst; "
                    "select count(*) from customer A, customer B,"
                    "customer C;"
                )
            except Exception as e:
                log.info("Possibly cancelled with {}".format(e))

    def run_volt_tt_query(self, db_session):
        try:
            with db_session.cursor() as cursor:
                cursor.execute(TPCDS_Q1_SQL)
        except Exception as e:
            pytest.fail("run_volt_tt_query failed {}".format(e))

    def kill_all_queued_and_running_queries(self, db_session):
        with db_session.cursor() as cursor:
            max_trial = 5
            while max_trial > 0:
                # Try to find the processes multiple times because some
                # queries may be queued and not show up in stv_inflight.
                sleep(5)
                cursor.execute("set query_group to metrics;")
                cursor.execute(
                    "select pid from stv_inflight A, stv_wlm_query_state B "
                    "where A.query = B.query and (B.service_class >= 100 or "
                    "service_class = 14)")
                rows = cursor.fetchall()
                for row in rows:
                    pid = row[0]
                    log.info("Killing {}".format(pid))
                    cursor.execute(
                        "select pg_terminate_backend({})".format(pid))
                if len(rows) == 0:
                    break
                max_trial -= 1

    def kill_running_main_queries(self, db_session, count):
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to metrics;")
            cursor.execute(
                "select pid from stv_inflight A, stv_wlm_query_state B "
                "where A.query = B.query and B.service_class >= 100 "
                "and concurrency_scaling_status = 0 "
                "order by exec_time limit {}".format(count))
            rows = cursor.fetchall()
            for i in range(count):
                pid = rows[i][0]
                log.info("Killing {}".format(pid))
                cursor.execute(
                    "select pg_terminate_backend({})".format(pid))

    def kill_running_burst_queries(self, db_session, count):
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to metrics;")
            cursor.execute(
                "select pid from stv_inflight A, stv_wlm_query_state B "
                "where A.query = B.query and B.service_class >= 100 "
                "and concurrency_scaling_status = 1 "
                "order by exec_time desc limit {}".format(count))
            rows = cursor.fetchall()
            for i in range(count):
                pid = rows[i][0]
                log.info("Killing {}".format(pid))
                cursor.execute(
                    "select pg_terminate_backend({})".format(pid))

    def print_stv_stats(self, a_session, query):
        with a_session.cursor() as cursor:
            cursor.execute(query)
            for row in cursor.fetchall():
                log.info(row)

    def print_inflight_info(self):
        """
        Utility function to print main and burst cluster inflight info.
        """
        stv_inflight_sql = (
            "select userid, query, btrim(label), xid, pid, "
            "concurrency_scaling_status, btrim(text) "
            "from stv_inflight")
        stv_wlm_query_state_sql = (
            "select xid, task, query, service_class, state, "
            "queue_time, exec_time from stv_wlm_query_state")

        log.info("Main cluster stv_inflight")
        self.print_stv_stats(self.db, stv_inflight_sql)
        log.info("Main cluster stv_wlm_query_state")
        self.print_stv_stats(self.db, stv_wlm_query_state_sql)
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        log.info("Burst cluster stv_inflight")
        self.print_stv_stats(burst_session, stv_inflight_sql)
        log.info("Burst cluster stv_wlm_query_state")
        self.print_stv_stats(burst_session, stv_wlm_query_state_sql)

    def dump_gucs(self):
        guc_show_sql = "show all"
        log.info("Main cluster gucs")
        self.print_stv_stats(self.db, guc_show_sql)
        log.info("Burst cluster gucs")
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        self.print_stv_stats(burst_session, guc_show_sql)

    def wait_until_queries_with_state(self, a_session, target_num, state):
        """
        Wait until target_num queries are with the given state. The test is
        failed if this doesn't happen even after 2hr.
        """
        with a_session.cursor() as cursor:
            count = 0
            max_trial = WAITING_TIMEOUT
            while count != target_num:
                sleep(1)
                xids = {}
                cursor.execute("set query_group to metrics;")
                cursor.execute("select xid, query from stv_wlm_query_state "
                               "where service_class = 100 "
                               "and state = '{}'".format(state))
                rows = cursor.fetchall()
                for row in rows:
                    xids[int(row[0])] = int(row[1])
                count = len(xids)
                log.info("Waiting for state {}. Current count={}. "
                         "(xid, qid)= {}".format(state, count, list(xids.items())))
                max_trial -= 1
                if max_trial == 0:
                    self.print_inflight_info()
                    pytest.fail("wait_until_queries_with_state timed out")

    def wait_until_queries_running_on_burst(self, target_num):
        """
        Wait until target_num queries are running on burst.

        When checking for running on burst, checking for cs status 1 is not
        enough because queries frequently try to run on main until no queuing.
        During those times of trying queries will show cs status 1. A more
        consistent check is to check on burst cluster how many running.
        """
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        self.wait_until_queries_with_state(burst_session, target_num,
                                           'Running')

    def wait_until_message_seen(self, session, ts, msg):
        with session.cursor() as cursor:
            cursor.execute("set query_group to metrics;")
            max_trial = WAITING_TIMEOUT
            sql = "select count(*) from stl_event_trace " \
                  "where message ilike '%{}%' " \
                  "and EXTRACT(epoch FROM eventtime) >= {} ".format(msg, ts)
            msg_seen = 0
            while msg_seen == 0 and max_trial > 0:
                sleep(1)
                cursor.execute(sql)
                msg_seen = int(cursor.fetch_scalar())
                max_trial -= 1
            if msg_seen == 0 and max_trial == 0:
                self.print_inflight_info()
                pytest.fail("wait_until_message_seen timed out")

    def wait_until_queries_running_on_main(self, target_num):
        """
        Wait until target_num queries are running on main. The test is
        failed if this doesn't happen even after 2hr.

        When checking for running on main, check for cs status 0.
        """
        with self.db.cursor() as cursor:
            count = 0
            max_trial = WAITING_TIMEOUT
            while count != target_num:
                sleep(1)
                cursor.execute("set query_group to metrics;")
                cursor.execute("select count(*) from stv_inflight "
                               "where concurrency_scaling_status = 0 "
                               "and label ilike '%burst%'")
                rows = cursor.fetchall()
                for row in rows:
                    count = int(row[0])
                log.info("Waiting for queries on main. Current count={}"
                         "".format(count))
                max_trial -= 1
                if max_trial == 0:
                    self.print_inflight_info()
                    pytest.fail("wait_until_queries_running_on_main timed out")

    def wait_until_burst_rejects_due_to_queuing(self, ts):
        """
        Utility function to wait and check that rejected queries from burst
        have appropriate status set.
        """
        with self.db.cursor() as cursor:
            cursor.execute("set query_group to metrics;")
            rejected_count = 0
            max_trial = WAITING_TIMEOUT
            sql = "select count(*) from stl_query " \
                  "where concurrency_scaling_status = 65 " \
                  "and EXTRACT(epoch FROM starttime) >= {} ".format(ts)
            while rejected_count == 0:
                cursor.execute(sql)
                rejected_count = int(cursor.fetch_scalar())
                max_trial -= 1
                if max_trial == 0:
                    self.print_inflight_info()
                    pytest.fail("wait_until_burst_rejects_due_to_queuing "
                                "timed out")
            sql = "select count(*) from stl_query " \
                  "where concurrency_scaling_status = 29 " \
                  "and EXTRACT(epoch FROM starttime) >= {} ".format(ts)
            cursor.execute(sql)
            failed_count = int(cursor.fetch_scalar())
            assert failed_count == 0, "No queries should have failed status."

    def wait_until_last_volt_query_makes_progress(self, ts, sub_query_count):
        """
        Utility function to check the progress of volt query on the main.

        Find the xid of the last volt ctas after the given timestamp, and then
        check how many queries in the same xid completed on burst. Fail the
        test if these checks don't finish in 2hr.
        """
        with self.db.cursor() as cursor:
            cursor.execute("set query_group to metrics;")
            cs_count = 0
            max_trial = WAITING_TIMEOUT
            while cs_count != sub_query_count:
                sleep(1)
                sql = "select xid from stl_query " \
                      "where querytxt ilike '%CREATE TEMP TABLE volt_tt%' " \
                      "and concurrency_scaling_status = 1 and aborted = 0 " \
                      "and EXTRACT(epoch FROM starttime) >= {} " \
                      "order by endtime desc limit 1".format(ts)
                cursor.execute(sql)
                xid = cursor.fetch_scalar()
                if isinstance(xid, int):
                    cursor.execute(VOLT_TT_CS_VERIFY_SQL.format(xid))
                    cs_count = cursor.fetch_scalar()
                max_trial -= 1
                if max_trial == 0:
                    self.print_inflight_info()
                    pytest.fail("wait_until_last_volt_query_makes_progress "
                                "timed out")

    def start_queries(self, cluster, num_queries):
        for _ in range(num_queries):
            thread = threading.Thread(
                target=self.run_long_query,
                args=(DbSession(cluster.get_conn_params()), ))
            thread.start()

    def current_time(self):
        with self.db.cursor() as cursor:
            cursor.execute("SELECT EXTRACT(epoch FROM SYSDATE)")
            ts = cursor.fetch_scalar()
        return ts

    def set_auto_wlm_concurrency(self, session, heavy, on_demand):
        ts = self.current_time()
        with session.cursor() as burst_cursor:
            burst_cursor.execute(
                "xpx 'event set EtSetAutoWlmConcurrency,heavy={},"
                "on_demand={}'".format(heavy, on_demand))
        self.wait_until_message_seen(
            session, ts, FORCED_WLM_CONC_MSG.format(heavy, on_demand))

    def set_auto_wlm_flexibility(self, cluster, flexible):
        ts = self.current_time()
        cluster.set_event('EtFlexibleMemoryAlwaysOn, enabled={}'.format(
            flexible))
        self.wait_until_message_seen(self.db, ts,
                                     FORCED_FLEXIBLE_MEM_MSG.format(flexible))


class TestBasicAutoWlm(TestBurstSSAutoWlm):
    def test_basic_auto_wlm_on_burst(self, cluster):
        """
        Tests that more queries burst as burst auto wlm concurrency increases
        and less queries burst as it decreases.

        Tests the following:
        1. Queuing on main when burst is fully occupied.
        2. As concurrency increases on burst more queries burst.
        3. When a query finishes on burst, a queued query bursts.
        4. When concurrency decreases on burst queued queries remain queued.
        5. When a query on main finishes, a queued query runs on main.
        """
        ts = self.current_time()
        cold_start_ss_mode(cluster)
        try:
            burst_session = RedshiftDb(conn_params=get_burst_conn_params())
            with cluster.event('EtLogAutoConcurrency', 'level=ElDebug5'), \
                    cluster.event('EtSetStaticPrediction', 'memory=10'), \
                    cluster.event('EtBurstTracing', 'level=ElDebug5'):
                with burst_session.cursor() as burst_cursor:
                    burst_cursor.execute(
                        "xpx 'event set EtLogAutoConcurrency,level=ElDebug5'")
                    # Need to make sure flexible memory allocation is on so that
                    # queries can run with small amount of memory.
                    burst_cursor.execute(
                        "xpx 'event set EtFlexibleMemoryAlwaysOn,enabled=1'")
                self.wait_until_message_seen(
                    burst_session, ts, FORCED_FLEXIBLE_MEM_MSG.format(1))
                # Initially flexible memory allocation is off until enough
                # queries are correctly estimated. So, force here for the test.
                self.set_auto_wlm_concurrency(self.db, heavy=5,
                                              on_demand=0)
                self.set_auto_wlm_flexibility(cluster, flexible=1)
                self.set_auto_wlm_concurrency(burst_session, heavy=5,
                                              on_demand=0)
                # Start with 5 queries on main. 25 left to burst as the test
                # progress with different concurrency.
                for _ in range(30):
                    thread = threading.Thread(
                        target=self.run_long_query,
                        args=(DbSession(cluster.get_conn_params()), ))
                    thread.start()

                # 5 running on main, 5 on burst, 20 waiting.
                self.wait_until_queries_running_on_main(5)
                self.wait_until_queries_running_on_burst(5)
                self.wait_until_queries_with_state(
                    self.db, target_num=20, state='QueuedWaiting')

                self.set_auto_wlm_concurrency(burst_session, heavy=10,
                                              on_demand=0)

                self.wait_until_queries_running_on_main(5)
                self.wait_until_queries_running_on_burst(10)
                self.wait_until_queries_with_state(
                    self.db, target_num=15, state='QueuedWaiting')

                self.set_auto_wlm_concurrency(burst_session, heavy=20,
                                              on_demand=1)

                self.wait_until_queries_running_on_main(5)
                self.wait_until_queries_running_on_burst(20)
                self.wait_until_queries_with_state(
                    self.db, target_num=5, state='QueuedWaiting')

                # 5 running on main, 20 on burst. Kill 1 burst query so that
                # one more query can burst and one less query is waiting.
                self.kill_running_burst_queries(self.db, 1)
                self.wait_until_queries_running_on_main(5)
                self.wait_until_queries_running_on_burst(20)
                self.wait_until_queries_with_state(
                    self.db, target_num=4, state='QueuedWaiting')

                self.set_auto_wlm_concurrency(burst_session, heavy=17,
                                              on_demand=0)

                self.kill_running_burst_queries(self.db, 4)
                self.wait_until_queries_running_on_main(5)
                self.wait_until_queries_running_on_burst(17)
                self.wait_until_queries_with_state(
                    self.db, target_num=3, state='QueuedWaiting')

                # Kill a query on main. Since the concurrency on burst is still
                # 17, 17 queries will be bursting. But 1 less query will be
                # queued as it starts running on main.
                self.kill_running_main_queries(self.db, 1)
                self.wait_until_queries_running_on_main(5)
                self.wait_until_queries_running_on_burst(17)
                self.wait_until_queries_with_state(
                    self.db, target_num=2, state='QueuedWaiting')

                # Verify that queries were marked rejected due to queuing on
                # burst and not marked failed with execution error.
                self.wait_until_burst_rejects_due_to_queuing(ts)
        finally:
            self.kill_all_queued_and_running_queries(self.db)


class TestAutoWlmFlexibleMemoryAllocation(TestBurstSSAutoWlm):
    def test_auto_wlm_flexible_memory_allocation_on_burst(self, cluster):
        """
        Tests that burst auto wlm follows main auto wlm flexible memory
        allocation.

        Tests the following:
        1. When flexible memory allocation is "off" on main cluster, burst
            cluster only executes 5 queries.
        2. When flexible memory allocation is "on" on main cluster, burst
            cluster executes more than 5 queries.
        """
        cold_start_ss_mode(cluster)
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        try:
            with cluster.event('EtLogAutoConcurrency', 'level=ElDebug5'), \
                    cluster.event('EtSetStaticPrediction', 'memory=10'), \
                    cluster.event('EtBurstTracing', 'level=ElDebug5'):
                with burst_session.cursor() as burst_cursor:
                    burst_cursor.execute(
                        "xpx 'event set EtLogAutoConcurrency,level=ElDebug5'")
                self.set_auto_wlm_concurrency(self.db, heavy=5, on_demand=0)
                # First, turn off flexible memory allocation.
                self.set_auto_wlm_flexibility(cluster, flexible=0)
                # Turn back on flexible memory allocation. Now it should
                # sync with burst cluster.
                ts = self.current_time()
                self.set_auto_wlm_flexibility(cluster, flexible=1)
                self.wait_until_message_seen(self.db, ts,
                                             AUTO_WLM_NOT_SYNC_MSG)
                ts = self.current_time()
                with cluster.event('EtSageScalingForceEnable'):
                    self.wait_until_message_seen(self.db, ts,
                                                 AUTO_WLM_SYNC_MSG)
                self.set_auto_wlm_concurrency(burst_session, heavy=15,
                                              on_demand=1)
                self.start_queries(cluster, num_queries=20)
                # 5 running on main, 15 on burst, because all of them fits.
                self.wait_until_queries_running_on_main(target_num=5)
                self.wait_until_queries_running_on_burst(target_num=15)
                # Kill all burst queries.
                self.kill_running_burst_queries(self.db, count=15)

                # Burst already has 15 target slots, those now need to be
                # removed. Remove them deterministically without waiting for
                # concurrency adjustment.
                self.set_auto_wlm_concurrency(burst_session, heavy=5,
                                              on_demand=1)

                ts = self.current_time()
                self.set_auto_wlm_flexibility(cluster, flexible=0)
                self.wait_until_message_seen(self.db, ts,
                                             AUTO_WLM_NOT_SYNC_MSG)
                ts = self.current_time()
                with cluster.event('EtSageScalingForceEnable'):
                    self.wait_until_message_seen(self.db, ts,
                                                 AUTO_WLM_SYNC_MSG)

                # Send another 15 queries. Main is occupied, only 5 can run
                # on burst because flexible memory allocation is off on main.
                self.start_queries(cluster, num_queries=15)
                # 5 running on main, 5 on burst, because flexible memory
                # allocation is off. And, 10 are waiting.
                self.wait_until_queries_running_on_main(target_num=5)
                self.wait_until_queries_running_on_burst(target_num=5)
                self.wait_until_queries_with_state(
                    self.db, target_num=10, state='QueuedWaiting')
        finally:
            self.kill_all_queued_and_running_queries(self.db)


class TestVoltTtRetriesAfterQueuingError(TestBurstSSAutoWlm):
    def test_volt_tt_retries_after_queuing_error(self, cluster):
        """
        Tests that in case of queuing error volt ctas queries can still burst.

        Tests the following case:
        1. First Volt tt ctas queues on main in case of queuing error on burst.
        2. All subsequent volt queries queues on burst in case of queuing.
        """
        try:
            self.dump_gucs()
            cold_start_ss_mode(cluster)
            burst_session = RedshiftDb(conn_params=get_burst_conn_params())
            ts = self.current_time()
            with cluster.event('EtLogAutoConcurrency', 'level=ElDebug5'), \
                    cluster.event('EtSetAutoWlmConcurrency', 'heavy=1',
                                  'on_demand=0'), \
                    cluster.event('EtSetStaticPrediction', 'memory=10000'), \
                    cluster.event('EtBurstTracing', 'level=ElDebug5'), \
                    burst_session.cursor() as burst_cursor:
                burst_cursor.execute(
                    "xpx 'event set EtLogAutoConcurrency,level=ElDebug5'")
                # Initially flexible memory allocation is off until enough
                # queries are correctly estimated. So, force here for the test.
                cluster.set_event('EtFlexibleMemoryAlwaysOn, enabled=1')
                # Allow only 5 queries to burst.
                burst_cursor.execute(
                    "xpx 'event set EtSetAutoWlmConcurrency,heavy=1,"
                    "on_demand=0'")

                self.wait_until_message_seen(self.db, ts,
                                             FORCED_WLM_CONC_MSG.format(1, 0))
                self.wait_until_message_seen(self.db, ts,
                                             FORCED_FLEXIBLE_MEM_MSG.format(1))
                self.wait_until_message_seen(burst_session, ts,
                                             FORCED_WLM_CONC_MSG.format(1, 0))

                # Occupy both main and burst with 1 queries running on each.
                for _ in range(2):
                    thread = threading.Thread(
                        target=self.run_long_query,
                        args=(DbSession(cluster.get_conn_params()), ))
                    thread.start()

                # 5 running on main, 5 on burst.
                self.wait_until_queries_running_on_main(1)
                self.wait_until_queries_running_on_burst(1)

                with self.db.cursor() as cursor:
                    cursor.execute("SELECT EXTRACT(epoch FROM SYSDATE)")
                    ts = cursor.fetch_scalar()
                # Run the query with volt tt.
                thread_volt = threading.Thread(
                    target=self.run_volt_tt_query,
                    args=(DbSession(cluster.get_conn_params()), ))
                thread_volt.start()
                self.wait_until_queries_with_state(
                    self.db, target_num=1, state='QueuedWaiting')

                # 5 running on main, 5 on burst. Kill 1 burst query so that
                # the waiting query with volt tt can now run on burst.
                self.kill_running_burst_queries(self.db, 1)
                # Burst query with volt tt should finish normally.
                # sub_query_count = 2 means, both queries finished bursting.
                self.wait_until_last_volt_query_makes_progress(
                    ts, sub_query_count=2)

                with cluster.event('EtSimulateSlowPrepareInStickySession',
                                   'sleep=10000'):
                    with self.db.cursor() as cursor:
                        cursor.execute("SELECT EXTRACT(epoch FROM SYSDATE)")
                        ts = cursor.fetch_scalar()
                    # Run another query with volt tt which will wait after the
                    # the temp table is created on burst cluster.
                    thread_volt = threading.Thread(
                        target=self.run_volt_tt_query,
                        args=(DbSession(cluster.get_conn_params()), ))
                    thread_volt.start()

                    # sub_query_count = 1 means, only volt CTAS bursted,
                    # the follow up query is expected to be waiting in burst
                    # prepare due to EtSimulateSlowPrepareInStickySession.
                    self.wait_until_last_volt_query_makes_progress(
                        ts, sub_query_count=1)

                    # After volt tt creation, above query is waiting in prepare
                    # due to EtSimulateSlowPrepareInStickySession.
                    self.wait_until_queries_running_on_burst(0)
                    last_long_query_thread = threading.Thread(
                        target=self.run_long_query,
                        args=(DbSession(cluster.get_conn_params()), ))
                    last_long_query_thread.start()
                    # This new long query should be bursting soon because
                    # prepare is waiting in sticky session.
                    self.wait_until_queries_running_on_burst(1)

                # Check on the burst cluster, one query waiting.
                self.wait_until_queries_with_state(
                    burst_session, target_num=1, state='QueuedWaiting')
                self.kill_running_burst_queries(self.db, 1)
                # Burst query with volt tt should now finish normally.
                # Burst query with volt tt should finish normally.
                # sub_query_count = 2 means, both queries finished bursting.
                self.wait_until_last_volt_query_makes_progress(
                    ts, sub_query_count=2)
        finally:
            self.kill_all_queued_and_running_queries(self.db)
