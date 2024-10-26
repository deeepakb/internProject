# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
from contextlib import contextmanager

import pytest
from datetime import datetime, timedelta
from time import sleep
from raff.burst.burst_test import (BurstTest, setup_teardown_burst,
                                   verify_query_bursted,
                                   verify_query_didnt_burst)
from raff.util.utils import run_bootstrap_sql
from raff.common.remote_helper import RedMachine
from raff.common.ssh_user import SSHUser
from raff.arcadia.arcadia_test import run_bootstrap_query

log = logging.getLogger(__name__)
# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [
    'setup_teardown_burst', 'verify_query_bursted', 'verify_query_didnt_burst'
]

CUSTOM_GUCS = {
    'burst_cursor_prefetch_rows': '5',
    'enable_burst_result_cache': 'false',
    'burst_billing_period': '1'  # To allow faster tests
}

LOCALHOST_GUCS = {
    'enable_burst_async_acquire': 'false',
    'enable_burst_billing': 'true',
    'burst_billing_period': '1',
    'enable_burst_release': 'true'
}

SQL_BASIC = "SELECT * FROM customer LIMIT 100;"
SQL_FETCH = "FETCH FORWARD {} FROM c1;"


class BurstBillingTestHelpers(BurstTest):

    @contextmanager
    def burst_exec_error_event(self, cluster):
        try:
            cluster.set_event("EtSimulateBurstManagerError, error=Execute")
            yield
        finally:
            cluster.unset_event("EtSimulateBurstManagerError")

    def verify_no_error(self, cluster, start):
        start_str = start.isoformat(' ')
        res = run_bootstrap_sql(
            cluster,
            ("select count(*) from stl_concurrency_scaling_usage_error "
             "where recordtime > '{}' and "
             "message not like 'Cluster release reported on %';"
             ).format(start_str))
        c = int(res[0][0])
        assert c == 0, ("{} errors found in "
                        "stl_concurrency_scaling_usage_error").format(c)

    def exec_time_ms(self, query_id):
        with self.db.cursor() as cursor:
            cursor.execute(
                ("select datediff(millisecond, starttime, endtime) "
                 "from stl_query where query = {}").format(query_id))
            ms = cursor.fetch_scalar()
        return ms

    def bill_time_ms(self, cluster, query_id):
        res = run_bootstrap_sql(
            cluster, ("select sum(incurred_usage_ms) "
                      "from stl_concurrency_scaling_usage where query = {}"
                      ).format(query_id))
        try:
            ms = int(res[0][0])
            return ms
        except IndexError:
            return 0

    def declare_cursor_and_fetch(self, db_cursor):
        """
        On the db_cursor, start a transaction, declare a server side cursor,
        and fetch a few times.
        """
        db_cursor.execute("BEGIN;")
        db_cursor.execute("DECLARE c1 CURSOR FOR SELECT * FROM customer;")
        db_cursor.execute(SQL_FETCH.format(5))
        db_cursor.fetchall()
        qid = self.last_query_id(db_cursor)
        sleep(2)
        db_cursor.execute(SQL_FETCH.format(5))
        db_cursor.fetchall()
        sleep(2)
        db_cursor.execute(SQL_FETCH.format(5))
        db_cursor.fetchall()
        sleep(2)
        return qid

    def _warmup(self, cluster, db_session):
        """
        Burst cluster may take a few seconds to cold start. Running this
        function can ensure the personalization-coldstart process is done.
        """
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute("select count(*) from customer;")
            _ = cursor.fetchall()
            qid = self.last_query_id(cursor)
            self.verify_query_bursted(cluster, qid)

    def bill_time_ris(self, cluster):
        res = run_bootstrap_query(cluster, self.db,
                                  ("select usage_sec "
                                   "from stl_burst_billing_ris_reports"))
        try:
            ms = int(res[0][0])
            return ms
        except IndexError:
            return 0

    def verify_burst_query(self, cluster):
        bill_time_ris = self.bill_time_ris(cluster)
        # Asserting usage_sec value has been populated in stl_burst_billing_ris_reports table
        assert bill_time_ris > 0

    def basic_query_ris(self, cluster, db_session, query, num_rows):
        with self.burst_db_cursor(db_session) as cursor:
            # Event set to ensure that Burst Cluster is released
            with cluster.event('EtSimulateBillingBurstClusterRelease'):
                cursor.execute(query)
                # Wait for the report to occur.
                sleep(2)
                self.verify_burst_query(cluster)


# TODO(mingda): replace the operating_mode value (100) with mode enum later.


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(
    gucs={
        'manual_mv_auto_refresh_run_with_user_queries': '0',
        'manual_mv_auto_refresh_threshold_type': '1',
        'operating_mode': '100',
        "xen_guard_enabled": "true",
        "try_burst_first": "false",
        'enable_burst_billing': 'true',  # localhost setup for burst billing
        'burst_billing_period': '1',
        'enable_burst_async_acquire': 'false',
        "multi_az_enabled": "false",
        'enable_burst_release': 'true'
    })
@pytest.mark.serial_only
@pytest.mark.no_jdbc
class TestBurstAutoTunerLogging(BurstBillingTestHelpers):
    def test_burst_auto_tuner_billing_report(self, cluster, db_session):
        """
        In this test, we are testing when user query is bursted in
        lieu of auto tasks running on main, the generated usage in
        the stl table will lable this cost as is_cs_at_query (means
        the cost is from bursting for autotuner, at query). The test
        is done on provisioned.

        We will firstly set the event EtAutoTunerQueryCannotRunOnMain
        and then run a query.
        AutoTuner will burst the query as it CannotRunOnMain in lieu of
        auto task to be executed on main.
        We verified that stl table contains a row with is_cs_at_query=1
        """

        # Set Event to simulate the condition user query cannot run on
        # main as auto tasks are running/queueing on main.
        with cluster.event('EtAutoTunerQueryCannotRunOnMain'):
            with db_session.cursor() as cursor, self.db.cursor() as cur:

                # Submit user query to burst queue.
                self.execute_test_file('burst_query', session=db_session)
                qid = self.last_query_id(cursor)
                # Wait for the report to occur.
                sleep(2)
                self.verify_query_bursted(cluster, qid)
                cur.execute(
                    ("select COUNT(*) "
                     "from stl_concurrency_scaling_usage where query = {}"
                     " AND is_cs_at_query = 1").format(qid))
                res = cur.fetch_scalar()
                # The query is bursted in lie of auto tasks and is recorded
                # in stl_concurrency_scaling_usage
                assert res > 0


@pytest.mark.load_tpcds_data
@pytest.mark.cluster_only  # We need to talk to RIS, which is only on cluster.
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS)
@pytest.mark.serial_only
class TestBurstBilling(BurstBillingTestHelpers):

    def verify_regular_query(self, cluster, db_session, query, num_rows):
        start = datetime.utcnow()
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(query)
            res = cursor.fetchall()
            assert len(res) == num_rows
            qid = self.last_query_id(cursor)
            exec_time = self.exec_time_ms(qid)
            bill_time = self.bill_time_ms(cluster, qid)
            assert bill_time > 0
            # The execute time of the whole query must be no longer than bill.
            assert exec_time >= bill_time
            # The difference should be contained in 3 seconds though.
            assert bill_time >= exec_time - 3000
            # Wait for the report to occur.
            sleep(2)
            self.verify_no_error(cluster, start)

    @pytest.mark.no_jdbc
    def test_basic_query(self, cluster, db_session):
        # Warm up the burst cluster so that the test is more reliable.
        self._warmup(cluster, db_session)
        self.verify_regular_query(cluster, db_session, SQL_BASIC, 100)

    @pytest.mark.no_jdbc
    def test_basic_cursor_query(self, cluster, db_session):
        """Test a basic cursor query with typical use pattern."""
        with self.burst_db_cursor(db_session) as cursor:
            cluster.run_xpx('clear_result_cache')
            start = datetime.utcnow()
            qid = self.declare_cursor_and_fetch(cursor)
            cursor.execute("CLOSE c1;")
            cursor.execute("COMMIT;")
            elapsed_ms = (datetime.utcnow() - start).total_seconds() * 1000
            bill = self.bill_time_ms(cluster, qid)
            assert elapsed_ms >= bill
            assert bill >= elapsed_ms - 3000
            sleep(2)
            self.verify_no_error(cluster, start)

    @pytest.mark.no_jdbc
    def test_cursor_unrelated_error(self, cluster, db_session):
        """Test a cursor query with user error and transaction abort."""
        cluster.run_xpx('clear_result_cache')
        with self.burst_db_cursor(db_session) as cursor:
            start = datetime.utcnow()
            qid = self.declare_cursor_and_fetch(cursor)
            try:
                cursor.execute("SELECT * from wtf_non_exist_tbl;")
            except Exception:
                pass
            cursor.execute("ABORT;")
            elapsed_ms = (datetime.utcnow() - start).total_seconds() * 1000
            bill = self.bill_time_ms(cluster, qid)
            assert elapsed_ms >= bill
            assert bill >= elapsed_ms - 3000
            sleep(2)
            self.verify_no_error(cluster, start)

    @pytest.mark.no_jdbc
    def test_cursor_burst_error(self, cluster, db_session):
        """Test a cursor query with server error and transaction abort."""
        cluster.run_xpx('clear_result_cache')
        start = datetime.utcnow()
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute("BEGIN;")
            qid = self.declare_cursor_and_fetch(cursor)
            with self.burst_exec_error_event(cluster):
                try:
                    cursor.execute(SQL_FETCH.format(5))
                except Exception:
                    pass
            cursor.execute("ABORT;")
            bill = self.bill_time_ms(cluster, qid)
            assert bill == 0
            sleep(2)
            self.verify_no_error(cluster, start)

    @pytest.mark.no_jdbc
    def test_burst_2562(self, cluster, db_session):
        """Test Burst-2562, verify fix of crash on proc_exit."""
        cluster.run_xpx('clear_result_cache')
        sshc = RedMachine(cluster.leader_public_ip, cluster.region)

        with self.burst_db_cursor(db_session) as cursor:
            self.declare_cursor_and_fetch(cursor)
        db_session.close()
        # There is no fast way of detecting crash, it may take couple minutes
        # for HM to notice and kill padb. Instead of monitoringg the PIDs, we
        # just grep the log, which seems to be reliable enough.
        r, o, e = sshc.run_remote_cmd(
            "timeout 5 tail -n 50 -f /rdsdbdata/data/log/start_node.log",
            user=SSHUser.RDSDB,
            retcode=124)
        assert o.find("received signal 11") == -1, "Found signal 11"


@pytest.mark.burst
@pytest.mark.load_tpcds_data
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=LOCALHOST_GUCS)
@pytest.mark.serial_only
@pytest.mark.localhost_only
class TestBurstBillingRIS(BurstBillingTestHelpers):

    def test_basic_query_ris(self, cluster, db_session):
        # Warm up the burst cluster so that the test is more reliable.
        self._warmup(cluster, db_session)
        self.basic_query_ris(cluster, db_session, SQL_BASIC, 100)
