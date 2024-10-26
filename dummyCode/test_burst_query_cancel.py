# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved
from __future__ import division
import logging
import uuid
import pytest
import time
import multiprocessing

from contextlib import contextmanager
from six.moves import range
from raff.common.cluster.cluster_session import ClusterSession
from raff.common.aws_clients.redshift_client import RedshiftClient
from raff.common.profile import Profiles
from raff.common.region import Regions
from raff.common.db.session import DbSession
from raff.common.db.redshift_db import RedshiftDb
from raff.common.db.session_context import SessionContext
from raff.burst.burst_test import (
    BurstTest,
    setup_teardown_burst,
    verify_no_leaked_sessions
)
from raff.common.cluster_xen_guard import ClusterXenGuardControl
from raff.util.utils import run_bootstrap_sql
from raff.common.host_type import HostType
from raff.common.dimensions import Dimensions
from raff.burst.burst_temp_write import burst_user_temp_support_gucs, \
    BurstTempWrite

log = logging.getLogger(__name__)

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst]

CUSTOM_GUCS = {
    'burst_mode': '3',
    'enable_short_query_bias': 'false',
    'skip_count_for_ctas': 'false',
    'enable_prefetching': 'false',
    'enable_burst_async_acquire': 'false',
    'xen_guard_enabled': 'true'
}

CUSTOM_GUCS_WITH_CANCEL = dict(
    list(CUSTOM_GUCS.items()) + [('burst_check_cancel_s', 1)])
QUERY_RUNNING_TIMEOUT_MINUTES = 10

CREATE_TEMPORARY_TABLE_SQL = (
    "CREATE TEMPORARY TABLE {} as (SELECT * FROM {}.{})")


@pytest.yield_fixture(scope='class')
def enable_burst_temp_table_guc(request, cluster):
    session = ClusterSession(cluster)

    with session(gucs=burst_user_temp_support_gucs):
        yield


class BurstQueryCancelHelpers:
    @contextmanager
    def _temp_regular_user(self, cluster):
        with RedshiftDb(cluster.get_conn_params()) as db:
            with db.cursor() as cursor:
                cursor.execute("CREATE USER \"{}\" "
                               "WITH PASSWORD '{}'".format(
                                   self.TEMP_USER, "Testing1234"))
                cursor.execute('GRANT ALL ON DATABASE "{}" TO "{}"'.format(
                    "dev", self.TEMP_USER))
        yield
        with RedshiftDb(cluster.get_conn_params()) as db:
            with db.cursor() as cursor:
                cursor.execute('REVOKE ALL ON DATABASE "{}" FROM "{}"'.format(
                    "dev", self.TEMP_USER))
                cursor.execute('DROP USER "{}"'.format(self.TEMP_USER))

    def _clone_temp_table(self, db, schema):
        table_list = ["catalog_sales", "store_sales"]
        with db.cursor() as cursor:
            for table in table_list:
                log.info("Creating temp table clone for: {}.{}".format(
                    schema, table))
                clone_temp_table = table + "_temp"
                cursor.execute(
                    CREATE_TEMPORARY_TABLE_SQL.format(clone_temp_table, schema,
                                                      table))

    def _execute_query(self,
                       db,
                       query_uuid,
                       is_temp,
                       in_txn,
                       temp_table_dict=None):
        """
        Execute the test query.

        Args:
            db (RedshiftDb): Redshift database connection
            query_uuid (str): preset uuid of the query
        """
        with db.cursor() as cursor:
            log.info("Running execute query...")
            cursor.execute("set query_group to burst;")
            tbl_name = "catalog_sales"
            if is_temp:
                tbl_name += "_temp"
            if temp_table_dict:
                schema = temp_table_dict.get(tbl_name)
                tbl_name = "{}.{}".format(schema, tbl_name)
            query = ("select count(*) /* {} */ from "
                     "{};").format(query_uuid, tbl_name)
            if in_txn:
                cursor.execute("BEGIN")
            cursor.execute(query)
            if in_txn:
                cursor.execute("COMMIT")

    def _get_burst_cluster(self, burst_cluster_arn):
        profile_obj = Profiles.get_by_name(Profiles.QA_BURST_TEST.name)
        burst_client = RedshiftClient(profile=profile_obj, region=Regions.QA)
        burst_cluster_name = burst_cluster_arn.split(':cluster:')[-1].strip()
        burst_cluster = burst_client.describe_cluster(burst_cluster_name)
        return burst_cluster

    def _create_guard(self, cluster, guard_name):
        """
        Returns a ClusterXenGuardControl object.
        """
        return ClusterXenGuardControl(cluster, guard_name)

    def _execute_long_query(self,
                            db,
                            query_uuid,
                            is_temp,
                            in_txn,
                            temp_table_dict=None):
        """
        Execute the long test query.

        Args:
            db (RedshiftDb): Redshift database connection
            query_uuid (str): preset uuid of the query
        """
        with db.cursor() as cursor:
            log.info("Running long query...")
            cursor.execute("set query_group to burst;")
            tbl_name = "catalog_sales"
            if is_temp:
                tbl_name += "_temp"
            if temp_table_dict:
                schema = temp_table_dict.get(tbl_name)
                tbl_name = "{}.{}".format(schema, tbl_name)
            long_query = ("select count(*) /* {qid} */ from "
                          "{tbl} A, {tbl} B, {tbl} C;"
                          "").format(
                              qid=query_uuid, tbl=tbl_name)
            if in_txn:
                cursor.execute("BEGIN")
            cursor.execute(long_query)
            if in_txn:
                cursor.execute("COMMIT")
            log.info("Finished executing long query!")

    def _fetch_query(self,
                     db,
                     query_uuid,
                     limit,
                     is_temp,
                     in_txn,
                     temp_table_dict=None):
        """
        Execute the test query.

        Args:
            db (RedshiftDb): Redshift database connection
            query_uuid (str): preset uuid of the query
            limit (int): number of rows to fetch from query
                         (to simulate first fetch or next fetch)
        """
        with db.cursor() as cursor:
            log.info("Running fetch query...")
            cursor.execute("set query_group to burst;")
            tbl_name = "store_sales"
            if is_temp:
                tbl_name += "_temp"
            if temp_table_dict:
                schema = temp_table_dict.get(tbl_name)
                tbl_name = "{}.{}".format(schema, tbl_name)
            long_query = ("select * /* {} */ from "
                          "{} limit {};").format(query_uuid, tbl_name, limit)
            if in_txn:
                cursor.execute("BEGIN")
            cursor.execute(long_query)
            if limit == 1:
                cursor.fetchone()
            else:
                cursor.fetchall()
            if in_txn:
                cursor.execute("COMMIT")

    def _get_query_info(self, cluster, query_uuid, timeout_minutes):
        """
        Gets the query info from stv_inflight.

        Args:
            cluster (RedshiftCluster): Redshift cluster instance
            query_uuid (str): Uuid used for searching the query
            timeout_minutes (int): Timeout value for waiting the query to run

        Return:
            Tuple of (query id integer, process id integer, query text string)
        """
        query = ("select query, pid, text from stv_inflight "
                 "where text like '%/* {} */%' and label = 'burst' "
                 "and userid != 1;").format(query_uuid)
        delay_seconds = 10
        retries = timeout_minutes * 60 // delay_seconds
        for _ in range(retries):
            log.info("Getting query info...")
            result = run_bootstrap_sql(cluster, query)
            if result:
                log.info("Query info got!")
                assert (len(result) == 1), (
                    "Simultaneous query calls detected")
                return result[0]
            time.sleep(delay_seconds)
        raise RuntimeError("Timeout for waiting the query to run")

    def _verify_query_cancelled(self, cluster, query_uuid, timeout_minutes):
        """
        Wait for the query in the thread to cancel.

        Args:
            cluster (RedshiftCluster): Redshift cluster instance
            query_uuid (str): Uuid used for searching the query
            timeout_minutes (int): Timeout value for waiting the query to run
        """
        query = ("select query, pid, text from stv_inflight "
                 "where text like '%/* {} */%' and label = 'burst';"
                 "").format(query_uuid)

        delay_seconds = 10
        retries = timeout_minutes * 60 // delay_seconds

        if cluster.host_type == HostType.CLUSTER:
            db_session = DbSession(
                cluster.get_conn_params(),
                session_ctx=SessionContext(
                    user_type=SessionContext.SUPER,
                    host_type=HostType.CLUSTER))
        else:
            session_ctx = SessionContext(user_type='bootstrap')
            conn_params = cluster.get_conn_params()
            db_session = DbSession(conn_params, session_ctx=session_ctx)
        with db_session.cursor() as cursor:
            for _ in range(retries):
                log.info("Verifying that query is cancelled...")
                cursor.execute("set query_group to metrics;")
                cursor.execute(query)
                result = cursor.fetchall()
                if result:
                    time.sleep(delay_seconds)
                else:
                    log.info("Query is cancelled!")
                    return
        raise RuntimeError("Timeout for waiting the long query to cancel")

    def _verify_query_not_cancelled(self, cluster, query_uuid):
        query = ("select query, pid, text from stv_inflight "
                 "where text like '%/* {} */%' and label = 'burst' and "
                 "concurrency_scaling_status = 1;"
                 "").format(query_uuid)

        if cluster.host_type == HostType.CLUSTER:
            db_session = DbSession(
                cluster.get_conn_params(),
                session_ctx=SessionContext(
                    user_type=SessionContext.SUPER,
                    host_type=HostType.CLUSTER))
        else:
            session_ctx = SessionContext(user_type='bootstrap')
            conn_params = cluster.get_conn_params()
            db_session = DbSession(conn_params, session_ctx=session_ctx)
        with db_session.cursor() as cursor:
            log.info("Verifying that query is not cancelled...")
            cursor.execute("set query_group to metrics;")
            cursor.execute(query)
            result = cursor.fetchall()
            if result:
                log.info("Query is not cancelled!")
                return True
            else:
                raise RuntimeError("Query got cancelled")


@pytest.mark.load_tpcds_data
@pytest.mark.cluster_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_local_gucs(
    gucs=dict(
        list(burst_user_temp_support_gucs.items()) + list(CUSTOM_GUCS.items()))
)
@pytest.mark.custom_burst_gucs(
    gucs=dict(
        list(burst_user_temp_support_gucs.items()) + list(CUSTOM_GUCS.items()))
)
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstQueryTerminateAndRetry(BurstTempWrite, BurstQueryCancelHelpers):
    '''
    Put test_terminate_burst_query_cluster and test_retry_cancel_burst_query
    into a seperate class to avoid dimensions of hang_event in test_cancel_burst_query
    causing repeated test execution.
    '''

    @classmethod
    def modify_test_dimensions(cls):
        """
        List of simulated burst errors is the dimension.
        """
        return Dimensions({
            "is_temp": [True, False],
            "in_txn": [True, False]
        })

    def test_terminate_burst_query_cluster(self, cluster, vector):
        """
        Tests to see if a bursted query is able to be terminated properly.

        Args:
            db_session (DbSession): Redshift database session
            cluster (RedshiftCluster): Redshift cluster instance
        """
        self.TEMP_USER = "regular" + str(uuid.uuid4().hex)
        query_uuid = str(uuid.uuid4().hex)

        with self._temp_regular_user(cluster):
            db = RedshiftDb(cluster.get_conn_params())
            if vector.is_temp:
                self._clone_temp_table(db, "public")
            process = multiprocessing.Process(
                target=self._execute_long_query,
                args=(db, query_uuid, vector.is_temp, vector.in_txn))
            process.start()

            qid, pid, text = self._get_query_info(
                cluster, query_uuid, QUERY_RUNNING_TIMEOUT_MINUTES)

            # terminate query
            assert 'pid' in locals(), (
                "Query finished execution before termination")
            log.info(
                "Terminating query {}, pid {}: {}".format(qid, pid, text))
            log.info("Terminating test query; start...")
            query = "select pg_terminate_backend({});".format(pid)
            run_bootstrap_sql(cluster, query)
            log.info("Terminating test query; end...")
            self._verify_query_cancelled(
                cluster, query_uuid, QUERY_RUNNING_TIMEOUT_MINUTES)
            log.info("Verifying that no leaked sessions exists...")
            verify_no_leaked_sessions(cluster)
            log.info("No leaked sessions!")

    def test_retry_cancel_burst_query(self, cluster, vector):
        """
            Tests to see if a retry cancel can properly cancel bursted query.
        """
        self.TEMP_USER = "regular" + str(uuid.uuid4().hex)
        query_uuid = str(uuid.uuid4().hex)

        with self._temp_regular_user(cluster):
            burst_cluster_arns = cluster.list_acquired_burst_clusters()
            burst_cluster = self._get_burst_cluster(burst_cluster_arns[0])
            db = RedshiftDb(cluster.get_conn_params())
            if vector.is_temp:
                self._clone_temp_table(db, "public")
            process = multiprocessing.Process(
                target=self._execute_long_query,
                args=(db, query_uuid, vector.is_temp, vector.in_txn))
            process.start()
            try:
                wait_sec = 10
                burst_cluster.set_event(
                    'EtTestBurstQueryCancelRetry, wait = {}'.format(wait_sec))
                qid, pid, text = self._get_query_info(
                    cluster, query_uuid, QUERY_RUNNING_TIMEOUT_MINUTES)
                assert 'pid' in locals(), (
                    "Query finished execution before cancellation")
                log.info("Cancelling query {}, pid {}: {}".format(
                    qid, pid, text))
                query = "select pg_cancel_backend({});".format(pid)
                run_bootstrap_sql(cluster, query)
                log.info("Sent cancel request")
                # Verify first cancel try won't cancel the query as the event
                # 'EtTestBurstQueryCancelRetry' is set.
                self._verify_query_not_cancelled(cluster, query_uuid)
                # Verify qeury is cancelled as the retry cancel finally cancels
                # the query.
                maxcount = 100
                is_cancel = self._verify_query_not_cancelled(
                    cluster, query_uuid)
                while not is_cancel:
                    time.sleep(1)
                    is_cancel = self._verify_query_not_cancelled(
                        cluster, query_uuid)
                    maxcount -= 1
                    if (maxcount == 0):
                        pytest.fail("Query is not cancelled in the end.")
                self._verify_query_cancelled(cluster, query_uuid,
                                             QUERY_RUNNING_TIMEOUT_MINUTES)
            finally:
                burst_cluster.unset_event('EtTestBurstQueryCancelRetry')
                log.info("Verifying that no leaked sessions exists...")
                verify_no_leaked_sessions(cluster)
                log.info("No leaked sessions!")


@pytest.mark.load_tpcds_data
@pytest.mark.cluster_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_local_gucs(
    gucs=dict(
        list(burst_user_temp_support_gucs.items()) + list(CUSTOM_GUCS.items()))
)
@pytest.mark.custom_burst_gucs(
    gucs=dict(
        list(burst_user_temp_support_gucs.items()) + list(CUSTOM_GUCS.items()))
)
@pytest.mark.burst_precommit
@pytest.mark.serial_only
@pytest.mark.no_jdbc
class TestBurstQueryCancel(BurstTempWrite, BurstQueryCancelHelpers):
    @classmethod
    def modify_test_dimensions(cls):
        """
        List of simulated burst errors is the dimension.
        """
        return Dimensions({
            "xen_guard": [
                "burst:execute_query:after_prepare_request",
                "burst:execute_query:after_execute_request",
                "burst:execute_query:perform_request",
                "burst:internal_fetch:fetch_request",
                "burst:internal_fetch:perform_request"
            ],
            "is_temp": [True, False],
            "in_txn": [True, False]
        })

    def test_cancel_burst_query(self, cluster, vector):
        """
        Tests to see if a bursted query is able to be cancelled properly.

        Args:
            db_session (DbSession): Redshift database session
            cluster (RedshiftCluster): Redshift cluster instance
            hang_event (String): Event to set to simulate slowness for easy
                query cancelling at specific points
        """
        # set cluster to self.cluster for test:
        xen_guard = self._create_guard(cluster, vector.xen_guard)
        self.TEMP_USER = "regular" + str(uuid.uuid4().hex)
        query_uuid = str(uuid.uuid4().hex)

        with self._temp_regular_user(cluster):
            db = RedshiftDb(cluster.get_conn_params())
            if vector.is_temp:
                self._clone_temp_table(db, "public")
            log.info("Enabling xen_guard: {}".format(xen_guard.guardname))
            xen_guard.enable()

            if "fetch" in xen_guard.guardname:
                limit = 1
                process = multiprocessing.Process(
                    target=self._fetch_query,
                    args=(db, query_uuid, limit, vector.is_temp,
                          vector.in_txn))
                process.start()
            else:
                process = multiprocessing.Process(
                    target=self._execute_query,
                    args=(db, query_uuid, vector.is_temp, vector.in_txn))
                process.start()

            cancelled = False

            try:
                log.info("Sleeping for 5 seconds...")
                time.sleep(5)

                xen_guard.wait_until_process_blocks(
                    timeout_secs=QUERY_RUNNING_TIMEOUT_MINUTES * 60)

                qid, pid, text = self._get_query_info(
                    cluster, query_uuid, QUERY_RUNNING_TIMEOUT_MINUTES)

                # cancel query
                assert 'pid' in locals(), (
                    "Query finished execution before cancellation")
                log.info(
                    "Cancelling query {}, pid {}: {}".format(qid, pid, text))
                log.info("Cancelling test query; start...")
                query = "select pg_cancel_backend({});".format(pid)
                run_bootstrap_sql(cluster, query)
                log.info("Cancelling test query; end...")
                xen_guard.disable()

                self._verify_query_cancelled(
                    cluster, query_uuid, QUERY_RUNNING_TIMEOUT_MINUTES)
                cancelled = True
            finally:

                # terminate query if not cancelled
                if 'pid' in locals() and not cancelled:
                    log.info("Not cancelled: Terminating query; start...")
                    query = "select pg_terminate_backend({});".format(pid)
                    run_bootstrap_sql(cluster, query)
                    log.info("Not cancelled: Terminating query; end...")
                xen_guard.cleanup()
                log.info("Verifying that no leaked sessions exists...")
                verify_no_leaked_sessions(cluster)
                log.info("No leaked sessions!")



@pytest.mark.load_tpcds_data
@pytest.mark.cluster_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_local_gucs(
    gucs=dict(
        list(burst_user_temp_support_gucs.items()) +
        list(CUSTOM_GUCS_WITH_CANCEL.items())))
@pytest.mark.custom_burst_gucs(
    gucs=dict(
        list(burst_user_temp_support_gucs.items()) +
        list(CUSTOM_GUCS_WITH_CANCEL.items())))
@pytest.mark.serial_only
@pytest.mark.no_jdbc
class TestBurstQueryCancelMisc(BurstTest, BurstQueryCancelHelpers):

    def test_burst_query_handler_leak(self, cluster):
        """
        Test for Redshift-8897.
        Before the fix in the above SIM, on query cancel and a specific event
        setup, there was a burst query handler leak in the BurstManager list of
        active query handlers.
        This test verifies that exact issue repro works after the fix.
        """
        # Xen_guard to easily cancel the query.
        xen_guard = self._create_guard(cluster,
                                       "burst:execute_query:perform_request")
        # Event to cause a burst query handler leak issue.
        issue_event = "EtFakeBurstErrorAfterSnapIn2"

        self.TEMP_USER = "regular" + str(uuid.uuid4().hex)
        query_uuid = str(uuid.uuid4().hex)
        unsetEvent = True

        with self._temp_regular_user(cluster), xen_guard, cluster.event(
                issue_event):

            burst_cluster_arns = cluster.list_acquired_burst_clusters()
            burst_cluster = self._get_burst_cluster(burst_cluster_arns[0])
            db = RedshiftDb(cluster.get_conn_params())

            process = multiprocessing.Process(
                target=self._execute_query,
                args=(
                    db,
                    query_uuid,
                    False,  # is_tmp
                    False,  # in_txn
                ))
            process.start()

            xen_guard.wait_until_process_blocks(
                timeout_secs=QUERY_RUNNING_TIMEOUT_MINUTES * 60)

            try:
                qid, pid, text = self._get_query_info(
                    cluster, query_uuid, QUERY_RUNNING_TIMEOUT_MINUTES)
                # Set event on burst cluster.
                burst_cluster.set_event('EtTestBurstIgnoreCancel')
                assert 'pid' in locals(), (
                    "Query finished execution before cancellation")
                log.info("Cancelling query {}, pid {}: {}".format(
                    qid, pid, text))
                log.info("Cancelling test query; start...")
                query = "select pg_cancel_backend({});".format(pid)
                run_bootstrap_sql(cluster, query)
                log.info("Cancelling test query; end...")
                self._verify_query_cancelled(cluster, query_uuid,
                                             QUERY_RUNNING_TIMEOUT_MINUTES)
                cancelled = True
            finally:
                # terminate query if not cancelled
                if 'pid' in locals() and not cancelled:
                    log.info("Not cancelled: Terminating query; start...")
                    query = "select pg_terminate_backend({});".format(pid)
                    run_bootstrap_sql(cluster, query)
                    log.info("Not cancelled: Terminating query; end...")
                log.info("Verifying that no leaked sessions exists...")
                verify_no_leaked_sessions(cluster)
                log.info("No leaked sessions!")
