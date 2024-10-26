# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
from time import sleep
import threading
from contextlib import contextmanager
import operator

from raff.common.db.db_exception import Error
from raff.burst.burst_test import (setup_teardown_burst, BurstTest,
                                   verify_query_bursted, is_burst_hydration_on)
from raff.common.db.session import DbSession
from raff.dory.fixtures.sra import sra_force_miss  # noqa

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst, verify_query_bursted]

log = logging.getLogger(__name__)

ERR_MSG = {
    "Prepare": "Could not prepare cluster",
    "ConnectionLost": "Connection lost during execution",
    "Connection": "Could not execute query",
    "Fetch": "Could not execute query",
    "Execute": "Could not execute query"
}

CUSTOM_WLM_CONFIGS = ('[{"query_group":["burst"], "user_group":["burst"],'
                      '"concurrency_scaling":"auto", "query_concurrency": 1},'
                      '{"query_group":["burst"], "user_group":["burst"],'
                      '"concurrency_scaling":"auto", "query_concurrency": 1},'
                      '{"query_group":["noburst"], "user_group":["noburst"],'
                      '"query_concurrency": 5}]')

ENABLE_FAILURE_HANDLING_GUCS = {
    'wlm_json_configuration': CUSTOM_WLM_CONFIGS,
    'enable_burst_failure_handling': 'true',
    'enable_burst_spectrum': 'true',
    'spectrum_burst_retry_on_main': 'true',
}

CUSTOM_WLM_CONFIGS_ONE_SC = (
    '[{"query_group":["burst"], "user_group":["burst"],'
    '"concurrency_scaling":"auto", "query_concurrency": 1}]')

ENABLE_FAILURE_HANDLING_NO_TRY_BURST_FIRST_GUCS = {
    'try_burst_first': 'false',
    'wlm_json_configuration': CUSTOM_WLM_CONFIGS_ONE_SC,
    'enable_burst_failure_handling': 'true',
    'enable_burst_spectrum': 'true',
    'spectrum_burst_retry_on_main': 'true',
}

CUSTOM_WLM_CONFIGS_AUTO_WLM = (
    '[{"auto_wlm":true, "concurrency_scaling":"auto"}]')

ENABLE_FAILURE_HANDLING_WITH_AUTO_WLM_GUCS = {
    'try_burst_first': 'false',
    'autowlm_num_heavy_queries': '1',
    # Disable on-demand slot creation to avoid increased concurrency.
    'autowlm_enable_on_demand_slot_creation': 'false',
    'wlm_json_configuration': CUSTOM_WLM_CONFIGS_AUTO_WLM,
    'enable_burst_failure_handling': 'true',
    'enable_burst_spectrum': 'true',
    'spectrum_burst_retry_on_main': 'true',
}

DISABLE_FAILURE_HANDLING_GUCS = {
    'enable_burst_failure_handling': 'false',
    'enable_burst_spectrum': 'true',
    'spectrum_burst_retry_on_main': 'true',
}

WLM_BURST_ALWAYS_CONFIG = (
    '[{"query_group":["burst"], "user_group":["burst"], '
    '"concurrency_scaling":"always"}, {"query_group":["noburst"], '
    '"user_group":["noburst"], "query_concurrency": 5}]')

ALWAYS_BURST_GUCS = {
    'wlm_json_configuration': WLM_BURST_ALWAYS_CONFIG,
    'enable_burst_status_always': 'true',
    'enable_burst_failure_handling': 'true',
    'enable_burst_spectrum': 'true',
    'spectrum_burst_retry_on_main': 'true',
}


@pytest.mark.load_data
@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=ENABLE_FAILURE_HANDLING_GUCS)
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstManagerFailuresWithHandling(BurstTest):
    """Test burst manager failure handling."""

    def test_burst_succeeds(self, cluster, db_session, verify_query_bursted):
        """Tests burst succeeds before testing the failures."""
        self.execute_test_file('burst_query', session=db_session)

    @pytest.mark.parametrize("error_type",
                             ["Connection", "Execute", "ConnectionLost"])
    def test_failures(self, error_type, cluster, db_session):
        """
        Tests if a failure is introduced with EtSimulateBurstManagerError,
        the query is routed as a main cluster query.
        For this, we take a note of time before running the burst query
        then check in stl_wlm_query for Evicted query after that time.
        If we found an evicted query, then the test succeeds if
        the query was routed to be ran as a main query
        with the same xid, same task and same service class
        but with a different query id.
        """
        try:
            with db_session.cursor() as cursor:
                cursor.execute("SELECT EXTRACT(epoch FROM SYSDATE)")
                ts = cursor.fetch_scalar()
            cluster.set_event(
                'EtSimulateBurstManagerError, error={}'.format(error_type))
            self.execute_test_file('burst_query', session=db_session)
            with db_session.cursor() as cursor, self.db.cursor() as boot_curs:
                cursor.execute("set query_group to noburst")
                query = ("select xid, task, query, service_class "
                         "from stl_wlm_query "
                         "where final_state = 'Evicted' and "
                         "EXTRACT(epoch FROM service_class_start_time) >= {}"
                         "".format(ts))
                cursor.execute(query)
                assert cursor.rowcount == 1
                evicted_row = cursor.fetchone()
                log.info(evicted_row)
                xid = evicted_row[0]
                task = evicted_row[1]
                qid = evicted_row[2]
                sc = evicted_row[3]
                query = ("select s.concurrency_scaling_status,"
                         "s.concurrency_scaling_status_txt from "
                         "stl_query q join "
                         "svl_query_concurrency_scaling_status s on "
                         "q.query = s.query "
                         "where q.xid = {} and "
                         "q.query = {} and "
                         "q.aborted = 1 and "
                         "EXTRACT(epoch FROM q.starttime) >= {}"
                         "".format(xid, qid, ts))
                boot_curs.execute(query)
                assert boot_curs.rowcount == 1
                result = boot_curs.fetchone()
                status = result[0]
                assert status == 29
                query = ("select xid, task, query, service_class "
                         "from stl_wlm_query "
                         "where final_state = 'Completed' "
                         "and xid = {} "
                         "and task = {} "
                         "and query > {} "
                         "and service_class = {}".format(xid, task, qid, sc))
                cursor.execute(query)
                assert cursor.rowcount == 1
                log.info(cursor.fetchone())
        finally:
            cluster.unset_event('EtSimulateBurstManagerError')

    @pytest.mark.parametrize("error_type",
                             ["Connection", "Execute", "ConnectionLost"])
    def test_restart_at_failures(self, error_type, cluster, db_session):
        """
        Tests if restart signal cause the query to be executed in
        the next service class.
        For this, we test the same way above but check if the service
        class after the restart is bigger than the first time.
        """
        try:
            with db_session.cursor() as cursor:
                cursor.execute("SELECT EXTRACT(epoch FROM SYSDATE)")
                ts = cursor.fetch_scalar()
            cluster.set_event(
                'EtSimulateBurstManagerError, error={}'.format(error_type))
            cluster.set_event('EtSimulateRestartAtBmFailure')
            self.execute_test_file('burst_query', session=db_session)
            with db_session.cursor() as cursor:
                cursor.execute("set query_group to noburst")
                query = ("select xid, task, query, service_class "
                         "from stl_wlm_query "
                         "where final_state = 'Evicted' and "
                         "EXTRACT(epoch FROM service_class_start_time) >= {}"
                         "".format(ts))
                cursor.execute(query)
                assert cursor.rowcount == 1
                evicted_row = cursor.fetchone()
                log.info(evicted_row)
                xid = evicted_row[0]
                task = evicted_row[1]
                qid = evicted_row[2]
                sc = evicted_row[3]
                query = ("select concurrency_scaling_status from stl_query "
                         "where xid = {} and "
                         "query = {} and "
                         "aborted = 1 and "
                         "EXTRACT(epoch FROM starttime) >= {}"
                         "".format(xid, qid, ts))
                cursor.execute(query)
                assert cursor.rowcount == 1
                result = cursor.fetchone()
                status = result[0]
                assert status == 29
                query = ("select xid, task, query, service_class "
                         "from stl_wlm_query "
                         "where final_state = 'Completed' "
                         "and xid = {} "
                         "and task = {} "
                         "and query > {} "
                         "and service_class > {}".format(xid, task, qid, sc))
                cursor.execute(query)
                assert cursor.rowcount == 1
                log.info(cursor.fetchone())
        finally:
            cluster.set_event('EtSimulateRestartAtBmFailure')
            cluster.unset_event('EtSimulateBurstManagerError')

    @pytest.mark.parametrize("error_type",
                             ["Connection", "Execute", "ConnectionLost"])
    def test_abort_at_failures(self, error_type, cluster, db_session):
        """Tests that abort aborts the burst query"""
        try:
            with db_session.cursor() as cursor:
                cursor.execute("SELECT EXTRACT(epoch FROM SYSDATE)")
                ts = cursor.fetch_scalar()
            cluster.set_event(
                'EtSimulateBurstManagerError, error={}'.format(error_type))
            cluster.set_event('EtSimulateAbortAtBmFailure')
            with db_session.cursor() as cursor, self.db.cursor() as boot_curs:
                ERR_MSG = "abort query"
                try:
                    cursor.execute("set query_group to burst")
                    cursor.execute("SELECT count(*) FROM catalog_sales A;")
                    pytest.fail("Query should fail as {}".format(ERR_MSG))
                except Error as error:
                    assert ERR_MSG in error.pgerror
                    cursor.execute("set query_group to noburst")
                    query = (
                        "select xid, task, query, service_class "
                        "from stl_wlm_query "
                        "where final_state = 'Evicted' and "
                        "EXTRACT(epoch FROM service_class_start_time) >= {}"
                        "".format(ts))
                    cursor.execute(query)
                    assert cursor.rowcount == 1
                    evicted_row = cursor.fetchone()
                    log.info(evicted_row)
                    xid = evicted_row[0]
                    qid = evicted_row[2]
                    query = ("select s.concurrency_scaling_status,"
                             "s.concurrency_scaling_status_txt from "
                             "stl_query q join "
                             "svl_query_concurrency_scaling_status s on "
                             "q.query = s.query "
                             "where q.xid = {} and "
                             "q.query = {} and "
                             "q.aborted = 1 and "
                             "EXTRACT(epoch FROM q.starttime) >= {}"
                             "".format(xid, qid, ts))
                    boot_curs.execute(query)
                    assert boot_curs.rowcount == 1
                    result = boot_curs.fetchone()
                    status = result[0]
                    assert status == 29
        finally:
            cluster.unset_event('EtSimulateAbortAtBmFailure')
            cluster.unset_event('EtSimulateBurstManagerError')

    def test_no_failure_handling_for_internal_fetching(self, cluster,
                                                       db_session):
        """
        Tests that in case of burst query already started to return and
        some rows are already consumed, we don't rerun as a main query.
        """
        error_type = 'Fetch'
        try:
            cluster.set_event(
                'EtSimulateBurstManagerError, error={}'.format(error_type))
            with db_session.cursor() as cursor:
                try:
                    cursor.execute("set query_group to burst")
                    cursor.execute("SELECT * FROM catalog_sales A;")
                    pytest.fail("Query should fail as {}".format(
                        ERR_MSG[error_type]))
                except Error as error:
                    assert ERR_MSG[error_type] in error.pgerror
                qid = self.last_query_id(cursor)
                query = ("select concurrency_scaling_status from "
                         "stl_query where query = {}".format(qid))
                cursor.execute(query)
                assert cursor.rowcount == 1
                result = cursor.fetchone()
                status = result[0]
                assert status == 27
        finally:
            cluster.unset_event('EtSimulateBurstManagerError')

    @pytest.mark.parametrize("error_type",
                             ["Connection", "Execute", "ConnectionLost"])
    def test_abort_is_preferred_over_restart_at_failures(
            self, error_type, cluster, db_session):
        """
        Tests that if both abort and restart signal is there,
        abort gets precedence over restart
        """
        try:
            cluster.set_event(
                'EtSimulateBurstManagerError, error={}'.format(error_type))
            cluster.set_event('EtSimulateAbortAtBmFailure')
            cluster.set_event('EtSimulateRestartAtBmFailure')
            with db_session.cursor() as cursor, self.db.cursor() as boot_curs:
                cursor.execute("SELECT EXTRACT(epoch FROM SYSDATE)")
                ts = cursor.fetch_scalar()
                ERR_MSG = "abort query"
                try:
                    cursor.execute("set query_group to burst")
                    cursor.execute("SELECT count(*) FROM catalog_sales A;")
                    pytest.fail("Query should fail as {}".format(ERR_MSG))
                except Error as error:
                    assert ERR_MSG in error.pgerror
                cursor.execute("set query_group to noburst")
                query = (
                    "select xid, task, query, service_class "
                    "from stl_wlm_query "
                    "where final_state = 'Evicted' and "
                    "EXTRACT(epoch FROM service_class_start_time) >= {}"
                    "".format(ts))
                cursor.execute(query)
                assert cursor.rowcount == 1
                evicted_row = cursor.fetchone()
                qid = evicted_row[2]
                query = ("select concurrency_scaling_status,"
                         "concurrency_scaling_status_txt from "
                         "svl_query_concurrency_scaling_status "
                         "where query = {}".format(qid))
                boot_curs.execute(query)
                assert boot_curs.rowcount == 1
                result = boot_curs.fetchone()
                status = result[0]
                assert status == 29
        finally:
            cluster.unset_event('EtSimulateRestartAtBmFailure')
            cluster.unset_event('EtSimulateAbortAtBmFailure')
            cluster.unset_event('EtSimulateBurstManagerError')

    @pytest.mark.skip(reason="rsqa-14302")
    def test_handle_failure_redshift_format_errors(self, db_session, cluster):
        """
        If we hit error during scanning Redshift format data, we need to retry
        on main.
        """
        if is_burst_hydration_on(cluster):
            pytest.skip("Hydration build doesn't use redshift format. "
                        "See https://issues.amazon.com/issues/Burst-2968")
        with cluster.event("EtSpectrumSystemFailure", execute=1, format=9):
            self.execute_test_file(
                'handle_error_in_redshift_format', session=db_session)

    @pytest.mark.usefixtures("sra_force_miss")
    def test_handle_failure_parquet_format_errors(self, db_session, cluster):
        """
        If we hit error during scanning Parquet external tables via Spectrum
        we don't retry.
        """
        with cluster.event("EtSpectrumSystemFailure", execute=1, format=4):
            self.execute_test_file(
                'handle_error_in_parquet_format', session=db_session)

    @pytest.mark.skip(reason="rsqa-14303")
    @pytest.mark.usefixtures("sra_force_miss")
    def test_handle_failure_any_format_errors(self, db_session, cluster):
        """
        If we hit error during scanning S3 external tables and possibly
        Redshift data, we can only retry if we saw the Redshift scanning
        error first.
        """
        if is_burst_hydration_on(cluster):
            pytest.skip("Hydration build doesn't use redshift format. "
                        "See https://issues.amazon.com/issues/Burst-2968")
        with cluster.event("EtSpectrumSystemFailure", execute=1, format=-1):
            self.execute_test_file(
                'handle_error_in_any_format', session=db_session)

    def test_first_fetch_can_retry(self, db_session, cluster):
        """
        If we hit error during the first fetch when we didn't return any row
        we should be allowed to retry on main
        """
        with db_session.cursor() as cursor:
            cursor.execute("begin")
            # query similar to https://tt.amazon.com/0512777664
            cursor.execute("""declare \"SQL_CUR7\" cursor with hold for
                              SELECT count(*) FROM catalog_sales A;""")
            # First query is begin transaction, second query is declare cursor,
            # third query is the actual fetch
            # (see burst_client.cpp::ExecuteFirstFetch)
            with cluster.event("EtFailNthBurstQueries", frequency=3):
                cursor.execute('select pg_backend_pid()')
                pid = cursor.fetch_scalar()
                log.info("Bursting query from pid {}".format(pid))
                cursor.execute("set query_group to burst")
                cursor.execute("fetch 10000 in \"SQL_CUR7\";")
                cursor.fetchall()
                # We need to order by starttime because the last query
                # is the retry on main
                burst_query = """ select query from stl_query
                                  where querytxt like '%fetch 10000%'
                                  and pid = {} order by starttime limit 1
                              """.format(pid)
                cursor.execute(burst_query)
                qid = cursor.fetch_scalar()
                # 29 = Elegible to rerun on main cluster
                self.verify_query_status(cluster, qid, 29)


class TestBurstManagerFailuresWithTryBurstFirstOff(BurstTest):
    """
    Base class to test burst manager failure handling with
    try_burst_first off.
    """

    def run_long_query(self, db_session, querytxt):
        try:
            with db_session.cursor() as cursor:
                cursor.execute(
                    "set query_group to burst; " + querytxt
                )
        except Exception as e:
            log.info("Expected to be killed: " + str(e))

    def kill_all_running_queries(self):
        with self.db.cursor() as cursor:
            cursor.execute("set query_group to metrics;")
            cursor.execute(
                "select pid from stv_inflight A, stv_wlm_query_state B "
                "where A.query = B.query and (B.service_class = 6 or "
                "B.service_class >= 100) and "
                "A.label = 'burst'")
            rows = cursor.fetchall()
            for row in rows:
                pid = row[0]
                log.info("Killing {}".format(pid))
                cursor.execute("select pg_terminate_backend({})".format(pid))

    def get_db_session(self, cluster):
        db_session = DbSession(cluster.get_conn_params())
        with db_session.cursor() as cursor, self.db.cursor() as cursor_db:
            # Get regular user name.
            user = db_session.session_ctx.username
            # Get the user id of the regular user.
            cursor_db.execute(
                "select usesysid from pg_user "
                "where usename = '{}'".format(user)
            )
            userid = cursor_db.fetch_scalar()
            log.info("Regular user id: {}".format(userid))
            cursor_db.execute(
                "GRANT ALL ON ALL TABLES IN SCHEMA public "
                "to {}".format(user)
            )
            cursor.close()
        return db_session

    @contextmanager
    def long_running_context(self, cluster):
        querytxt = \
            "select count(*) from web_sales A, web_sales B, web_sales C"
        try:
            thread = threading.Thread(
                target=self.run_long_query,
                args=(self.get_db_session(cluster), querytxt))
            thread.start()
            yield thread
        finally:
            self.kill_all_running_queries()

    def is_query_running(self, db_session, ts):
        with db_session.cursor() as cursor:
            is_running = 0
            trial = 30
            while is_running == 0 and trial > 0:
                query = ("select count(*) from stv_inflight "
                         "where starttime >= '{}'")
                cursor.execute("set query_group to metrics;")
                cursor.execute(query.format(ts))
                is_running = cursor.fetch_scalar()
                trial -= 1
                sleep(1)
            if is_running > 0:
                return True
            else:
                return False

    def is_query_evicted_then_queued(self, db_session, ts):
        # Make sure the query is evicted first
        with db_session.cursor() as cursor:
            evicted_count = 0
            trial = 30
            while evicted_count == 0 and trial > 0:
                query = ("select xid, task, service_class "
                         "from stl_wlm_query "
                         "where final_state = 'Evicted' and "
                         "service_class_start_time >= '{}'")
                cursor.execute("set query_group to metrics;")
                cursor.execute(query.format(ts))
                evicted_count = cursor.rowcount
                if evicted_count > 0:
                    evicted_row = cursor.fetchone()
                    log.info(evicted_row)
                    xid = evicted_row[0]
                    task = evicted_row[1]
                    sc = evicted_row[2]
                trial -= 1
                sleep(1)
            if evicted_count == 0:
                pytest.fail("Burst query didn't get evicted.")

        # Now the query should be queued waiting.
        with db_session.cursor() as cursor:
            is_queued = 0
            trial = 30
            while is_queued == 0 and trial > 0:
                query = ("select count(*) from stv_wlm_query_state "
                         "where xid = {} "
                         "and task = {} "
                         "and service_class = {} "
                         "and state = 'QueuedWaiting' ")
                cursor.execute("set query_group to metrics;")
                cursor.execute(query.format(xid, task, sc))
                is_queued = cursor.fetch_scalar()
                trial -= 1
                sleep(1)
            if is_queued == 0:
                pytest.fail("Burst query didn't queue.")


@pytest.mark.load_data
@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(
    gucs=ENABLE_FAILURE_HANDLING_NO_TRY_BURST_FIRST_GUCS)
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstManagerFailuresWithoutAutoWLM(
        TestBurstManagerFailuresWithTryBurstFirstOff):
    """
    Test burst manager failure handling with try_burst_first off and
    no auto wlm.
    """

    @pytest.mark.parametrize("error_type",
                             ["Connection", "Execute", "ConnectionLost"])
    def test_failures_queued_waiting(self, error_type, cluster, db_session):
        """
        Tests if a failure is introduced with EtSimulateBurstManagerError,
        the query is routed as a main cluster query.
        But since the queue is full, the burst query should be in queued
        waiting state.
        """
        try:
            with db_session.cursor() as cursor:
                cursor.execute("SELECT SYSDATE")
                ts = cursor.fetch_scalar()

            with self.long_running_context(cluster):
                if not self.is_query_running(self.db, ts):
                    pytest.fail("Long query didn't run.")

                cluster.set_event(
                    'EtSimulateBurstManagerError, error={}'.format(error_type))

                with db_session.cursor() as cursor:
                    cursor.execute("SELECT SYSDATE")
                    ts = cursor.fetch_scalar()

                burst_thread = threading.Thread(
                    target=self.execute_test_file,
                    args=('burst_query', db_session))
                burst_thread.daemon = True
                burst_thread.start()

                self.is_query_evicted_then_queued(self.db, ts)

            # Once we are out of long_running_context, burst query will
            # start running. let's wait for it to finish. If it doesn't
            # finish in 30 seconds, it will eventually be killed in "finally".
            burst_thread.join(30)
        finally:
            # Kill any left over queries.
            self.kill_all_running_queries()
            cluster.unset_event('EtSimulateBurstManagerError')


@pytest.mark.load_data
@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(
    gucs=ENABLE_FAILURE_HANDLING_WITH_AUTO_WLM_GUCS)
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstManagerFailuresWithAutoWLM(
        TestBurstManagerFailuresWithTryBurstFirstOff):
    @pytest.mark.parametrize("error_type",
                             ["Connection", "Execute", "ConnectionLost"])
    def test_failures_queued_waiting(self, error_type, cluster, db_session):
        """
        Tests if a failure is introduced with EtSimulateBurstManagerError,
        the query is routed as a main cluster query.
        But since the queue is full, the burst query should be in queued
        waiting state.
        """
        try:
            with db_session.cursor() as cursor:
                cursor.execute("SELECT SYSDATE")
                ts = cursor.fetch_scalar()

            with self.long_running_context(cluster):
                if not self.is_query_running(self.db, ts):
                    pytest.fail("Long query didn't run.")

                cluster.set_event(
                    'EtSimulateBurstManagerError, error={}'.format(error_type))

                with db_session.cursor() as cursor:
                    cursor.execute("SELECT SYSDATE")
                    ts = cursor.fetch_scalar()

                burst_thread = threading.Thread(
                    target=self.execute_test_file,
                    args=('burst_query', db_session))
                burst_thread.daemon = True
                burst_thread.start()

                self.is_query_evicted_then_queued(self.db, ts)

            # Once we are out of long_running_context, burst query will
            # start running. let's wait for it to finish. If it doesn't
            # finish in 30 seconds, it will eventually be killed in "finally".
            burst_thread.join(30)
        finally:
            # Kill any left over queries.
            self.kill_all_running_queries()
            cluster.unset_event('EtSimulateBurstManagerError')


@pytest.mark.load_data
@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=DISABLE_FAILURE_HANDLING_GUCS)
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstManagerFailuresWithoutHandling(BurstTest):
    def test_burst_succeeds(self, cluster, db_session, verify_query_bursted):
        self.execute_test_file('burst_query', session=db_session)

    @pytest.mark.parametrize("error_type",
                             ["Connection", "Execute", "ConnectionLost"])
    def test_failures(self, error_type, cluster, db_session):
        # Tests that burst manager failures are thrown as it is
        # and there is no failure handling
        try:
            cluster.set_event(
                'EtSimulateBurstManagerError, error={}'.format(error_type))
            with db_session.cursor() as cursor:
                try:
                    cursor.execute("set query_group to burst")
                    cursor.execute("SELECT count(*) FROM catalog_sales A;")
                    pytest.fail("Query should fail as {}".format(
                        ERR_MSG[error_type]))
                except Error as error:
                    assert ERR_MSG[error_type] in error.pgerror
        finally:
            cluster.unset_event('EtSimulateBurstManagerError')

    def get_scan_type_count(self):
        """
        Helper method to get the number of rows recorded in stl_s3log group
        by scan_type.
        """
        scan_type_query = """select scan_type, count(1) from stl_s3log where message like
          '%Spectrum Scan Error.%' group by scan_type order by
          scan_type"""
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(scan_type_query)
            return bootstrap_cursor.fetchall()

    def compare_one_scan_type(self, pre_dict, post_dict, scan_type, op_func,
                              count):
        pre_count = pre_dict[scan_type] if scan_type in pre_dict else 0
        post_count = post_dict[scan_type] if scan_type in post_dict else 0

        diff = post_count - pre_count
        assert op_func(diff, count), str(pre_dict) + " => " + str(post_dict)

    @contextmanager
    def verify_scan_type_logging(self, unspec_op, unspec_count, internal_op,
                                 internal_count, external_op, external_count):
        """
        Verifies that correct scan_type is recorded in stl_s3log.
        """
        pre_tup = self.get_scan_type_count()
        yield
        post_tup = self.get_scan_type_count()

        pre_dict = dict(pre_tup)
        post_dict = dict(post_tup)

        self.compare_one_scan_type(pre_dict, post_dict, 0, unspec_op,
                                   unspec_count)
        self.compare_one_scan_type(pre_dict, post_dict, 1, internal_op,
                                   internal_count)
        self.compare_one_scan_type(pre_dict, post_dict, 2, external_op,
                                   external_count)

    def test_disable_handle_failure_redshift_format_errors(
            self, db_session, cluster):
        """
        If we hit error during scanning Redshift format data, we print a
        different error than hitting Spectrum external table reading error.
        """
        if is_burst_hydration_on(cluster):
            pytest.skip("Hydration build doesn't use redshift format. "
                        "See https://issues.amazon.com/issues/Burst-2968")
        # We only expect to see Spectrum error (external scan type).
        with self.verify_scan_type_logging(operator.eq, 0, operator.gt, 0,
                                           operator.eq, 0):
            with cluster.event(
                   "EtSpectrumSystemFailure", execute=1, format=9):
                self.execute_test_file(
                    'disable_handle_failure_redshift_format',
                    session=db_session)

    @pytest.mark.usefixtures("sra_force_miss")
    def test_disable_handle_failure_parquet_format_errors(
            self, db_session, cluster):
        """
        If we hit error during scanning Spectrum external table data, we print
        a different error than hitting error during Redshift format data.
        """
        # We only expect to see Redshift error (internal scan type).
        with self.verify_scan_type_logging(operator.eq, 0, operator.eq, 0,
                                           operator.gt, 0):
            with cluster.event(
                   "EtSpectrumSystemFailure", execute=1, format=4):
                self.execute_test_file(
                    'disable_handle_failure_parquet_format',
                    session=db_session)

    @pytest.mark.usefixtures("sra_force_miss")
    def test_disable_handle_failure_any_format_errors(self, db_session,
                                                      cluster):
        """
        When we are likely to hit either Redshift format or external table
        read error, we need to verify that the error message corresponds to
        the error that we first hit.
        """
        if is_burst_hydration_on(cluster):
            pytest.skip("Hydration build doesn't use redshift format. "
                        "See https://issues.amazon.com/issues/Burst-2968")
        # We expect to see both Redshift error (internal scan type) and
        # Spectrum error (external scan type) but no "unspecified" scan type.
        with self.verify_scan_type_logging(operator.eq, 0, operator.gt, 0,
                                           operator.gt, 0):
            with cluster.event(
                   "EtSpectrumSystemFailure", execute=1, format=-1):
                self.execute_test_file(
                    'disable_handle_failure_any_format', session=db_session)


@pytest.mark.load_data
@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=ALWAYS_BURST_GUCS)
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstManagerFailuresWithAlwaysBursting(BurstTest):
    def test_burst_succeeds(self, cluster, db_session, verify_query_bursted):
        self.execute_test_file('burst_query', session=db_session)

    @pytest.mark.parametrize("error_type",
                             ["Connection", "Execute", "ConnectionLost"])
    def test_failures(self, error_type, cluster, db_session):
        """
        Tests that burst manager failures are thrown as it is
        and there is no failure handling
        """
        try:
            cluster.set_event(
                'EtSimulateBurstManagerError, error={}'.format(error_type))
            with db_session.cursor() as cursor:
                try:
                    cursor.execute("set query_group to burst")
                    cursor.execute("SELECT count(*) FROM catalog_sales A;")
                    pytest.fail("Query should fail as {}".format(
                        ERR_MSG[error_type]))
                except Error as error:
                    assert ERR_MSG[error_type] in error.pgerror
        finally:
            cluster.unset_event('EtSimulateBurstManagerError')
