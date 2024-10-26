# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest

from contextlib import contextmanager

from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst
from raff.common.db.db_exception import Error

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst"]

BEGIN_Q = "BEGIN;"
COMMIT_Q = "COMMIT;"
DECLARE_Q = """declare c1 {} cursor for
select top 30 distinct ss_sold_time_sk
from store_sales
    ,household_demographics
    ,time_dim, store
where ss_sold_time_sk = time_dim.t_time_sk
    and ss_hdemo_sk = household_demographics.hd_demo_sk
    and ss_store_sk = s_store_sk
    and time_dim.t_hour = 8
    and time_dim.t_minute >= 30
    and household_demographics.hd_dep_count = 5
    and store.s_store_name = 'ese'
order by ss_sold_time_sk
;
"""

FETCH_NEXT_Q = "FETCH FORWARD {} FROM c1;"
FETCH_ALL_Q = "FETCH ALL FROM c1;"

Result = [
    30600, 30612, 30622, 30643, 30671, 30691, 30741, 30762, 30765, 30768,
    30769, 30781, 30796, 30801, 30806, 30811, 30824, 30869, 30878, 30941,
    30952, 30962, 30975, 31002, 31003, 31036, 31040, 31058, 31147, 31159
]

CUSTOM_GUCS = {
    # Only fetch 1 row at a time from burst. Useful to test sticky session
    # and to simulate exception during a subsequent fetch.
    'burst_cursor_prefetch_rows': '1',
    'enable_burst_result_cache': 'false',
}


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS)
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstCursor(BurstTest):
    @contextmanager
    def event_context(self, cluster, event_name, **kwargs):
        """
        Set event and unset on exit.
        """
        try:
            cluster.set_event(
                event_name + ', {}'.format(', '.join(key + '=' + kwargs[key]
                                                     for key in kwargs)))
            yield
        finally:
            cluster.unset_event(event_name)

    def clear_result_cache(self, cluster):
        cluster.run_xpx('clear_result_cache')

    @pytest.mark.no_jdbc
    def test_burst_cursor_success(self, cursor, db_session):
        """
        Tests that multiple fetches from the same cursor with valid fetch
        counter works correctly. This tests sticky session implementation as
        well as correctness of burst cursor execution.
        """
        assert len(Result) == 30
        start_idx = 0
        end_idx = 1

        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(BEGIN_Q)
            cursor.execute(DECLARE_Q.format(""))
            count = 1
            cursor.execute(FETCH_NEXT_Q.format(count))
            flattened_result = list(sum(cursor.fetchall(), ()))
            assert flattened_result == Result[start_idx:end_idx]

            count = 5
            start_idx = end_idx
            end_idx = start_idx + count
            cursor.execute(FETCH_NEXT_Q.format(count))
            flattened_result = list(sum(cursor.fetchall(), ()))
            assert flattened_result == Result[start_idx:end_idx]

            start_idx = end_idx
            end_idx = len(Result)
            cursor.execute(FETCH_ALL_Q)
            flattened_result = list(sum(cursor.fetchall(), ()))
            assert flattened_result == Result[start_idx:end_idx]

            cursor.execute(FETCH_NEXT_Q.format(count))
            assert cursor.fetchall() == []

            cursor.execute(FETCH_ALL_Q)
            flattened_result = list(sum(cursor.fetchall(), ()))
            assert flattened_result == []
            cursor.execute(COMMIT_Q)

    def fetch_error_test(self, cluster, cursor, db_session, is_scroll,
                         fetch_count):
        self.clear_result_cache(cluster)

        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(BEGIN_Q)
            scroll_string = ""
            if is_scroll:
                scroll_string = "SCROLL"
            cursor.execute(DECLARE_Q.format(scroll_string))
            # Execute a non-zero fetch query to force it to burst. If the very
            # first fetch count is negative, PADB simply refuses to execute
            # and returns empty rows.
            cursor.execute(FETCH_NEXT_Q.format(1))
            with pytest.raises(Exception):
                cursor.execute(FETCH_NEXT_Q.format(fetch_count))
            cursor.execute(COMMIT_Q)

    @pytest.mark.no_jdbc
    def test_burst_cursor_error(self, cluster, cursor, db_session):
        """
        Tests that we correctly error out for negative offset for burst cursors
        regardless of whether they are scrolling or not. Also tests that
        non-scrollable cursors error out with negative offset even without
        burst.
        """
        # Negative 1 without SCROLL should error out in regular mode.
        self.fetch_error_test(cluster, cursor, db_session, False, -1)
        # Negative 1 with SCROLL will hit burst error.
        self.fetch_error_test(cluster, cursor, db_session, True, -1)

        # 0. Should error out both for scroll and no scroll.
        self.fetch_error_test(cluster, cursor, db_session, False, 0)
        self.fetch_error_test(cluster, cursor, db_session, True, 0)

    @pytest.mark.no_jdbc
    def test_burst_cursor_with_fetch_error(self, cluster, db_session):
        """
        Tests that runs multiple fetches from the same cursor but introduces
        an exception after first fetch to simulate an exception during utility
        statement.
        """
        self.clear_result_cache(cluster)
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(BEGIN_Q)
            cursor.execute(DECLARE_Q.format(""))
            count = 5
            start_idx = 0
            end_idx = count
            cursor.execute(FETCH_NEXT_Q.format(count))
            flattened_result = list(sum(cursor.fetchall(), ()))
            assert flattened_result == Result[start_idx:end_idx]

            # Now introduce error.
            with self.event_context(
                    cluster, "EtSimulateBurstManagerError", error="Execute"):
                with pytest.raises(Exception):
                    cursor.execute(FETCH_NEXT_Q.format(10))
            # Also terminate the error transaction to avoid hitting
            # 'current transaction is aborted' when we try to unset query_group
            # from the burst session.
            cursor.execute(COMMIT_Q)

    @pytest.mark.no_jdbc
    def test_burst_2113_error_first_fetch(self, cluster, db_session):
        """
        Tests that runs a BEGIN that successfully completes, but the first
        fetch errors out on the burst side. This tests if the success code
        path correctly restores a PG exception stack so that successive
        failures can correctly long jump without hitting SIG11.
        """

        run_query_error = "Burst simulated run_query error"
        self.clear_result_cache(cluster)
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(BEGIN_Q)
            cursor.execute(DECLARE_Q.format(""))

            # Now introduce error.
            with self.event_context(cluster, "EtBurstRunQueryError"):
                try:
                    cursor.execute(FETCH_NEXT_Q.format(1))
                    pytest.fail(
                        "Query should fail as {}".format(run_query_error))
                except Error as error:
                    assert run_query_error in error.pgerror

                with pytest.raises(Exception):
                    cursor.execute(FETCH_NEXT_Q.format(1))
            # Also terminate the error transaction to avoid hitting
            # 'current transaction is aborted' when we try to unset query_group
            # from the burst session.
            cursor.execute(COMMIT_Q)

    @pytest.mark.no_jdbc
    def test_burst_2113_error_third_fetch(self, cluster, db_session):
        """
        Tests that runs a BEGIN and the two more fetches where all successfully
        complete, but the third fetch errors out on the burst side. This tests
        if the success code path correctly restores a PG exception stack even
        we submit utility queries directly from Postgres side and that
        successive failures can correctly long jump without hitting SIG11.
        """

        serde_error = "Incompatible serializer version"
        self.clear_result_cache(cluster)
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(BEGIN_Q)
            cursor.execute(DECLARE_Q.format(""))
            # First fetch, should complete successfully.
            cursor.execute(FETCH_NEXT_Q.format(1))
            # Second fetch, should complete successfully.
            cursor.execute(FETCH_NEXT_Q.format(1))

            # Now introduce error. Instead of using EtBurstRunQueryError
            # as we did for test_burst_2113_error_first_fetch, we are using
            # serde version mistmatch error event. This is because, the cursor
            # is already executed and cached on the burst side during first
            # fetch and we can no longer hit run_query exception there. Note:
            # this event only works for this test. Using the
            # EtBurstFailSerDeVersion for test_burst_2113_error_first_fetch
            # will not result in error as the BEGIN itself will error out
            # (BEGIN is issued implicitly for first fetch) and we need at least
            # one success run along with first fetch failure to exercise the
            # SIG11 code path for test_burst_2113_error_first_fetch.
            with self.event_context(cluster, "EtBurstFailSerDeVersion"):
                try:
                    cursor.execute(FETCH_NEXT_Q.format(1))
                    pytest.fail("Query should fail as {}".format(serde_error))
                except Error as error:
                    assert serde_error in error.pgerror

                with pytest.raises(Exception):
                    cursor.execute(FETCH_NEXT_Q.format(1))
            # Also terminate the error transaction to avoid hitting
            # 'current transaction is aborted' when we try to unset query_group
            # from the burst session.
            cursor.execute(COMMIT_Q)

    @pytest.mark.no_jdbc
    def test_burst_cursor_with_commit_error(self, cluster, db_session):
        """
        Tests that runs multiple fetches from the same cursor but introduces
        an exception after first fetch to simulate an exception during utility
        statement.
        """
        self.clear_result_cache(cluster)
        with self.burst_db_cursor(db_session) as cursor:
            cursor.execute(BEGIN_Q)
            cursor.execute(DECLARE_Q.format(""))
            count = 5
            start_idx = 0
            end_idx = count
            cursor.execute(FETCH_NEXT_Q.format(count))
            flattened_result = list(sum(cursor.fetchall(), ()))
            assert flattened_result == Result[start_idx:end_idx]

            # Now introduce error.
            with self.event_context(
                    cluster, "EtSimulateBurstManagerError", error="Execute"):
                # Commit shouldn't throw any exception as it is a critical
                # section.
                cursor.execute(COMMIT_Q)
