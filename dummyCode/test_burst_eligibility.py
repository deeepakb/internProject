# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest

from contextlib import contextmanager

from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst
from raff.common.db.jdbc_cursor import JdbcCursor
from raff.common.host_type import HostType
from raff.util.utils import run_bootstrap_sql

import enum

from decimal import Decimal

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst"]

# Maximum of ss_item_sk in store_sales. Used in the WHERE clause to ensure all
# rows in the store_sales table are considered. Conversely, we use "0" as a
# filter in the parameterized query below to exclude all rows to simulate a
# query that returns 0 row.
MAX_SS_ITEM_SK = 18000
SELECT_FROM_STORE = """SELECT * FROM store_sales where ss_item_sk <= {} ORDER
 BY 1, 2, 3, 4, 5 LIMIT {};"""
QUERY_ID = 'SELECT pg_last_query_id();'
VALIDATION = """select invalidated_by > 0 as is_invalidated, result_size_rows,
 result_size_bytes, hit_count from stv_result_cache WHERE source_query = {};"""
BURST_CHECK = """select count(*) from  stl_query
 where concurrency_scaling_status = 1"""

BEGIN_Q = "BEGIN;"
DECLARE_Q = "DECLARE c1 CURSOR FOR {}"
FETCH_NEXT_Q = "FETCH NEXT from c1;"
FETCH_FORWARD_Q = "FETCH FORWARD {} FROM c1;"
COMMIT_Q = "COMMIT;"

# For testing prepared statements we first prepare a query with a parameter
# and then execute that query with the provided parameter value.
PREPARE_Q = "PREPARE q1(int, int) as {};"
EXECUTE_Q = "EXECUTE q1({}, {});"
DEALLOCATE_Q = "DEALLOCATE q1;"

SPECTRUM_QUERY = """SELECT l_orderkey,
         sum(l_extendedprice * (1 - l_discount)) AS revenue,
         o_orderdate,
         o_shippriority
  FROM tpch1_customer_redshift,
       tpch1_orders_redshift,
       s3.tpch1_lineitem_parquet
  WHERE c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < '1995-03-15'
    AND l_shipdate > '1995-03-15'
  GROUP BY l_orderkey,
           o_orderdate,
           o_shippriority
  ORDER BY revenue DESC,
           o_orderdate LIMIT 10;

"""

DISABLED_QUERIES_GUCS = {
    'enable_result_cache': 'true',
    'enable_burst_result_cache': 'true',
    'enable_burst_param_prepare': 'false',
    'enable_burst_ext_protocol': 'true',
    'enable_burst_cursor': 'false',
    'burst_cursor_prefetch_rows': '1000'
}

DISABLED_SPECTRUM_GUCS = {'enable_burst_spectrum': 'false'}

DISABLED_CACHE_GUCS = {
    'enable_result_cache': 'true',
    'enable_burst_result_cache': 'false',
    'enable_burst_param_prepare': 'true',
    'enable_burst_ext_protocol': 'true',
    'enable_burst_cursor': 'true',
    'burst_cursor_prefetch_rows': '1000'
}

DISABLED_PROTOCOL_GUCS = {
    'enable_result_cache': 'true',
    'enable_burst_result_cache': 'true',
    'enable_burst_param_prepare': 'true',
    'enable_burst_ext_protocol': 'false',
    'enable_burst_cursor': 'true',
    'burst_cursor_prefetch_rows': '1000'
}

SELECTED_RESULTS = {
    0: (2450816, 30978, 2558, 32941, 1648120, 2146, None, None, None, 156728,
        22, 17.15, None, None, 38.49, 213.84, 377.30, None, None, 38.49,
        175.35, None, None),
    1: (2450816, 30978, 4148, 32941, 1648120, 2146, 33325, 10, 258, 156728, 38,
        71.20, 103.24, 85.68, 0.00, 3255.84, 2705.60, 3923.12, 65.11, 0.00,
        3255.84, 3320.95, 550.24),
    9: (2450816, 30978, 12217, 32941, 1648120, 2146, 33325, 10, 203, 156728,
        21, 62.83, 113.72, 35.25, 473.76, 740.25, 1319.43, 2388.12, 7.99,
        473.76, 266.49, 274.48, -1052.94),
    99: (2450816, 35731, 1022, 80973, 774531, 3642, 43953, 7, 110, 12619, 90,
         74.47, 136.28, 117.20, 10231.56, 10548.00, 6702.30, 12265.20, 9.49,
         10231.56, 316.44, 325.93, -6385.86),
    999: (2450817, 31596, 17722, 75628, 584835, 5242, 44802, 1, 225, 78856, 98,
          52.84, 61.29, 60.67, 0.00, 5945.66, 5178.32, 6006.42, 535.10, 0.00,
          5945.66, 6480.76, 767.34),
    9999: (2450827, 34854, 662, 38037, 536953, 6269, 17185, 4, 244, 34147, 17,
           48.60, 83.10, 24.93, 245.80, 423.81, 826.20, 1412.70, 14.24, 245.80,
           178.01, 192.25, -648.19)
}


class ExecType(enum.Enum):
    EXEC_SELECT = 1
    EXEC_PREPARE = 2
    EXEC_CURSOR = 3


class InternalTestDriver:
    """
    Utility class to run various queries with the expectation of burst/no-burst
    and cache/no-cache. Checks if the run meets the expectation.
    """

    def __init__(self, db, cluster):
        self.db = db
        self.cluster = cluster

    @contextmanager
    def burst_db_session(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst")
            yield cursor
            cursor.execute("reset query_group")

    def get_query_Id(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute(QUERY_ID)
            return cursor.fetchall()[0][0]

    def run_priledged_query(self, query):
        if self.cluster.host_type == HostType.CLUSTER:
            nested_lst = run_bootstrap_sql(self.cluster, query)
            nested_lst_of_tuples = [tuple(l) for l in nested_lst]
            return nested_lst_of_tuples
        else:
            with self.db.cursor() as bootstrap_cursor:
                bootstrap_cursor.execute(query)
                return bootstrap_cursor.fetchall()

    def get_burst_query_count(self):
        return int(self.run_priledged_query(BURST_CHECK)[0][0])

    @contextmanager
    def validate_query_burstness(self, should_burst):
        """
        Compares the number of queries bursted before and after an executed
        query to determine if the query bursted.
        """
        query_count1 = self.get_burst_query_count()
        yield
        query_count2 = self.get_burst_query_count()
        if should_burst:
            assert query_count2 > query_count1
        else:
            assert query_count1 == query_count2

    def clear_result_cache(self, cluster):
        cluster.run_xpx('clear_result_cache')

    @contextmanager
    def execute_query_using_cursor(self, ss_item_sk_limit, limit, cursor,
                                   fetch_how_many):
        """
        Wraps a query in cursor and executes. Yields the result for validation
        by the caller.
        """
        cursor.execute(BEGIN_Q)
        query = SELECT_FROM_STORE.format(ss_item_sk_limit, limit)
        cursor.execute(DECLARE_Q.format(query))
        if fetch_how_many == 0:
            # Can't forward fetch 0 as psycopg optimizes it and doesn't forward
            # anything.
            cursor.execute(FETCH_NEXT_Q)
        else:
            cursor.execute(FETCH_FORWARD_Q.format(fetch_how_many))
        yield cursor.fetchall()
        cursor.execute(COMMIT_Q)

    @contextmanager
    def execute_query_using_prepare(self, ss_item_sk_limit, limit, cursor):
        """
        Substitute the SELECT statement to make the limit as parameterized
        instead of substituting constant limit as in the regular SELECT and
        cursor tests.
        """

        query = PREPARE_Q.format(SELECT_FROM_STORE.format("$1", "$2"))
        cursor.execute(query)
        # Now bind the parameter and execute.
        cursor.execute(EXECUTE_Q.format(ss_item_sk_limit, limit))
        yield cursor.fetchall()
        cursor.execute(DEALLOCATE_Q)

    def validate_correctness(self, result, expected_count):
        """
        Iterates through randomly picked result set and validates the
        correctness of the provided 'result'.
        """
        assert len(result) == expected_count
        for i in SELECTED_RESULTS:
            # Deliberately not doing early termination to not make assumption
            # on the order of the expected result set dictionary.
            if i < len(result):
                assert len(result[i]) == len(SELECTED_RESULTS[i])
                for j in range(0, len(result[i])):
                    if result[i][j] is None:
                        assert SELECTED_RESULTS[i][j] is None
                    else:
                        assert abs(
                            Decimal(result[i][j]) -
                            Decimal(SELECTED_RESULTS[i][j])) < 0.01

    def validate_select(self, ss_item_sk_limit, limit, expected_count, cursor):
        """
        Helper method to run a SELECT and validate correctness.
        """
        query = SELECT_FROM_STORE.format(ss_item_sk_limit, limit)
        cursor.execute(query)
        result = cursor.fetchall()
        self.validate_correctness(result, expected_count)

    def validate_cursor(self, ss_item_sk_limit, limit, expected_count, fetch_1,
                        cursor):
        """
        Helper method to run a CURSOR and validate correctness.
        """
        # We fetch at least 1 row even if we are expecting 0 row.
        fetch_count = 1 if fetch_1 else max(expected_count, 1)
        with self.execute_query_using_cursor(ss_item_sk_limit, limit, cursor,
                                             fetch_count) as result:
            array_len_expected = min(fetch_count, expected_count)
            self.validate_correctness(result, array_len_expected)

    def validate_prepare(self, ss_item_sk_limit, limit, expected_count,
                         cursor):
        """
        Helper method to run a PREPARE and validate correctness.
        """
        with self.execute_query_using_prepare(ss_item_sk_limit, limit,
                                              cursor) as result:
            self.validate_correctness(result, expected_count)

    def validate_common(self, db_session, ss_item_sk_limit, limit,
                        expected_count, exec_type, should_burst,
                        cursor_fetch_1):
        """
        Helper method to run a SELECT/CURSOR/PREPARE and validate correctness.
        """
        with self.validate_query_burstness(should_burst):
            with self.burst_db_session(db_session) as cursor:
                if (exec_type == ExecType.EXEC_CURSOR):
                    # Always fetch just 1 that should trigger populating the
                    # entire cache.
                    self.validate_cursor(ss_item_sk_limit, limit,
                                         expected_count, cursor_fetch_1,
                                         cursor)
                elif exec_type == ExecType.EXEC_SELECT:
                    self.validate_select(ss_item_sk_limit, limit,
                                         expected_count, cursor)
                else:
                    self.validate_prepare(ss_item_sk_limit, limit,
                                          expected_count, cursor)

    def validate_cache_entry_created(self, query_id, expected_count,
                                     expected_size, should_cache):
        """
        Helper method to ensure that a cache entry was created by the provided
        'query_id' with specific property ('expected_count' and
        'expected_size').
        """

        # Validate that we created a cache entry.
        cache_stat = self.run_priledged_query(VALIDATION.format(query_id))
        if expected_size >= 0 and should_cache:
            entry_invalidated = False if cache_stat[0][
                0] == 'f' else cache_stat[0][0]
            entry_count = int(cache_stat[0][1])
            entry_size = int(cache_stat[0][2])
            hit_count = int(cache_stat[0][3])
            assert entry_invalidated is False and \
                entry_count == expected_count and \
                entry_size == expected_size and hit_count == 0
        else:
            # Nothing should be cached.
            assert cache_stat == []

    def validate_cache_insert(self, db_session, ss_item_sk_limit, limit,
                              expected_count, expected_size, exec_type,
                              should_burst, should_cache):
        """
        Helper method to run a query, check correctness of the query and
        make sure that a cache entry was inserted.
        """
        # Fetch only 1 row for cursors.
        self.validate_common(db_session, ss_item_sk_limit, limit,
                             expected_count, exec_type, should_burst, True)
        query_id = self.get_query_Id(db_session)
        self.validate_cache_entry_created(query_id, expected_count,
                                          expected_size, should_cache)
        return query_id

    def validate_one_query(self, cluster, db_session, ss_item_sk_limit, limit,
                           expected_count, exp_size_sel, exp_size_cursor,
                           exec_type, should_burst, should_cache):
        """
        Driver method to run a query, check correctness, burstness and cache
        entry creation. Match the burstness and cache entry creation against
        expected 'should_burst' and 'should_cache'.
        """

        exp_size = exp_size_cursor

        if exec_type == ExecType.EXEC_SELECT and not isinstance(
                db_session.cursor(), JdbcCursor):
            exp_size = exp_size_sel

        self.clear_result_cache(cluster)
        self.validate_cache_insert(db_session, ss_item_sk_limit, limit,
                                   expected_count, exp_size, exec_type,
                                   should_burst, should_cache)

    def validate_all_queries(self, cluster, db_session, exec_type,
                             should_burst, should_cache):
        """
        Driver method to run a set of queries, check correctness, burstness and
        cache entry creation. Match the burstness and cache entry creation
        against expected 'should_burst' and 'should_cache'.
        """
        # Select 0 row. Still should be cached if caching is expected.
        self.validate_one_query(cluster, db_session, 0, 10, 0, 0, 0, exec_type,
                                should_burst, should_cache)
        # Select 1 row.
        self.validate_one_query(cluster, db_session, MAX_SS_ITEM_SK, 1, 1, 167,
                                200, exec_type, should_burst, should_cache)


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.serial_only
@pytest.mark.custom_burst_gucs(gucs=DISABLED_QUERIES_GUCS)
@pytest.mark.burst_precommit
class TestBurstIneligibleQueries(BurstTest):
    def test_burst_ineligible_queries(self, cluster, db_session):
        """
        Simple tests that runs a set of SELECT/PREPARE statements with
        disabled parameterized prepared statements. The SELECT
        should burst, but parameterized prepared statements should
        not burst. They are all still eligible for result caching.
        """
        v = InternalTestDriver(self.db, cluster)
        # Should burst and cache. We don't have a mechanism to disable SELECT.
        v.validate_all_queries(cluster, db_session, ExecType.EXEC_SELECT, True,
                               True)

        # Parameterized PREPARE Should not burst, but should cache.
        v.validate_all_queries(cluster, db_session, ExecType.EXEC_PREPARE,
                               False, True)

    @pytest.mark.no_jdbc
    def test_burst_ineligible_cursor(self, cluster, db_session):
        """
        Simple tests that runs a CURSOR statement with bursting disabled.
        Caching is still enabled.
        """
        v = InternalTestDriver(self.db, cluster)
        # Should not burst, but should cache.
        v.validate_all_queries(cluster, db_session, ExecType.EXEC_CURSOR,
                               False, True)


@pytest.mark.load_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.serial_only
@pytest.mark.custom_burst_gucs(gucs=DISABLED_SPECTRUM_GUCS)
@pytest.mark.burst_precommit
class TestBurstDisableSpectrum(BurstTest):
    def test_burst_disable_spectrum(self, cluster, db_session):
        """
        Simple tests that runs a Spectrum query with burst enabled but Spectrum
        query bursting disabled and verifies the query doesn't burst.
        """
        v = InternalTestDriver(self.db, cluster)

        should_burst = False
        with v.validate_query_burstness(should_burst):
            with v.burst_db_session(db_session) as cursor:
                cursor.execute(SPECTRUM_QUERY)
                result = cursor.fetchall()
                # Actual result verification is not important as the query
                # didn't even burst and we run the same test for non-burst
                # scenario where we verify result.
                assert len(result) == 10


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.serial_only
@pytest.mark.custom_burst_gucs(gucs=DISABLED_CACHE_GUCS)
@pytest.mark.burst_precommit
class TestBurstDisableCache(BurstTest):
    def test_burst_disable_cache(self, cluster, db_session):
        """
        Simple tests that runs a set of SELECT/PREPARE statements and
        verifies they didn't cache. Bursting is enabled.
        """
        v = InternalTestDriver(self.db, cluster)
        # Should burst, but should not cache.
        v.validate_all_queries(cluster, db_session, ExecType.EXEC_SELECT, True,
                               False)

        # Should burst, but should not cache.
        v.validate_all_queries(cluster, db_session, ExecType.EXEC_PREPARE,
                               True, False)

    @pytest.mark.no_jdbc
    def test_burst_disable_cursor_cache(self, cluster, db_session):
        """
        Simple tests that runs a set of CURSOR statements and
        verifies it didn't cache. Bursting is enabled.
        """
        v = InternalTestDriver(self.db, cluster)
        # Should burst, but should not cache.
        v.validate_all_queries(cluster, db_session, ExecType.EXEC_CURSOR, True,
                               False)


@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.serial_only
@pytest.mark.custom_burst_gucs(gucs=DISABLED_PROTOCOL_GUCS)
@pytest.mark.burst_precommit
class TestBurstDisableExtended(BurstTest):
    # commit in auto-commit connection is not allowed in current jdbc driver
    @pytest.mark.no_jdbc
    def test_burst_disable_extended(self, cluster, db_session):
        """
        Simple tests that runs a set of SELECT/PREPARE statements and
        verifies they didn't burst when run in extended protocol.
        """

        # Only test for jdbc extended protocol.
        if not isinstance(db_session.cursor(), JdbcCursor):
            return
        v = InternalTestDriver(self.db, cluster)
        # Should not burst, but should cache.
        v.validate_all_queries(cluster, db_session, ExecType.EXEC_SELECT,
                               False, True)

        # Should not burst, but should cache.
        v.validate_all_queries(cluster, db_session, ExecType.EXEC_PREPARE,
                               False, True)

    @pytest.mark.no_jdbc
    def test_burst_disable_extended_cursor(self, cluster, db_session):
        """
        Simple tests that runs a CURSOR statement and verifies they didn't
        burst when run in extended protocol.
        Note: This test is not currently exercised as RAFF doesn't support
        cursors with jdbc driver.
        """

        # Only test for jdbc extended protocol.
        # TODO(kalimutk): Remove the no_jdbc marker once jdbc is supported.
        if not isinstance(db_session.cursor(), JdbcCursor):
            return
        v = InternalTestDriver(self.db, cluster)
        # Should not burst, but should cache.
        v.validate_all_queries(cluster, db_session, ExecType.EXEC_CURSOR,
                               False, True)
