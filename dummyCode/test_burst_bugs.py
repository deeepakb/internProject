# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import uuid

from contextlib import contextmanager

from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst
from raff.common.host_type import HostType
from raff.util.utils import run_bootstrap_sql

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst]

BURST_CHECK = """select count(*) from  stl_query
 where concurrency_scaling_status = 1"""

# This OID is from a user table and the only thing we can verify is
# consistency across SELECT/PREPARE/CURSOR and burst/no-burst cases.
OID_USER_TABLE = """SELECT 'call_center'::regclass::oid FROM
 call_center;"""
# This OID should not change and therefore is verifiable across tests.
OID_SYSTEM_TABLE = """select pg_type.oid from pg_type, call_center
 where pg_type.typname = 'oid';"""

all_data_types_tests_1 = """select * from alldatatypes_redshift
 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12;"""
all_data_types_tests_2 = """select * from alltypes_redshift
 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14;"""

BEGIN_Q = "BEGIN;"
DECLARE_Q = "DECLARE c1 CURSOR FOR {}"
FETCHALL_Q = "fetch all from c1"
COMMIT_Q = "COMMIT;"

# For testing prepared statements we first prepare a query and then execute
# that query.
PREPARE_Q = "PREPARE q1 as {};"
EXECUTE_Q = "EXECUTE q1;"
DEALLOCATE_Q = "DEALLOCATE q1;"


class InternalTestDriver:
    """
    Utility class to run various queries with the expectation of burst/no-burst
    and cache/no-cache. Checks if the run meets the expectation.
    """

    def __init__(self, db, cluster):
        self.db = db
        self.cluster = cluster

    @contextmanager
    def burst_db_session(self, db_session, should_burst):
        burstness = "burst" if should_burst else "noburst"
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to {}".format(burstness))
            yield cursor
            cursor.execute("reset query_group")

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
    def execute_query_direct(self, query, cursor):
        """
        Executes a query as-is without wrapping in cursor or prepare.
        """
        cursor.execute(query)
        yield cursor.fetchall()
        pass

    @contextmanager
    def execute_query_using_cursor(self, query, cursor):
        """
        Wraps a query in CURSOR and then executes it.
        """
        cursor.execute(BEGIN_Q)
        cursor.execute(DECLARE_Q.format(query))
        cursor.execute(FETCHALL_Q)
        yield cursor.fetchall()
        cursor.execute(COMMIT_Q)

    @contextmanager
    def execute_query_using_prepare(self, query, cursor):
        """
        Wraps a query in PREPARE and then executes it.
        """
        prepare_query = PREPARE_Q.format(query)
        cursor.execute(prepare_query)
        cursor.execute(EXECUTE_Q)
        yield cursor.fetchall()
        cursor.execute(DEALLOCATE_Q)


@pytest.mark.load_data
@pytest.mark.load_tpcds_data
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.burst_precommit
# commit in auto-commit connection is not allowed in current jdbc driver
@pytest.mark.no_jdbc
@pytest.mark.serial_only
class TestBurstBugs(BurstTest):
    @pytest.mark.serial_only
    def test_session_functions(self, cluster, db_session):
        """
        Test for Burst-2173: Burst a query with a bunch of session specific
        functions such as current_user, current_user_id, user and verify
        output.
        """

        v = InternalTestDriver(self.db, cluster)
        SESSION_STATE_QUERY = """select count(*),current_user, current_user_id,
         user, current_database() from store_sales;"""
        with v.burst_db_session(db_session, True) as cursor:
            with v.validate_query_burstness(True):
                cursor.execute(SESSION_STATE_QUERY)
                burst_result = cursor.fetchall()
                assert len(burst_result) == 1

        v.clear_result_cache(cluster)

        with v.burst_db_session(db_session, False) as cursor:
            with v.validate_query_burstness(False):
                cursor.execute(SESSION_STATE_QUERY)
                noburst_result = cursor.fetchall()
                assert len(noburst_result) == 1
                assert noburst_result[0][0] == 2880404
        assert noburst_result == burst_result

    def verify_oid_serialization(self, cluster, db_session, query, exec_class,
                                 exec_func, run_burst):
        with exec_class.burst_db_session(db_session, False) as cursor:
            with exec_class.validate_query_burstness(False):
                with exec_func(exec_class, query, cursor) as result:
                    no_burst_result = result

        exec_class.clear_result_cache(cluster)

        # If burstable query (e.g., no system table) also verify burst is
        # consistent with main.
        if run_burst:
            with exec_class.burst_db_session(db_session, True) as cursor:
                with exec_class.validate_query_burstness(True):
                    with exec_func(exec_class, query, cursor) as result:
                        burst_result = result
            assert no_burst_result == burst_result

        return no_burst_result

    def verify_oid_serialization_for_one_query(self, cluster, db_session,
                                               query, exec_class, exec_func,
                                               run_burst):
        select_result = self.verify_oid_serialization(
            cluster, db_session, query, exec_class,
            InternalTestDriver.execute_query_direct, run_burst)
        prepare_result = self.verify_oid_serialization(
            cluster, db_session, query, exec_class,
            InternalTestDriver.execute_query_using_prepare, run_burst)
        cursor_result = self.verify_oid_serialization(
            cluster, db_session, query, exec_class,
            InternalTestDriver.execute_query_using_cursor, run_burst)

        assert select_result == prepare_result
        assert prepare_result == cursor_result

        return select_result

    @pytest.mark.serial_only
    @pytest.mark.no_jdbc
    def test_oid_serialization(self, cluster, db_session):
        """
        Test for Burst-2175: Burst a query with Oid data type in the target
        list and check correctness.
        """

        v = InternalTestDriver(self.db, cluster)
        result_user_table = self.verify_oid_serialization_for_one_query(
            cluster, db_session, OID_USER_TABLE, v,
            InternalTestDriver.execute_query_direct, True)
        assert len(result_user_table) == 6

        result_system_table = self.verify_oid_serialization_for_one_query(
            cluster, db_session, OID_SYSTEM_TABLE, v,
            InternalTestDriver.execute_query_direct, False)
        # We selected from pg_type where typname == 'oid'. So, we should get
        # back 26.
        assert result_system_table[0][0] == 26
        assert len(result_system_table) == 6

        # Now verify we didn't regress existing Redshift data types.
        self.verify_oid_serialization_for_one_query(
            cluster, db_session, all_data_types_tests_1, v,
            InternalTestDriver.execute_query_direct, True)

        self.verify_oid_serialization_for_one_query(
            cluster, db_session, all_data_types_tests_2, v,
            InternalTestDriver.execute_query_direct, True)

    def test_spectrum_char_len_burst(self, db_session):
        """
        Test for Burst-365: Spectrum char length is too small to cover
        Redshift BPCHAR.
        """
        with db_session.cursor() as cursor:
            cursor.execute(
                "create table large_bpchar (a char(512) encode raw);")
            self.execute_test_file('test_burst_365', session=db_session)

    def test_burst_misc_bugs(self, db_session):
        """
        Repros for misc burst bugs on top of the TPC-DS dataset.
        """
        self.execute_test_file('burst_misc_bugs', session=db_session)

    def test_qual_subplan_walker(self, db_session):
        """
        Test for Burst-391: Range table based table oid collection.
        """
        with db_session.cursor() as cursor:
            cursor.execute("create table qual_subplan_test_main(a int);")
            cursor.execute(
                "create table qual_subplan_test_sub(b varchar(25));")
            self.execute_test_file(
                'test_qual_subplan_walker', session=db_session)
