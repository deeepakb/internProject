# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest
import uuid

from contextlib import contextmanager
from raff.burst.burst_test import (BurstTest, setup_teardown_burst,
                                   verify_query_bursted)
from raff.common.db.session import DbSession
from threading import Thread
from time import sleep

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst", "verify_query_bursted"]

CUSTOM_GUCS = {
    'enable_streaming_mjoin': 'true',
    'burst_enable_streaming_mjoin': 'true',
    # Disable hashjoin for testing mergejoin.
    'enable_hashjoin': 'false',
    # Required by ported tests
    'enable_nestloop': 'false',
}

TEST_PORTED_MJOIN_BUG2067_SETUP = """
create table X(a int not null);
create table Y(b int not null);
insert into X values(-3);
insert into Y values(-3);
insert into Y values(0);
analyze X;
analyze Y;
"""

TEST_PORTED_MJOIN_BUG2067_CLEANUP = """
drop table X;
drop table Y;
"""

TEST_PORTED_MJOIN_SETUP = """
CREATE TABLE friend5049_burst (
      id                  integer,
      account_id          integer,
      friend_account_id   integer,
      dt_created          TIMESTAMP WITHOUT TIME ZONE
)
DISTSTYLE KEY DISTKEY ( account_id )
SORTKEY (account_id, dt_created)
;
CREATE TABLE trust5049_burst (
      account_id          integer,
      trusted_account_id  integer,
      dt_created          TIMESTAMP WITHOUT TIME ZONE
)
DISTSTYLE KEY DISTKEY ( account_id )
SORTKEY (account_id, dt_created)
;
INSERT INTO friend5049_burst VALUES (1,1,1,TIMESTAMP '2001-09-28 01:00');
INSERT INTO trust5049_burst  VALUES (1,3,TIMESTAMP '2005-09-28 01:00');
"""

TEST_PORTED_MJOIN_CLEANUP = """
DROP TABLE IF EXISTS friend5049_burst,
                     trust5049_burst;
"""

TEST_PORTED_MJOIN_OUTER2_SETUP = """
-- Verifies outer merge joining
create table alpha (acol int);
insert into alpha values(1);
insert into alpha values(2);
insert into alpha values(3);
insert into alpha values(4);
insert into alpha values(5);
insert into alpha values(6);
insert into alpha values(7);
insert into alpha values(8);
insert into alpha values(9);
insert into alpha values(10);
insert into alpha values(null);
analyze alpha;

create table beta (bcol int);
insert into beta values(0);
insert into beta values(2);
insert into beta values(4);
insert into beta values(6);
insert into beta values(8);
insert into beta values(null);
analyze beta;

create table theta (acol int);
insert into theta values(-1);
insert into theta values(6);
analyze theta;

create table omega (acol int);
insert into omega values(-7);
insert into omega values(0);
insert into omega values(4);
analyze omega;

create table zed (bcol int);
insert into zed values(-3);
insert into zed values(4);
analyze zed;
"""

TEST_PORTED_MJOIN_OUTER2_CLEANUP = """
drop table alpha;
drop table beta;
drop table theta;
drop table omega;
drop table zed;
"""

CANCELLATION_TEST_SQL = """
SELECT count(*)
FROM tpch1_lineitem_redshift
LEFT JOIN tpch1_orders_redshift ON l_orderkey = o_orderkey
AND l_shipdate = o_orderdate;
"""

INFLIGHT_QUERIES = """set query_group to 'health';
                   select pid, text from stv_inflight where label = 'burst'"""

COUNT_INFLIGHT_QUERIES = "select count(*) from stv_inflight where query = {}"

TERMINATE_QUERY = "select pg_terminate_backend({})"

SLEEP_SECONDS_BEFORE_VALIDATING_CANCELLED_QUERIES = 30


@pytest.mark.load_data
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS)
@pytest.mark.serial_only
@pytest.mark.localhost_only
class TestBurstMergejoin(BurstTest):
    """
    Tests for streaming mjoin for two sorted streams.
    """
    @contextmanager
    def burst_db_session(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst")
            yield cursor
            cursor.execute("reset query_group")

    def test_burst_streaming_mjoin(self, db_session,
                                   verify_query_bursted):
        """
        Correctness test for streaming mjoin using TPC-H dataset.
        """
        self.execute_test_file('burst_streaming_mjoin',
                               db_session)

    def validate_cancelled_query(self, cluster, cursor, query_id, query_text):
        """
        Confirm that the query has actually been cancelled by checking to see
        if it is still inflight.

        Args:
            cluster:  Cluster object.
            cursor: Cursor against the cluster.
            query_id: Query ID of the cancelled query.
            query_text: Query text of the cancelled query.

        """
        sql = COUNT_INFLIGHT_QUERIES.format(query_id)
        cursor.execute(sql)
        if cursor.fetch_scalar() > 0:
            pytest.fail("All burst queries should have been cancelled")

    def validate_cancelled_queries(self, cluster, queries):
        """
        Take a list of queries that have been cancelled and verify each
        is no longer in flight.

        Args:
            queries: List of queries to confirm have been cancelled.
        """
        if queries:
            with self.db.cursor() as cursor:
                for query_id in queries.keys():
                    self.validate_cancelled_query(
                        cluster=cluster,
                        cursor=cursor, query_id=query_id,
                        query_text=queries[query_id])

    def run_query_worker(self, db, querytxt):
        """
        Run a Burst query within a new session.
        """
        # Establish a new connection to run the Burst query.
        with self.burst_db_session(db) as cursor:
            cursor.execute(CANCELLATION_TEST_SQL)

    def test_burst_mjoin_cancellation(self, db_session, cluster):
        """
        Cancellation test for streaming mjoin.
        """
        # Pause all Burst/Spectrum queries.
        with self.db.cursor() as cursor:
            cursor.execute("xpx 'event set EtSpectrumPauseQuery'")
        # Spin up a thread to run the query.
        query_worker_thread = Thread(target=self.run_query_worker,
                                     args=[DbSession(
                                            cluster.get_conn_params()),
                                           CANCELLATION_TEST_SQL])
        query_worker_thread.setDaemon(True)
        query_worker_thread.start()
        sleep(SLEEP_SECONDS_BEFORE_VALIDATING_CANCELLED_QUERIES)
        # Cancel the running queries.
        list_of_cancelled_queries = {}
        with self.db.cursor() as cursor:
            cursor.execute(INFLIGHT_QUERIES)
            for query_id, query_text in cursor.fetchall():
                list_of_cancelled_queries[query_id] = (query_text)
                sql = TERMINATE_QUERY.format(int(query_id))
                cursor.execute(sql)
        # Resume all Burst/Spectrum queries.
        with self.db.cursor() as cursor:
            cursor.execute("xpx 'event unset EtSpectrumPauseQuery'")
        sleep(SLEEP_SECONDS_BEFORE_VALIDATING_CANCELLED_QUERIES)
        # Join the worker thread and do the validation.
        query_worker_thread.join()
        self.validate_cancelled_queries(cluster, list_of_cancelled_queries)

    def run_ported_test(self, db_session, cluster, setup_queries,
                        cleanup_queries, testfile):
        """
        Run ported tests which create testing tables.
        """
        # Set up testing tables and backup.
        with db_session.cursor() as cursor, self.auto_release_local_burst(
                cluster):
            cursor.execute(setup_queries)
        SNAPSHOT_IDENTIFIER_1 = ("{}-{}".format(cluster.cluster_identifier,
                                                str(uuid.uuid4().hex)))
        cluster.backup_cluster(SNAPSHOT_IDENTIFIER_1)
        # Run test workload.
        try:
            with self.burst_db_session(db_session) as cursor:
                self.execute_test_file(testfile,
                                       session=db_session)
        finally:
            with db_session.cursor() as cursor, self.auto_release_local_burst(
                    cluster):
                cursor.execute(cleanup_queries)

    def test_ported_join_merge_bug2067(self, db_session, cluster,
                                       verify_query_bursted):
        """
        Ported merge join test from src/trivia/join_merge_bug2067.sql
        """
        self.run_ported_test(db_session, cluster,
                             TEST_PORTED_MJOIN_BUG2067_SETUP,
                             TEST_PORTED_MJOIN_BUG2067_CLEANUP,
                             'join_merge_bug2067')

    def test_ported_merge_join(self, db_session, cluster,
                               verify_query_bursted):
        """
        Ported merge join test from test/raff/qp/test_merge_join.py.
        Tests that merge join does not result in a Sig11 from run().
        """
        self.run_ported_test(db_session, cluster, TEST_PORTED_MJOIN_SETUP,
                             TEST_PORTED_MJOIN_CLEANUP, 'merge_join')

    def test_ported_join_merge_outer(self, db_session,
                                     verify_query_bursted):
        """
        Ported merge join test from src/trivia/join_merge_outer.sql
        """
        self.execute_test_file('join_merge_outer', db_session)

    def test_ported_join_merge_outer2(self, db_session, cluster,
                                      verify_query_bursted):
        """
        Ported merge join test from src/trivia/join_merge_outer2.sql
        """
        self.run_ported_test(db_session, cluster,
                             TEST_PORTED_MJOIN_OUTER2_SETUP,
                             TEST_PORTED_MJOIN_OUTER2_CLEANUP,
                             'join_merge_outer2')

    def test_ported_join_streaming(self, db_session,
                                   verify_query_bursted):
        """
        Ported merge join test from src/trivia/join_streaming.sql
        """
        self.execute_test_file('join_streaming', db_session)
