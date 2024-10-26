# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest
import uuid

from contextlib import contextmanager
from psycopg2 import Error
from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import BURST_DISABLE_C2S3_GUCS
from raff.burst.burst_test import setup_teardown_burst

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst]

CUSTOM_SORTEDNESS_GUCS = {
    'enable_hashjoin': 'false',
    'vacuum_auto_enable': 'false',
    'skip_larger_inner_joinpath': 'false',
    'use_stairows_as_reltuples': 'false',
    # Need to disable Query Plan Cache as we
    # are expecting Plan changes in concurrent runs
    # but DML doesn't invalidate QPC
    'enable_query_plan_cache': 'false'
}
CUSTOM_SORTEDNESS_GUCS.update(BURST_DISABLE_C2S3_GUCS)

CREATE_SQL1 = """create table if not exists burst_semisorted_distkey(
                  a varchar(20), b varchar(20))
                  distkey(a) sortkey(a);"""
CREATE_SQL2 = """create table if not exists burst_semisorted_distall(
                  a varchar(20), b varchar(20))
                  diststyle all sortkey(a);"""
CREATE_SQL3 = """create table if not exists burst_dimension_distkey(
                 a varchar(20), b varchar(20))
                 distkey(a) sortkey(a);"""
CREATE_SQL4 = """create table if not exists burst_dimension_distall(
                 a varchar(20), b varchar(20))
                 diststyle all sortkey(a);"""
DISTKEY_TABLE = "burst_semisorted_distkey"
DISTALL_TABLE = "burst_semisorted_distall"
DISTKEY_DIM = "burst_dimension_distkey"
DISTALL_DIM = "burst_dimension_distall"
INSERT_SQL1 = """insert into {} values
                   ('Fabian', 'Nagel'), ('Ippokratis', 'Pandis'),
                   ('Martin', 'Grund'), ('Naresh', 'Chainani'),
                   ('Foyzur', 'Rahman'), ('Nikos', 'Armenatzoglou'),
                   ('Bhavik', 'Bhuta');"""
VACUUM_SQL = "vacuum {};"
INSERT_SQL2 = """insert into {} values
                   ('Gaurav', 'Saxena');"""
INSERT_SQL3 = """insert into {} values
                   ('Yichao', 'Xue'), ('Kiran', 'Chinta'),
                   ('George', 'Caragea'), ('Jenny', 'Chen');"""
INSERT_SQL4 = """insert into {} values
                   ('Fabian', 'Nagel'), ('George', 'Caragea');"""
VALIDATE_SQL1 = "select * from {} order by a;"
VALIDATE_SQL2 = "select a, max(b) from {} group by a order by a;"
VALIDATE_SQL3 = """select t1.a, t1.b from {} t1, {} t2 where t1.a = t2.a order
by t1.a;"""
VALIDATE_SQL4 = "select count(*) from {} t1 left join {} t2 on t1.a = t2.a"
VALIDATE_SQL5 = """select count(*) from {} t1 left join {} t2 on t1.a = t2.a
                   where t1.a < 'Yichao' and t2.b > 'Caragea';"""
QUERY_ID = "select pg_last_query_id();"
VALIDATE_PLAN_SQL = """select substring(label, 1, 7)
                   from svl_query_summary
                   where query IN (
                     SELECT concurrency_scaling_query
                     FROM stl_concurrency_scaling_query_mapping
                     WHERE primary_query = {}
                   ) order by stm, seg, step;"""


@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_SORTEDNESS_GUCS,
                               initdb_before=True, initdb_after=True)
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestSortednessBurst(BurstTest):
    @contextmanager
    def burst_db_session(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst")
            yield cursor
            cursor.execute("reset query_group")

    @contextmanager
    def simulate_s3_exception(self):
        """
        Set event EtS3ScanTestFailQueryInClient so that any Spectrum query
        will fail.
        """
        try:
            with self.db.cursor() as cursor:
                cursor.execute("xpx 'event set EtS3ScanTestFailQueryInClient, "
                               "fetch_count=2'")
                yield
        finally:
            with self.db.cursor() as cursor:
                cursor.execute("xpx 'event unset "
                               "EtS3ScanTestFailQueryInClient'")

    # This tests dead code and should be deprecated.
    # Skipping for now since it is incompatible with S3 Commit.
    @pytest.mark.skip(reason="DP-60653")
    def test_burst_sortedness(self, db_session):
        self.execute_test_file('test_burst_sortedness', session=db_session)


    def test_burst_semisorted_join_distkey(self, cursor, db_session, cluster):
        """
        Tests that a query that requires sortedness on a table that contains a
        sorted and an unsorted region has the correct result and validates the
        query's execution plan.
        """
        # Create and populate dataset. Unsorted region has to be smaller 20%
        # ('gconf_backend_unsorted_percentage_threshold').
        with db_session.cursor() as cursor, \
             self.auto_release_local_burst(cluster):
            cursor.execute(CREATE_SQL1)
            cursor.execute(CREATE_SQL3)
            cursor.execute(CREATE_SQL4)
            cursor.execute(INSERT_SQL1.format(DISTKEY_TABLE))
            cursor.execute(INSERT_SQL4.format(DISTKEY_DIM))
            cursor.execute(INSERT_SQL4.format(DISTALL_DIM))
            cursor.execute(VACUUM_SQL.format(DISTKEY_TABLE))
            cursor.execute(VACUUM_SQL.format(DISTKEY_DIM))
            cursor.execute(VACUUM_SQL.format(DISTALL_DIM))
            cursor.execute(INSERT_SQL2.format(DISTKEY_TABLE))
            cursor.execute(INSERT_SQL2.format(DISTKEY_DIM))
            cursor.execute(INSERT_SQL2.format(DISTALL_DIM))
        SNAPSHOT_IDENTIFIER_1 = ("{}-{}".format(cluster.cluster_identifier,
                                                str(uuid.uuid4().hex)))
        cluster.backup_cluster(SNAPSHOT_IDENTIFIER_1)

        # Validate result of first query.
        QID = None
        with self.burst_db_session(db_session) as cursor:
            cursor.execute(VALIDATE_SQL3.format(DISTKEY_TABLE, DISTKEY_DIM))
            assert cursor.fetchall() == [('Fabian', 'Nagel'), ('Gaurav',
                                                               'Saxena')]
            cursor.execute(QUERY_ID)
            QID = cursor.fetchall()[0][0]

        # Validate execution plan. The plan will utilize the sorted region.
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(VALIDATE_PLAN_SQL.format(QID))
            assert bootstrap_cursor.fetchall() == [
                ('scan   ', ), ('project', ), ('project', ), ('sort   ', ),
                ('scan   ', ), ('project', ), ('save   ', ), ('scan   ', ),
                ('sort   ', ), ('scan   ', ), ('merge  ', ), ('project', ),
                ('project', ), ('project', ), ('mjoin  ', ), ('project', ),
                ('return ', ), ('merge  ', ), ('project', ), ('return ', )
            ]

        # Validate result of second query.
        QID = None
        with self.burst_db_session(db_session) as cursor:
            cursor.execute(VALIDATE_SQL3.format(DISTKEY_TABLE, DISTALL_DIM))
            assert cursor.fetchall() == [('Fabian', 'Nagel'), ('Gaurav',
                                                               'Saxena')]
            cursor.execute(QUERY_ID)
            QID = cursor.fetchall()[0][0]

        # Validate execution plan. The plan will sort the entire Spectrum
        # result because we lost sortedness by pushing down the aggregation.
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(VALIDATE_PLAN_SQL.format(QID))
            assert bootstrap_cursor.fetchall() == [
                ('scan   ', ), ('sort   ', ), ('scan   ', ), ('project', ),
                ('project', ), ('dist   ', ), ('scan   ', ), ('sort   ', ),
                ('scan   ', ), ('project', ), ('mjoin  ', ), ('project', ),
                ('return ', ), ('scan   ', ), ('merge  ', ), ('project', ),
                ('project', ), ('merge  ', ), ('project', ), ('return ', )
            ]

        # Insert data to extend unsorted region over 20%.
        with db_session.cursor() as cursor, \
             self.auto_release_local_burst(cluster):
            cursor.execute(INSERT_SQL3.format(DISTKEY_TABLE))
        SNAPSHOT_IDENTIFIER_2 = ("{}-{}".format(cluster.cluster_identifier,
                                                str(uuid.uuid4().hex)))
        cluster.backup_cluster(SNAPSHOT_IDENTIFIER_2)

        # Validate first result.
        QID = None
        with self.burst_db_session(db_session) as cursor:
            cursor.execute(VALIDATE_SQL3.format(DISTKEY_TABLE, DISTKEY_DIM))
            assert cursor.fetchall() == [('Fabian', 'Nagel'),
                                         ('Gaurav', 'Saxena'), ('George',
                                                                'Caragea')]
            cursor.execute(QUERY_ID)
            QID = cursor.fetchall()[0][0]

        # Validate execution plan. Currently, the Burst plan does not yet
        # utilize the sortedness property of the table.
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(VALIDATE_PLAN_SQL.format(QID))
            assert bootstrap_cursor.fetchall() == [
                ('scan   ', ), ('project', ), ('project', ), ('sort   ', ),
                ('scan   ', ), ('project', ), ('save   ', ), ('scan   ', ),
                ('project', ), ('project', ), ('sort   ', ), ('scan   ', ),
                ('project', ), ('project', ), ('mjoin  ', ), ('project', ),
                ('return ', ), ('merge  ', ), ('project', ), ('return ', )
            ]

        # Validate second result.
        QID = None
        with self.burst_db_session(db_session) as cursor:
            cursor.execute(VALIDATE_SQL3.format(DISTKEY_TABLE, DISTALL_DIM))
            assert cursor.fetchall() == [('Fabian', 'Nagel'),
                                         ('Gaurav', 'Saxena'), ('George',
                                                                'Caragea')]
            cursor.execute(QUERY_ID)
            QID = cursor.fetchall()[0][0]

        # Validate execution plan. Currently, the Burst plan does not yet
        # utilize the sortedness property of the table.
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(VALIDATE_PLAN_SQL.format(QID))
            assert bootstrap_cursor.fetchall() == [
                ('scan   ', ), ('project', ), ('project', ), ('dist   ', ),
                ('scan   ', ), ('sort   ', ), ('scan   ', ), ('project', ),
                ('project', ), ('sort   ', ), ('scan   ', ), ('project', ),
                ('project', ), ('mjoin  ', ), ('project', ), ('return ', ),
                ('scan   ', ), ('merge  ', ), ('project', ), ('return ', )
            ]

    def test_burst_streaming_mjoin(
            self, cursor, db_session, cluster):
        """
        Tests that when we merge-join two sorted streams, we optimize the query
        by adding two scan steps in one segment to avoid saving the inner
        stream.
        Related SIM: https://sim.amazon.com/issues/Burst-1464
        """
        # Validate execution plan. The plan will have two scan in the first
        # segment.
        PLAN_SQL_VALIDATION = """select seg, substring(label, 1, 7)
                                 from svl_query_summary
                                 where query IN (
                                   SELECT concurrency_scaling_query
                                   FROM stl_concurrency_scaling_query_mapping
                                   WHERE primary_query = {}
                                 ) order by stm, seg, step;"""
        with db_session.cursor() as cursor, self.auto_release_local_burst(
                cluster):
            cursor.execute(CREATE_SQL1)
            cursor.execute(CREATE_SQL3)
            cursor.execute(INSERT_SQL3.format(DISTKEY_TABLE))
            cursor.execute(INSERT_SQL3.format(DISTKEY_DIM))
        SNAPSHOT_IDENTIFIER_1 = ("{}-{}".format(cluster.cluster_identifier,
                                                str(uuid.uuid4().hex)))
        cluster.backup_cluster(SNAPSHOT_IDENTIFIER_1)

        # Validate the result for simply join query.
        QID = None
        with self.burst_db_session(db_session) as cursor:
            cursor.execute(VALIDATE_SQL4.format(DISTKEY_TABLE, DISTKEY_DIM))
            assert cursor.fetchall() == [(4,)]
            cursor.execute(QUERY_ID)
            QID = cursor.fetchall()[0][0]
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(PLAN_SQL_VALIDATION.format(QID))
            assert bootstrap_cursor.fetchall() == [
                (0, 'scan   '), (0, 'project'), (0, 'project'), (0, 'project'),
                (0, 'mjoin  '), (0, 'project'), (0, 'project'), (0, 'aggr   '),
                (0, 'scan   '), (0, 'project'), (0, 'project'), (1, 'scan   '),
                (1, 'return '), (2, 'scan   '), (2, 'aggr   '), (3, 'scan   '),
                (3, 'project'), (3, 'project'), (3, 'return ')
            ]

        # Validate the result for simply join query with predicates.
        QID = None
        with self.burst_db_session(db_session) as cursor:
            cursor.execute(VALIDATE_SQL5.format(DISTKEY_TABLE, DISTKEY_DIM))
            assert cursor.fetchall() == [(2,)]
            cursor.execute(QUERY_ID)
            QID = cursor.fetchall()[0][0]
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(PLAN_SQL_VALIDATION.format(QID))
            assert bootstrap_cursor.fetchall() == [
                (0, 'scan   '), (0, 'project'), (0, 'project'), (0, 'project'),
                (0, 'mjoin  '), (0, 'project'), (0, 'project'), (0, 'aggr   '),
                (0, 'scan   '), (0, 'project'), (0, 'project'), (1, 'scan   '),
                (1, 'return '), (2, 'scan   '), (2, 'aggr   '), (3, 'scan   '),
                (3, 'project'), (3, 'project'), (3, 'return ')
            ]
