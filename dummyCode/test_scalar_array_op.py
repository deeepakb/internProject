# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
import uuid

from contextlib import contextmanager

from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import BURST_DISABLE_C2S3_GUCS
from raff.burst.burst_test import setup_teardown_burst

log = logging.getLogger(__name__)

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst"]

CREATE_TABLE_STMT = """CREATE TABLE test_busrt_scalar_op (
                            ccase char(10),
                            cint int,
                            cbigint bigint,
                            csmallint smallint,
                            cfloat float4,
                            cdouble float8,
                            cchar char(10),
                            cvarchar varchar(10),
                            cdecimal_small decimal(38, 33),
                            cdecimal decimal(10, 2),
                            cdecimal_big decimal(38, 2),
                            cstring varchar(65535),
                            ctimestamp timestamp,
                            cdate date);"""

INSERT_STMT = """INSERT INTO test_busrt_scalar_op VALUES
    ('null', 0, 0, 0, 0, 0, '0', '0', 0, 0, 0, '0', '2010-09-01 00:00:00',
     '2010-09-01'),
    ('min', -2147483648, -9223372036854775808, -32768, -9e18, -9e18,
     '-999999999', '-999999999', -99.999999999999999999999999999999999999,
      -99999999.99, -99999999999999999999999999999999999.99, '-999',
      '1985-09-25 17:45:30.005000', '1985-09-25'),
    ('max', 214, 922, 32, 9.01, 9.01, '9999999999',
     '9999999999', 99.999999999999999999999999999999999999, 99999999.99,
      99999999999999999999999999999999999.99, '-999', '1985-11-25 17:45:30',
      '1999-11-25'),
    ('10000', 10000, 10000, 10000, 10000, 10000, '10000', '10000', 10,
      10000, 10000, '10000', '1985-10-09 17:45:59', '1985-10-09'),
    ('round_up', 12345, 12345, 12345, 12344.69, 12344.69, '12344.69',
     '12344.69', 12344.69, 12344.69, 12344.69, '12344.69',
     '1985-10-01 20:45:30', '1985-10-31'),
    ('round_down', 12345, 12345, 12345, 12345.49, 12345.49, '12345.49',
     '12345.49', 12345.49, 12345.49, 12345.49, '12345.49',
     '2017-01-01', '2017-01-01'),
    ('round_even', 12344, 12344, 12345, 12344.50, 12344.50, '12344.50',
     '12344.50', 12344.50, 12344.50, 12344.50, '12344.50',
     '1985-10-03 17:45:30.005000', '1985-10-04'),
    ('agg', 3, 3, 3, 1234.5678, 0.001, '3', '3', 5000, 5000, 5000, '3',
     '1970-01-01 00:00:01', '1970-01-01'),
    ('agg', 6, 6, 6, 1234.5678, 0.001, '6', '6', 5000, 5000, 5000, '6',
     '1971-01-19 20:45:30', '1971-01-19'),
    ('agg', 3, 3, 3, 1234.5678, 0.001, '3', '3', 5001, 5001, 5001, '3',
     '2000-01-01 17:45:30.005000', '2000-01-01'),
    ('op', 3, 3, 3, 1234.5678, 0.001, '3', '3', 5000, 5000, 5000, '3',
     '2001-01-02 17:45:30.005000', '2001-01-02'),
    ('any', 49858, 2, 0321, 0.9996456, 12345.6445, '15.6444', '1.6666666',
      00.33645000, 57864526.17, 11.21, '0005.645000', '3001-01-02 17:45:30',
      '3001-01-02'
    );"""

# Queries to test rrscan for Burst scalar array operation. All the queries
# return 0 row and scanned block also should be 0.
QUERIES = ["""SELECT cint
              FROM   test_busrt_scalar_op
              WHERE  cint IN (49859, 49860, 49861, 49862,
                              49863, 49864, 49865, 49866,
                              49867, 49868, 49869, 49872,
                              49873, 49874, 49875, 49875);""",
           """SELECT cbigint
              FROM   test_busrt_scalar_op
              WHERE  cbigint IN (123451, 1234522, 1234523, 123454, 123454,
                                 123455, 123456, 123457, 123458, 123454,
                                 123459, 1234510, 1234511, 1234512,
                                 1234513, 1234514, 9223372036854775806);""",
           """SELECT csmallint
              FROM   test_busrt_scalar_op
              WHERE  csmallint IN (12360, 12346, 12347, 12348, 12348,
                                   12349, 12350, 12351, 12352, 12348,
                                   12353, 12354, 12355, 12356, 12348,
                                   12357, 12358, 12359);""",
           """SELECT cfloat
              FROM   test_busrt_scalar_op
              WHERE  cfloat IN (123451.02, 123452.01, 123453.3, 123454.1,
                                123455.31, 123456.02, 123457.1, 123458.011,
                                123459.11, 1234510.1, 1234511.1, 1234512.1,
                                1234513.1, 9e+18, 1234534.57, 123452.01);""",
           """SELECT cdouble
              FROM   test_busrt_scalar_op
              WHERE  cdouble IN (123451.02, 123452.01, 123453.3, 123454.1,
                                 123455.31, 123456.02, 123457.1, 123458.011,
                                 123459.11, 1234510.1, 1234511.1, 1234512.1,
                                 1234513.1, 1234599.6445, 9e+18, 9e+18);""",
           """SELECT ccase
              FROM   test_busrt_scalar_op
              WHERE  ccase IN ('  1', '  2', ' 3', ' 4', ' 8  ', ' 8 ',
                               ' 5', ' 6', ' 7', ' 8', '  8', ' 8',
                               ' 9', ' 10', ' 11', ' 12', ' 12  ',
                               ' round_down', ' 10000o1', ' agg');""",
           """SELECT cvarchar
              FROM   test_busrt_scalar_op
              WHERE  cvarchar IN (' 1', ' 2', ' 3', ' 4', ' 8  ', ' 8 ',
                                  ' 5', ' 6', ' 7', ' 8', ' 8  ', ' 8 ',
                                  ' 9', ' 10', ' 11', ' 12', ' 12  ',
                                  ' 12345.4', ' 100', ' 9999999999');""",
           """SELECT ctimestamp
              FROM   test_busrt_scalar_op
              WHERE  ctimestamp IN ('1969-09-28', '1969-09-29', '1969-09-30',
                                    '1969-10-01', '1969-10-02', '1969-10-03',
                                    '1969-10-04', '1969-10-05', '1969-10-06',
                                    '1969-10-07', '1969-10-08', '1969-10-09',
                                    '1969-10-10', '1969-10-11', '1969-10-11',
                                    '1969-01-01 17:45:30.005', '1969-01-01',
                                    '1969-01-15', '1969-12-25', '1969-12-25',
                                    '1969-11-25 17:45:30', '1969-12-25',
                                    '1969-10-01 20:45:30', '1969-12-31');""",
           """SELECT cdate
              FROM   test_busrt_scalar_op
              WHERE  cdate IN ('1965-09-28', '1969-01-01', '1969-09-25',
                               '1969-09-30', '1969-10-01', '1969-10-02',
                               '1969-10-03', '1969-10-04', '1969-10-05',
                               '1969-10-06', '1969-10-07', '1969-10-08',
                               '1969-10-09', '1969-10-10', '1969-10-11',
                               '1968-01-01', '1967-01-01', '1969-01-15',
                               '1968-01-01', '1967-01-01', '1969-01-15',
                               '1969-12-25', '1968-12-31');""",
           """SELECT cint
              FROM   test_busrt_scalar_op
              WHERE  cint IN (49859, 49860, 49861, 49862,
                              49863, 49864, 49865, 49866,
                              49867, 49868, 49869, 49872,
                              49873, 49874, 49875)
                     AND cbigint > 0;""",
           """SELECT Count(*)
              FROM   test_busrt_scalar_op
              WHERE  cbigint IN (123451, 1234522, 1234523, 123454,
                                 123455, 123456, 123457, 123458,
                                 123459, 1234510, 1234511, 1234512,
                                 1234513, 1234514, 9223372036854775806);""",
           """SELECT Max(cvarchar)
              FROM   test_busrt_scalar_op
              WHERE  cvarchar IN (' 1', ' 2', ' 3', ' 4',
                                  ' 5', ' 6', ' 7', ' 8',
                                  ' 9', ' 10', ' 11', ' 12',
                                  ' 12345.49 ', ' 100', ' 9999999999');""",
           """SELECT a.cbigint,
                     b. cvarchar
              FROM   test_busrt_scalar_op a
              JOIN   test_busrt_scalar_op b ON a.cint = b.cint
              WHERE  a.cbigint IN (123451, 1234522, 1234523, 123454,
                                   123455, 123456, 123457, 123458,
                                   123459, 1234510, 1234511, 1234512,
                                   1234513, 1234514, 9223372036854775806 )
                     AND b.cvarchar IN (' 1', ' 2', ' 3', ' 4',
                                        ' 5', ' 6', ' 7', ' 8',
                                        ' 9', ' 10', ' 11', ' 12',
                                        ' 12345.49 ', ' 100',
                                        ' 9999999999');""",
           """SELECT cint,
                     cfloat,
                     ccase
              FROM   test_busrt_scalar_op
              WHERE  cint IN (49859, 49860, 49861, 49862,
                              49863, 49864, 49865, 49866,
                              49867, 49868, 49869, 49872,
                              49873, 49874, 49875)
                     AND ctimestamp IN ('1985-09-28', '1985-09-29',
                                        '1985-09-30', '1985-10-01',
                                        '1985-10-02', '1985-10-03',
                                        '1985-10-04', '1985-10-05',
                                        '1985-10-06', '1985-10-07',
                                        '1985-10-08', '1985-10-09',
                                        '1985-10-10', '1985-10-11',
                                        '2000-01-01 17:45:30.005',
                                        '2017-01-01', '2017-01-15',
                                        '2017-12-25', '1985-11-25 17:45:30',
                                        '1985-10-01 20:45:30',
                                        '9999-12-31');""",
           """SELECT cint,
                     cfloat,
                     ccase
              FROM   test_busrt_scalar_op
              WHERE  cint IN (49859, 49860, 49861, 49862,
                              49863, 49864, 49865, 49866,
                              49867, 49868, 49869, 49872,
                              49873, 49874, 49875)
                     OR cfloat IN (123451.02, 123452.01, 123453.3, 123454.1,
                                   123455.31, 123456.02, 123457.1, 123458.011,
                                   123459.11, 1234510.1, 1234511.1, 1234512.1,
                                   1234513.1, 9e+18, 1234534.57);""",
           """SELECT cint,
                     cfloat,
                     ccase
              FROM   test_busrt_scalar_op
              WHERE  cint IN (49859, 49860, 49861, 49862,
                              49863, 49864, 49865, 49866,
                              49867, 49868, 49869, 49872,
                              49873, 49874, 49875)
                     AND ctimestamp IN ( '1985-09-28', '1985-09-29',
                                         '1985-09-30', '1985-10-01',
                                         '1985-10-02', '1985-10-03',
                                         '1985-10-04', '1985-10-05',
                                         '1985-10-06', '1985-10-07',
                                         '1985-10-08', '1985-10-09',
                                         '1985-10-10', '1985-10-11',
                                         '2000-01-01 17:45:30.005',
                                         '2017-01-01', '2017-01-15',
                                         '2017-12-25', '1985-11-25 17:45:30',
                                         '1985-10-01 20:45:30', '9999-12-31')
                     OR ccase IN (' 1', ' 2', ' 3', ' 4',
                                  ' 5', ' 6', ' 7', ' 8',
                                  ' 9', ' 10', ' 11', ' op',
                                  ' round_down', ' 10000', ' agg');"""]

# Validate if rrscan works by checking scanned blocks.
VALIDATION = ('SELECT segment, sum(num_blocks_to_scan) as num_blocks_to_scan, '
              'sum(total_blocks) as total_blocks '
              'FROM stl_scan_range_stats WHERE query in ({}) '
              'GROUP BY query, segment '
              'ORDER BY query, segment, num_blocks_to_scan, total_blocks;')

# Get Burst query id.
BURST_QUERY_ID = """select concurrency_scaling_query
                    from stl_concurrency_scaling_query_mapping
                    where primary_query = {};"""


@pytest.mark.localhost_only
@pytest.mark.burst_precommit
@pytest.mark.serial_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(
    gucs=dict(BURST_DISABLE_C2S3_GUCS, rr_dist_num_rows=1),
    initdb_before=True,
    initdb_after=True)
class TestBurstRRScanForInList(BurstTest):
    @contextmanager
    def burst_db_session(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst")
            yield cursor
            cursor.execute("reset query_group")

    def burst_inlist_rrscan_efficiency(self, cluster, db_session, queries,
                                       result):
        '''Helper function to check rrscan efficiency.'''
        with db_session.cursor() as cursor:
            cursor.execute(CREATE_TABLE_STMT)
            cursor.execute(INSERT_STMT)

        SNAPSHOT_IDENTIFIER = (
            "{}-{}".format(cluster.cluster_identifier, str(uuid.uuid4().hex))
        )
        cluster.backup_cluster(SNAPSHOT_IDENTIFIER)

        # Array to collect all Burst query ids.
        BQIDS = []
        for query in queries:
            QID = None
            with self.burst_db_session(db_session) as cursor:
                # Execute Burst query in a normal user session.
                cursor.execute(query)
                cursor.execute("select pg_last_query_id()")
                # Get primary query id.
                QID = cursor.fetch_scalar()
                log.info("Primary query id: {}".format(QID))

            with self.db.cursor() as bootstrap_cursor:
                # Get Burst query id.
                bootstrap_cursor.execute(BURST_QUERY_ID.format(QID))
                BQID = bootstrap_cursor.fetch_scalar()
                log.info("Burst query id: {}".format(BQID))
                BQIDS.append(BQID)

        with self.db.cursor() as bootstrap_cursor:
            # Check the rrscan efficiency by querying stl_scan_range_stats.
            # Scanned block should be all 0.
            bootstrap_cursor.execute(
                VALIDATION.format(", ".join([str(x) for x in BQIDS]))
            )
            assert bootstrap_cursor.fetchall() == result

    def test_burst_inlist_rrscan_correctness(self, db_session, cluster):
        '''This test is to check the correctness of Burst rrscan for
           scalar array operation. We validate the result by executing
           the yaml file generated by Burst queries using boostrap user.'''
        with self.auto_release_local_burst(cluster):
            with db_session.cursor() as cursor:
                cursor.execute(CREATE_TABLE_STMT)
                cursor.execute(INSERT_STMT)

            SNAPSHOT_IDENTIFIER = (
                "{}-{}".format(cluster.cluster_identifier,
                               str(uuid.uuid4().hex))
            )
            cluster.backup_cluster(SNAPSHOT_IDENTIFIER)
            self.execute_test_file(
                'busrt_inlist_rrscan_correctness', session=db_session
            )
            # Run as boostrap user to validate the result.
            with self.db.cursor():
                self.execute_test_file(
                    'busrt_inlist_rrscan_correctness', session=db_session
                )
