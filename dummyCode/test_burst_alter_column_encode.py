# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import logging
import uuid
import datetime
import time

from raff.data.data_utils import load_table
from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import (get_burst_cluster_arn, setup_teardown_burst,
                                   verify_query_bursted,
                                   verify_query_didnt_burst)
from raff.common.cluster.cluster_helper import RedshiftClusterHelper
from raff.storage.storage_test import disable_all_autoworkers

__all__ = [
    setup_teardown_burst, verify_query_bursted, verify_query_didnt_burst,
    disable_all_autoworkers
]

log = logging.getLogger(__name__)

ALTER_DISTAUTO = "alter table {} alter diststyle auto"
ALTER_DISTEVEN = "alter table {} alter diststyle even"
ALTER_DISTALL = "alter table {} alter diststyle all"
ALTER_DISTKEY = "alter table {} alter distkey {}"
ALTER_ENCODE = "alter table {} alter column {} encode {}"
ALTER_ENCODE_MULTI_COLUMNS = (
    "alter table {} alter column {} encode {}, alter "
    "column {} encode {}")

# TPCH query #8
BURST_CANARY_QUERY_1 = '''
select count(*) from customer_alter_encode;
'''
BURST_CANARY_QUERY_LARGE = '''
select count(*) from supplier_large_test{};
'''
BURST_CANARY_QUERY_SMALL = '''
select count(*) from region_small_test{};
'''
BURST_CANARY_QUERY_1_MULTI = '''
select count(*) from customer_alter_encode_multi;
'''
BURST_CANARY_QUERY_LARGE_MULTI = '''
select count(*) from supplier_large_test_multi{};
'''
BURST_CANARY_QUERY_SMALL_MULTI = '''
select count(*) from region_small_test_multi{};
'''

CUSTOM_ALTER_GUCS = {
    'enable_alter_column_encode': "true",
    "enable_alter_multi_column_encode": "true",
    "alter_column_encode_config": "1",
    'enable_alter_distauto': "true",
    'enable_alter_disteven': "true",
    'enable_alter_distkey': "true",
    'enable_alter_distall': "true",
    'burst_percent_threshold_to_trigger_backup': 100,
    'burst_cumulative_time_since_stale_backup_threshold_s': 86400,
    'burst_commit_refresh_check_frequency_seconds': -1,
    'enable_burst_lag_based_background_refresh': "false"
}


@pytest.mark.usefixtures("disable_all_autoworkers")
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(
    gucs=dict(**CUSTOM_ALTER_GUCS), initdb_before=True, initdb_after=True)
@pytest.mark.serial_only
@pytest.mark.cluster_only
class TestBurstAlterColumnEncode(BurstTest):
    def take_snapshot(self, cluster):
        """
        Take a snapshot.

        Args:
            cluster: cluster object
        """
        snapshot_identifier = ("{}-{}".format(cluster.cluster_identifier,
                                              str(uuid.uuid4().hex)))
        log.info("take snapshot {}".format(snapshot_identifier))
        cluster.backup_cluster(snapshot_identifier)
        return snapshot_identifier

    def setup_data(self, db_session):
        """
        Create tables based on TPCH and load data via COPY.
        Inilization tables include customer, region, supplier.
        The DDLs is as followed:
        create table customer (
            c_custkey int8 not null ,
            c_name varchar(25) not null,
            c_address varchar(40) not null,
            c_nationkey int4 not null,
            c_phone char(15) not null,
            c_acctbal numeric(12,2) not null,
            c_mktsegment char(10) not null,
            c_comment varchar(117) not null,
            Primary Key(C_CUSTKEY)
        ) distkey(c_custkey) sortkey(c_custkey);

        create table region (
            r_regionkey int4 not null,
            r_name char(25) not null ,
            r_comment varchar(152) not null,
            Primary Key(R_REGIONKEY)
        ) distkey(r_regionkey) sortkey(r_regionkey);

        create table supplier (
            s_suppkey int4 not null,
            s_name char(25) not null,
            s_address varchar(40) not null,
            s_nationkey int4 not null,
            s_phone char(15) not null,
            s_acctbal numeric(12,2) not null,
            s_comment varchar(101) not null,
            Primary Key(S_SUPPKEY)
        ) distkey(s_suppkey) sortkey(s_suppkey);
        """
        load_table(
            db=db_session,
            dataset_name='tpch',
            table_name='customer',
            scale='1',
            table_name_suffix='_alter_encode',
            grant_to_public=True)

        load_table(
            db=db_session,
            dataset_name='tpch',
            table_name='customer',
            scale='1',
            table_name_suffix='_alter_encode_multi',
            grant_to_public=True)

        for i in range(4):
            load_table(
                db=db_session,
                dataset_name='tpch',
                table_name='region',
                scale='1',
                table_name_suffix='_small_test{}'.format(i),
                grant_to_public=True)
            load_table(
                db=db_session,
                dataset_name='tpch',
                table_name='supplier',
                scale='1',
                table_name_suffix='_large_test{}'.format(i),
                grant_to_public=True)

            load_table(
                db=db_session,
                dataset_name='tpch',
                table_name='region',
                scale='1',
                table_name_suffix='_small_test_multi{}'.format(i),
                grant_to_public=True)
            load_table(
                db=db_session,
                dataset_name='tpch',
                table_name='supplier',
                scale='1',
                table_name_suffix='_large_test_multi{}'.format(i),
                grant_to_public=True)

        with db_session.cursor() as cursor:
            # setup small and large all/key/even tables for testing
            # `alter table alter column encode`
            # Set up tables region_small_test
            cursor.execute(ALTER_DISTALL.format('region_small_test0'))
            cursor.execute(
                ALTER_DISTKEY.format('region_small_test1', 'r_name'))
            cursor.execute(ALTER_DISTEVEN.format('region_small_test2'))
            cursor.execute(ALTER_DISTAUTO.format('region_small_test3'))
            # Set up tables supplier_large_test
            cursor.execute(ALTER_DISTALL.format('supplier_large_test0'))
            cursor.execute(
                ALTER_DISTKEY.format('supplier_large_test1', 's_name'))
            cursor.execute(ALTER_DISTEVEN.format('supplier_large_test2'))
            cursor.execute(ALTER_DISTAUTO.format('supplier_large_test3'))
            # Do the same for tables of Multi-ACE test
            cursor.execute(ALTER_DISTALL.format('region_small_test_multi0'))
            cursor.execute(
                ALTER_DISTKEY.format('region_small_test_multi1', 'r_name'))
            cursor.execute(ALTER_DISTEVEN.format('region_small_test_multi2'))
            cursor.execute(ALTER_DISTAUTO.format('region_small_test_multi3'))
            cursor.execute(ALTER_DISTALL.format('supplier_large_test_multi0'))
            cursor.execute(
                ALTER_DISTKEY.format('supplier_large_test_multi1', 's_name'))
            cursor.execute(ALTER_DISTEVEN.format('supplier_large_test_multi2'))
            cursor.execute(ALTER_DISTAUTO.format('supplier_large_test_multi3'))

    def _cleanup_tables(self, cursor):
        cursor.execute("drop table if exists customer_alter_encode;")
        for i in range(4):
            cursor.execute(
                "drop table if exists region_small_test{};".format(i))
            cursor.execute(
                "drop table if exists supplier_large_test{};".format(i))
        cursor.execute("drop table if exists customer_alter_encode_multi;")
        for i in range(4):
            cursor.execute(
                "drop table if exists region_small_test_multi{};".format(i))
            cursor.execute(
                "drop table if exists supplier_large_test_multi{};".format(i))

    def _run_alter_encode(self, cursor, tbl, col, encode):
        cmd = ALTER_ENCODE.format(tbl, col, encode)
        cursor.execute(cmd)

    def _run_alter_encode_multi_columns(self, cursor, tbl, col1, encode1, col2,
                                        encode2):
        cmd = ALTER_ENCODE_MULTI_COLUMNS.format(tbl, col1, encode1, col2,
                                                encode2)
        cursor.execute(cmd)

    def _run_alters(self, cursor):
        # Run ACE on tables
        log.info("run alter column encode")
        self._run_alter_encode(cursor, "customer_alter_encode", "c_custkey",
                               "LZO")
        for i in range(4):
            self._run_alter_encode(
                cursor, "region_small_test{}".format(i), "r_regionkey", "LZO")
            self._run_alter_encode(
                cursor, "supplier_large_test{}".format(i), "s_suppkey", "LZO")

        # Run Multi-ACE on tables
        log.info("run alter encode multiple columns")
        self._run_alter_encode_multi_columns(
            cursor, "customer_alter_encode_multi", "c_custkey", "LZO",
            "c_nationkey", "LZO")
        for i in range(4):
            self._run_alter_encode_multi_columns(
                cursor, "region_small_test_multi{}".format(i), "r_regionkey",
                "LZO", "r_name", "RUNLENGTH")
            self._run_alter_encode_multi_columns(
                cursor, "supplier_large_test_multi{}".format(i), "s_suppkey",
                "LZO", "s_nationkey", "LZO")

    def _run_burst_queries(self, burst_cursor):
        burst_cursor.execute(BURST_CANARY_QUERY_1)
        for i in range(4):
            log.info("run burst test on {}".format(
                BURST_CANARY_QUERY_SMALL.format(i)))
            burst_cursor.execute(BURST_CANARY_QUERY_SMALL.format(i))
            log.info("run burst test on {}".format(
                BURST_CANARY_QUERY_LARGE.format(i)))
            burst_cursor.execute(BURST_CANARY_QUERY_LARGE.format(i))
        # Run the same queries for Multi-ACE tables
        burst_cursor.execute(BURST_CANARY_QUERY_1_MULTI)
        for i in range(4):
            log.info("run burst test on {}".format(
                BURST_CANARY_QUERY_SMALL_MULTI.format(i)))
            burst_cursor.execute(BURST_CANARY_QUERY_SMALL_MULTI.format(i))
            log.info("run burst test on {}".format(
                BURST_CANARY_QUERY_LARGE_MULTI.format(i)))
            burst_cursor.execute(BURST_CANARY_QUERY_LARGE_MULTI.format(i))

    def test_burst_alter_encode_with_burst(self, db_session, cluster,
                                           verify_query_bursted):
        """
        Test runs ALTER COLUMN ENCODE on different tables and confirms it can
        still have queries run against it burst successfully.
        """

        # Ensure that the test has a burst cluster to operate on
        retries_left = 5
        while get_burst_cluster_arn(cluster) is None:
            self.take_snapshot(cluster)
            cluster.acquire_burst_cluster()
            if get_burst_cluster_arn(cluster) is None:
                time.sleep(60)
                retries_left -= 1
                assert retries_left >= 0, "Unable to acquire a burst cluster"

        # Personalize the burst cluster such that refresh can happen
        cluster.personalize_burst_cluster(get_burst_cluster_arn(cluster))

        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')

        with db_session.cursor() as cursor:
            self.setup_data(db_session)
            # RUN ALTER COMMAND
            self._run_alters(cursor)
            snapshot_identifier = self.take_snapshot(cluster)
            self.wait_for_refresh_to_start(cluster, start_str,
                                           get_burst_cluster_arn(cluster))
            # Run query and validate the query was bursted
            with self.burst_db_cursor(db_session) as burst_db_cursor:
                self._run_burst_queries(burst_db_cursor)
            self._cleanup_tables(cursor)
            RedshiftClusterHelper.delete_snapshot(snapshot_identifier)

    def test_burst_alter_encode_without_burst(self, cluster_session,
                                              db_session, cluster,
                                              verify_query_didnt_burst):
        """
        Test runs ALTER COLUMN ENCODE on a table and confirms it does not burst
        if no new snapshot is taken.
        Test is disabled for CBC as snapshot is not required for commit
        based cold start. Query can be bursted without snapshot.
        """

        # Ensure that the test has a burst cluster to operate on
        retries_left = 5
        while get_burst_cluster_arn(cluster) is None:
            self.take_snapshot(cluster)
            cluster.acquire_burst_cluster()
            if get_burst_cluster_arn(cluster) is None:
                time.sleep(60)
                retries_left -= 1
                assert retries_left >= 0, "Unable to acquire a burst cluster"

        # Personalize the burst cluster such that refresh can happen
        cluster.personalize_burst_cluster(get_burst_cluster_arn(cluster))

        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')
        # Disable CBC here because queries in setup table will be bursted
        # even we don't have snapshot
        custom_gucs = {'enable_burst_s3_commit_based_cold_start': 'false'}

        with cluster_session(gucs=custom_gucs), db_session.cursor() as cursor:
            self.setup_data(db_session)

            snapshot_identifier = self.take_snapshot(cluster)
            self.wait_for_refresh_to_start(cluster, start_str,
                                           get_burst_cluster_arn(cluster))
            # RUN ALTER COMMAND
            self._run_alters(cursor)
            # Run query and validate the query was not bursted
            with self.burst_db_cursor(db_session) as burst_db_cursor:
                self._run_burst_queries(burst_db_cursor)
            self._cleanup_tables(cursor)
            RedshiftClusterHelper.delete_snapshot(snapshot_identifier)
