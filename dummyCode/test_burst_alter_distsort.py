# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import logging
import uuid
import datetime
from raff.data.data_utils import load_table
from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import (get_burst_cluster_arn, setup_teardown_burst,
                                   verify_query_bursted,
                                   verify_query_didnt_burst)
from raff.common.cluster.cluster_helper import RedshiftClusterHelper
from raff.storage.storage_test import disable_all_autoworkers

__all__ = [
    "setup_teardown_burst", "verify_query_bursted", "verify_query_didnt_burst"
]

log = logging.getLogger('test_burst_alter_distsort')

ALTER_STMT = ("alter table customer_alter_distsort alter distkey c_nationkey,"
              "alter sortkey(c_custkey, c_nationkey);")
ALTER_DISTAUTO = "alter table {} alter diststyle auto"
ALTER_DISTEVEN = "alter table {} alter diststyle even"
ALTER_DISTALL = "alter table {} alter diststyle all"
ALTER_DISTKEY = "alter table {} alter distkey {}"
ALTER_SORTKEY = "alter table {} alter sortkey ({})"
ALTER_SORTKEY_AUTO = "alter table {} alter sortkey auto"
ALTER_SORTKEY_NONE = "alter table {} alter sortkey none"
ALTER_SORTKEY_NO_SORT = "alter table {} alter sortkey ({}) no sort"

# TPCH query #8
BURST_CANARY_QUERY_1 = '''
select count(*) from customer_alter_distsort;
'''
BURST_CANARY_SORTKEY_AUTO_QUERY = '''
select count(*) from supplier_large_test6;
'''
BURST_CANARY_SORTKEY_NONE_QUERY = '''
select count(*) from supplier_large_test{};
'''
BURST_CANARY_SORTKEY_ZINDEX_QUERY = '''
select count(*) from {}_zindex_test{};
'''
BURST_CANARY_QUERY_LARGE = '''
select count(*) from supplier_large_test{};
'''
BURST_CANARY_QUERY_SMALL = '''
select count(*) from region_small_test{};
'''

CUSTOM_ALTER_GUCS = {
    'enable_alter_distauto': "true",
    'enable_alter_disteven': "true",
    'enable_alter_distkey': "true",
    'enable_alter_distall': "true",
    'enable_sortkey_auto': "true",
    'enable_alter_sortkey_auto': "true",
    'enable_alter_sortkey_none': "true",
    'enable_alter_sortkey_on_zindex': "true",
    "enable_alter_sortkey_no_sort_on_zindex": "true",
    'small_table_row_threshold': 20,
    'small_table_row_lower_bound': 1,
    'small_table_block_threshold': 2,
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
class TestBurstAlterDistSort(BurstTest):
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
        Create the load the data to be used in the test.

        Args:
            db_session: db_session object
        """
        load_table(
            db=db_session,
            dataset_name='tpch',
            table_name='customer',
            scale='1',
            table_name_suffix='_alter_distsort',
            grant_to_public=True)

        for i in range(1, 7):
            load_table(
                db=db_session,
                dataset_name='tpch',
                table_name='region',
                scale='1',
                table_name_suffix='_small_test{}'.format(i),
                grant_to_public=True)
        for i in range(1, 9):
            load_table(
                db=db_session,
                dataset_name='tpch',
                table_name='supplier',
                scale='1',
                table_name_suffix='_large_test{}'.format(i),
                grant_to_public=True)
        # load zindexed tables
        for i in range(3):
            # distkey table
            load_table(
                db=db_session,
                dataset_name='tpch',
                table_name='supplier_zindex',
                scale='1',
                table_name_suffix='_test{}'.format(i),
                grant_to_public=True)
            # distall table
            load_table(
                db=db_session,
                dataset_name='tpch',
                table_name='nation_zindex',
                scale='1',
                table_name_suffix='_test{}'.format(i),
                grant_to_public=True)
            # disteven table
            load_table(
                db=db_session,
                dataset_name='tpch',
                table_name='customer_zindex',
                scale='1',
                table_name_suffix='_test{}'.format(i),
                grant_to_public=True)

        with db_session.cursor() as cursor:
            # setup small and large all/key/even tables for testing
            # `alter table alter diststyle auto` and
            # setup all/autoall/key/autokey/autoeven tables for testing
            # `alter table alter diststyle even`
            cursor.execute(ALTER_DISTALL.format('region_small_test1'))
            cursor.execute(
                ALTER_DISTKEY.format('region_small_test2', 'r_name'))
            cursor.execute(ALTER_DISTEVEN.format('region_small_test3'))
            cursor.execute(ALTER_DISTALL.format('supplier_large_test1'))
            cursor.execute(
                ALTER_DISTKEY.format('supplier_large_test2', 's_name'))
            cursor.execute(ALTER_DISTEVEN.format('supplier_large_test3'))

            cursor.execute(ALTER_DISTALL.format('region_small_test4'))
            cursor.execute(ALTER_DISTALL.format('region_small_test5'))
            cursor.execute(ALTER_DISTAUTO.format('region_small_test5'))
            cursor.execute(
                ALTER_DISTKEY.format('region_small_test6', 'r_name'))
            cursor.execute(
                ALTER_DISTKEY.format('supplier_large_test4', 's_name'))
            cursor.execute(ALTER_DISTAUTO.format('supplier_large_test4'))
            cursor.execute(ALTER_DISTEVEN.format('supplier_large_test5'))
            cursor.execute(ALTER_DISTAUTO.format('supplier_large_test5'))
            # setup table for testing `alter table alter sortkey auto`
            cursor.execute(
                ALTER_SORTKEY.format('supplier_large_test6', 's_nationkey'))
            # setup table for testing `alter table alter sortkey none
            # 1.set up a non-sortkey-auto table
            cursor.execute(
                ALTER_SORTKEY.format('supplier_large_test7', 's_nationkey'))
            # 2. set up a sortkey-auto table
            cursor.execute(
                ALTER_SORTKEY.format('supplier_large_test8', 's_nationkey'))
            cursor.execute(ALTER_SORTKEY_AUTO.format('supplier_large_test8'))

    def _cleanup_tables(self, cursor):
        cursor.execute("drop table if exists customer_alter_distsort;")
        for i in range(1, 7):
            cursor.execute(
                "drop table if exists region_small_test{};".format(i))
        for i in range(1, 9):
            cursor.execute(
                "drop table if exists supplier_large_test{};".format(i))
        for i in range(3):
            cursor.execute(
                "drop table if exists supplier_zindex_test{};".format(i))
            cursor.execute(
                "drop table if exists nation_zindex_test{};".format(i))
            cursor.execute(
                "drop table if exists customer_zindex_test{};".format(i))

    def _run_alters(self, cursor):
        log.info("run alter dist sort")
        cursor.execute(ALTER_STMT)
        # test `alter table alter diststyle auto`
        # setup alter auto on small all table
        log.info("run alter diststyle auto on small all table")
        cursor.execute(ALTER_DISTAUTO.format('region_small_test1'))

        # setup alter auto on small key table
        log.info("run alter diststyle auto on small key table")
        cursor.execute(ALTER_DISTAUTO.format('region_small_test2'))

        # setup alter auto on small even table
        log.info("run alter diststyle auto on small even table")
        cursor.execute(ALTER_DISTAUTO.format('region_small_test3'))

        # setup alter auto on large all table
        log.info("run alter diststyle auto on large all table")
        cursor.execute(ALTER_DISTAUTO.format('supplier_large_test1'))

        # setup alter auto on large key table
        log.info("run alter diststyle auto on large key table")
        cursor.execute(ALTER_DISTAUTO.format('supplier_large_test2'))

        # setup alter auto on large even table
        log.info("run alter diststyle auto on large even table")
        cursor.execute(ALTER_DISTAUTO.format('supplier_large_test3'))

        # test `alter table alter diststyle even`
        # setup alter even on all table
        log.info("run alter diststyle even on all table")
        cursor.execute(ALTER_DISTEVEN.format('region_small_test4'))

        # setup alter even on auto-all table
        log.info("run alter diststyle even on auto-all table")
        cursor.execute(ALTER_DISTEVEN.format('region_small_test5'))

        # setup alter even on key table
        log.info("run alter diststyle even on key table")
        cursor.execute(ALTER_DISTEVEN.format('region_small_test6'))

        # setup alter even on auto-key table
        log.info("run alter diststyle even on auto-key table")
        cursor.execute(ALTER_DISTEVEN.format('supplier_large_test4'))

        # setup alter even on auto-even table
        log.info("run alter diststyle even on auto-even table")
        cursor.execute(ALTER_DISTEVEN.format('supplier_large_test5'))

        # setup alter sortkey auto
        log.info("run alter sortkey auto")
        cursor.execute(ALTER_SORTKEY_AUTO.format('supplier_large_test6'))

        # setup alter sortkey none
        log.info("run alter sortkey none")
        cursor.execute(ALTER_SORTKEY_NONE.format('supplier_large_test7'))
        cursor.execute(ALTER_SORTKEY_NONE.format('supplier_large_test8'))

        # setup alter sortkey on zindexed table
        log.info("run alter sortkey on zindexed table")
        cursor.execute(
            ALTER_SORTKEY.format('supplier_zindex_test0', 's_suppkey'))
        cursor.execute(
            ALTER_SORTKEY_NO_SORT.format('supplier_zindex_test1', 's_suppkey'))
        cursor.execute(ALTER_SORTKEY_NONE.format('supplier_zindex_test2'))
        cursor.execute(
            ALTER_SORTKEY.format('nation_zindex_test0', 'n_nationkey'))
        cursor.execute(
            ALTER_SORTKEY_NO_SORT.format('nation_zindex_test1', 'n_nationkey'))
        cursor.execute(ALTER_SORTKEY_NONE.format('nation_zindex_test2'))
        cursor.execute(
            ALTER_SORTKEY.format('customer_zindex_test0', 'c_custkey'))
        cursor.execute(
            ALTER_SORTKEY_NO_SORT.format('customer_zindex_test1', 'c_custkey'))
        cursor.execute(ALTER_SORTKEY_NONE.format('customer_zindex_test2'))

    def _run_burst_queries(self, burst_cursor):
        burst_cursor.execute(BURST_CANARY_QUERY_1)
        for i in range(1, 7):
            log.info("run burst test on {}".format(
                BURST_CANARY_QUERY_SMALL.format(i)))
            burst_cursor.execute(BURST_CANARY_QUERY_SMALL.format(i))
        for i in range(1, 6):
            log.info("run burst test on {}".format(
                BURST_CANARY_QUERY_LARGE.format(i)))
            burst_cursor.execute(BURST_CANARY_QUERY_LARGE.format(i))
        log.info(
            "run burst test on {}".format(BURST_CANARY_SORTKEY_AUTO_QUERY))
        burst_cursor.execute(BURST_CANARY_SORTKEY_AUTO_QUERY)
        for i in range(7, 9):
            log.info("run burst test on {}".format(
                BURST_CANARY_SORTKEY_NONE_QUERY.format(i)))
            burst_cursor.execute(BURST_CANARY_SORTKEY_NONE_QUERY.format(i))
        for tbl_prefix in ['supplier', 'nation', 'customer']:
            for i in range(3):
                zindex_burst_query = BURST_CANARY_SORTKEY_ZINDEX_QUERY.format(
                    tbl_prefix, i)
                log.info("run burst test on {}".format(zindex_burst_query))
                burst_cursor.execute(zindex_burst_query)

    def test_burst_alter_distsort_with_burst(self, db_session, cluster,
                                             verify_query_bursted):
        """
        Test runs ALTER DIST SORT, ALTER DISTSTYLE AUTO, ALTER DISTSTYLE
        EVEN, ALTER SORTKEY AUTO/NONE on different tables and confirms it
        can still have queries run against it burst successfully.
        """
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

    def test_burst_alter_distsort_without_burst(self, db_session, cluster,
                                                cluster_session,
                                                verify_query_didnt_burst):
        """
        Test runs ALTER DIST SORT, ALTER DISTSTYLE AUTO, ALTER DISTSTYLE
        EVEN and ALTER SORTKEY AUTO/NONE on a table and confirms it does not
        burst if no new snapshot is taken.
        Test is disabled for commit based cold start as snapshot is not
        required for CBC.
        """
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
