# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
from itertools import product
from contextlib import contextmanager
from collections import namedtuple

from raff.common.host_type import HostType
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_test import setup_teardown_burst
from raff.burst.burst_write import burst_write_basic_gucs
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.storage.storage_test import disable_all_autoworkers
from test_burst_mv_refresh import TestBurstWriteMVBase
log = logging.getLogger(__name__)
__all__ = [disable_all_autoworkers, super_simulated_mode, setup_teardown_burst]

VC_LEN = 64
CREATE_TABLE = ("CREATE TABLE IF NOT EXISTS {} ("
                " c0 int, c1 int, c2 varchar(" + str(VC_LEN) + ")"
                " ) {} {}")
MY_TABLE = "test_table"
CREATE_TEST_TABLE = ("create table if not exists " + MY_TABLE + " (c0 int)"
                     " diststyle even")
DELETE = "delete from {} where c0 > 5"
DROP_TABLE = "drop table if exists {} cascade"
DROP_SCHEMA = "DROP SCHEMA IF EXISTS {} CASCADE"
INSERT = "insert into {} values {}"
MY_SCHEMA = "test_schema"
MY_USER = "test_user"
SELECT = "select * from {} order by 1,2"
UPDATE = "update {} set c0 = c0 + 2 where c0 > 4"

reldata = namedtuple('reldata', ['relname', 'dist', 'sort'])

diststyle = ['diststyle even', "distkey(c0)"]
sortkey = ['sortkey(c0)', '']


@pytest.mark.serial_only
@pytest.mark.skip_load_data
@pytest.mark.usefixtures("disable_all_autoworkers")
class TestBurstWriteMaxClustersBase(TestBurstWriteMVBase):

    def _check_rows_are_same(self, cluster, cursor, rdata):
        """
            Validates that following 4 tables have same rows
                - table on main
                - table on burst
        """

        table, dist, sort = rdata
        log.info("Validating data is same on main and burst clusters")

        cursor.execute("set query_group to metrics")

        cursor.execute(SELECT.format(table))
        table_rows_on_main = tuple(sorted(cursor.fetchall()))
        self._check_last_query_didnt_burst(cluster, cursor)

        cursor.execute("set query_group to burst")

        cursor.execute(SELECT.format(table))
        table_rows_on_burst = tuple(sorted(cursor.fetchall()))
        self._check_last_query_bursted(cluster, cursor)

        assert table_rows_on_main == table_rows_on_burst, \
               ("[FAIL]: Expected same table-rows on main & burst clusters")

    def _run_dmls(self, cluster, cursor, tables, try_burst, should_burst,
                  no_burst_status=0):
        """
            try_burst - True if try to burst, else False
            should_burst - True if query should burst, else false

            Runs INSERT/DELETE/UPDATE/COPY on the cluster
        """

        status = no_burst_status
        log.info("try_burst = {}, should_burst = {}".format(try_burst,
                                                            should_burst))
        if try_burst:
            cursor.execute("set query_group to burst")
        else:
            cursor.execute("set query_group to metrics")

        for rdata in tables:

            table, dist, sort = rdata

            self._do_copy(cursor, table)
            if try_burst and should_burst:
                self._check_last_copy_bursted(cluster, cursor)
            elif try_burst and not should_burst:
                self.check_last_copy_not_bursted_status(cluster, cursor,
                                                        status)

            cursor.execute(DELETE.format(table))
            if try_burst and should_burst:
                self._check_last_query_bursted(cluster, cursor)
            elif try_burst and not should_burst:
                self.check_last_query_didnt_burst_status(cluster, cursor,
                                                         status)

            cursor.execute(INSERT.format(table, self._values()))
            if try_burst and should_burst:
                self._check_last_query_bursted(cluster, cursor)
            elif try_burst and not should_burst:
                self.check_last_query_didnt_burst_status(cluster, cursor,
                                                         status)

            cursor.execute(UPDATE.format(table))
            if try_burst and should_burst:
                self._check_last_query_bursted(cluster, cursor)
            elif try_burst and not should_burst:
                self.check_last_query_didnt_burst_status(cluster, cursor,
                                                         status)

            if try_burst and should_burst:
                self._check_ownership(MY_SCHEMA, table,
                                      [("Burst", "Owned")], cluster)
                self._check_rows_are_same(cluster, cursor, rdata)
            elif try_burst and not should_burst:
                self._check_ownership(MY_SCHEMA, table,
                                      [], cluster)
            self._check_table(cluster, MY_SCHEMA, table, dist)

    @contextmanager
    def setup_tables(self, cluster, cursor, relprefix, diststyle, sortkey,
                     clean_on_exit=True):
        """
            Sets up tables for the test.

            Args:
                relprefix (str): User defined table name prefix
                clean_on_exit (bool): If true, tables are deleted when
                test fails. Else, they are not deleted.
        """
        # Create test table for health check later
        sql = ";".join([DROP_TABLE.format(MY_TABLE), CREATE_TEST_TABLE])
        cursor.execute(sql)

        # Create test tables
        res = []
        for relid, t in enumerate(product(diststyle, sortkey)):
            dist, sort = t
            relname = "_".join([relprefix, "table", str(relid)])
            sql = ";".join([DROP_TABLE.format(relname),
                            CREATE_TABLE.format(relname, dist, sort),
                            INSERT.format(relname, self._values(1))])
            cursor.execute(sql)
            res.append(reldata(relname, dist, sort))

        # Release all previous burst clusters
        # Ensure we have exactly one burst cluster attached
        cluster.run_xpx("event unset EtExceedMaxBurstWriteClusters")
        if cluster.host_type == HostType.CLUSTER:
            cluster.release_all_burst_clusters()
            cluster.run_xpx('burst_update_max_clusters 1')
        log.info("Start backup and wait for burst refresh")
        self._start_and_wait_for_refresh(cluster)
        self._check_burst_cluster_health(cluster, cursor, MY_TABLE)
        yield res
        # Save schema for debugging
        self._check_burst_cluster_health(cluster, cursor, MY_TABLE)
        if clean_on_exit:
            cursor.execute(DROP_SCHEMA.format(MY_SCHEMA))

    def base_test_burst_write_max_clusters(self, cluster):
        """
            This test checks if EtExceedMaxBurstWriteClusters works.
            It does the following:
                1. Sets up tables for bursting.
                2. Runs dmls on the tables and ensures they burst
                3. Set the event EtExceedMaxBurstWriteClusters
                4. Runs dmls on the tables and ensures they DO NOT burst
                5. Unset the event EtExceedMaxBurstWriteClusters
                6. Runs dmls on the tables and ensures they burst
        """
        relprefix = "burst_write_max_clusters"
        session1 = DbSession(cluster.get_conn_params(),
                             session_ctx=SessionContext(username=MY_USER,
                                                        schema=MY_SCHEMA))
        cur1 = session1.cursor()
        with self.setup_tables(cluster, cur1, relprefix, diststyle, sortkey,
                               clean_on_exit=False) as tables:
            cur1.execute("set query_group to burst")

            self._run_dmls(cluster, cur1, tables, True, True)
            cluster.run_xpx("event set EtExceedMaxBurstWriteClusters")
            self._run_dmls(cluster, cur1, tables, True, False, 61)
            cluster.run_xpx("event unset EtExceedMaxBurstWriteClusters")
            log.info("Start backup and wait for burst refresh")
            self._start_and_wait_for_refresh(cluster)
            self._run_dmls(cluster, cur1, tables, True, True)


@pytest.mark.cluster_only
@pytest.mark.custom_burst_gucs(
        gucs=dict(list(burst_write_basic_gucs.items()) + [
            ('burst_percent_threshold_to_trigger_backup', '100'),
            ('burst_cumulative_time_since_stale_backup_threshold_s', '86400'),
            ('burst_commit_refresh_check_frequency_seconds', '-1'),
            ('burst_write_max_concurrent_clusters', '99'),
            ('enable_burst_async_acquire', 'false')
            ]))
@pytest.mark.usefixtures("setup_teardown_burst")
class TestBurstWriteMaxClustersCluster(TestBurstWriteMaxClustersBase):
    def test_burst_write_max_clusters_cluster(self, cluster):
        self.base_test_burst_write_max_clusters(cluster)


@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=list(burst_write_basic_gucs.items()) +
                               [('slices_per_node', '3')])
@pytest.mark.custom_local_gucs(gucs=dict(list(burst_write_basic_gucs.items()) + [
            ('burst_percent_threshold_to_trigger_backup', '100'),
            ('burst_cumulative_time_since_stale_backup_threshold_s', '86400'),
            ('burst_write_max_concurrent_clusters', '99')
                            ]))
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteMaxClustersSS(TestBurstWriteMaxClustersBase):
    def test_burst_write_max_clusters_ss(self, cluster):
        self.base_test_burst_write_max_clusters(cluster)
