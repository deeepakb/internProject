# Copyright 2022 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from itertools import product
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_test import setup_teardown_burst
from raff.burst.burst_write import burst_write_mv_gucs
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.storage.storage_test import disable_all_autoworkers
from test_burst_mv_refresh import TestBurstWriteMVBase

log = logging.getLogger(__name__)
__all__ = [disable_all_autoworkers, super_simulated_mode, setup_teardown_burst]

VC_LEN = 64
# Added some more default columns for testing
CREATE_TABLE = ("CREATE TABLE IF NOT EXISTS {} ("
                " c0 int, c1 int, c2 varchar(" + str(VC_LEN) + "), "
                " c3 int default 2,"
                " c4 int default 3,"
                " c5 int default 5,"
                " c6 int default 7"
                " ) {} {}")
DELETE_ALL = "delete from {0}"
DROP_TABLE = "DROP TABLE IF EXISTS {} CASCADE"
INSERT = ("INSERT INTO {} VALUES {}")
MY_SCHEMA = "test_schema"
MY_TABLE = "test_table"
MY_USER = "test_user"
SELECT = "select * from {0} order by 1,2"
UPDATE = "update {0} set c0 = c0 + 2 where c0 > 4"

diststyle = ['diststyle even', "distkey(c0)"]
sortkey = ['sortkey(c0)', '']


class TestBurstWriteBasicBase(TestBurstWriteMVBase):
    """
    Base test class which is used to verify that no sig11 occur
    when executing a table's DML because of the AutoMV specific code
    paths. The below tests were introduced following the investigations
    made for DP-36112.
    """

    def _setup_source_tables(self, cluster, cursor):
        cursor.execute("set session authorization '{}'".format(MY_USER))
        tables = []
        for tbl_id, prop in enumerate(product(diststyle, sortkey)):
            t_dist, t_sort = prop
            table = "_".join([MY_TABLE, str(tbl_id)])
            cursor.execute(DROP_TABLE.format(table))
            cursor.execute(CREATE_TABLE.format(table, t_dist, t_sort))
            tables.append(table)
        self._start_and_wait_for_refresh(cluster)
        return tables

    def base_test_burst_basic(self, cluster):
        session = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))
        cursor = session.cursor()
        for table in self._setup_source_tables(cluster, cursor):
            cursor.execute("set session authorization '{}'".format(MY_USER))
            cursor.execute("set enable_result_cache_for_session to off")
            cursor.execute("set query_group to burst")
            cursor.execute(INSERT.format(table, self._values(1)))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(UPDATE.format(table))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(SELECT.format(table))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(DELETE_ALL.format(table))
            self._check_last_query_bursted(cluster, cursor)


@pytest.mark.cluster_only
@pytest.mark.custom_burst_gucs(
    gucs=dict(list(burst_write_mv_gucs.items()) + [(
        'burst_percent_threshold_to_trigger_backup',
        '100'), ('burst_cumulative_time_since_stale_backup_threshold_s',
                 '86400'), ('enable_burst_async_acquire', 'false'),
        ('burst_commit_refresh_check_frequency_seconds', '-1')]))
@pytest.mark.usefixtures("setup_teardown_burst")
class TestBurstWriteBasicCluster(TestBurstWriteBasicBase):
    def test_burst_basic_cluster(self, cluster):
        self.base_test_burst_basic(cluster)


@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(
    gucs=list(burst_write_mv_gucs.items()) + [('slices_per_node', '3')])
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteBasicSS(TestBurstWriteBasicBase):
    def test_burst_basic_ss(self, cluster):
        self.base_test_burst_basic(cluster)
