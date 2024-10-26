# Copyright 2022 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from itertools import product
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_test import setup_teardown_burst
from raff.burst.burst_write import burst_write_basic_gucs
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.storage.storage_test import disable_all_autoworkers
from test_burst_mv_refresh import TestBurstWriteMVBase
from raff.storage.megablock_suite import \
    (add_col_to_existed_tbl, has_megavalue_in_column, MegablockSuite)

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
CTAS = ("CREATE TABLE {} AS SELECT * FROM {}")
CTAS_EMPTY = ("CREATE TABLE {} AS SELECT * FROM {} WHERE 2=1")
DELETE_ALL = "delete from {}"
DROP_TABLE = "DROP TABLE IF EXISTS {} CASCADE"
GET_TABLE_ID = "SELECT '{}'::regclass::oid"
INSERT = ("INSERT INTO {} VALUES {}")
MY_SCHEMA = "test_schema"
MY_TABLE = "test_table"
MY_USER = "test_user"
SELECT = "select * from {0} order by 1,2"
UPDATE = "update {} set c0 = c0 + 2 where c0 > 4"

diststyle = ['diststyle even', "distkey(c0)"]
sortkey = ['sortkey(c0)', '']


class TestBurstWriteCTASBase(TestBurstWriteMVBase, MegablockSuite):
    def _setup_source_tables(self,
                             cluster,
                             cursor,
                             bootstrap_cursor,
                             has_megavalues=False):
        cursor.execute("set session authorization '{}'".format(MY_USER))
        tables = []
        for tbl_id, prop in enumerate(product(diststyle, sortkey)):
            t_dist, t_sort = prop
            table = "_".join([MY_TABLE, str(tbl_id)])
            cursor.execute(DROP_TABLE.format(table))
            cursor.execute(CREATE_TABLE.format(table, t_dist, t_sort))
            cursor.execute(INSERT.format(table, self._values(10)))
            if has_megavalues:
                add_col_to_existed_tbl(cursor, table, "c0 <= 1", "c0 > 1")
                has_megavalue_in_column(bootstrap_cursor, table, 7)
            tables.append(table)
        self._start_and_wait_for_refresh(cluster)
        return tables

    def _get_query_id(self, cluster, cursor, query):
        # Exact str match is faster than regex
        query = ("select query from stl_query"
                 " where querytxt ilike '%{}%'"
                 " order by 1 desc limit 1").format(query)
        cursor.execute(query)
        return cursor.fetch_scalar()

    def _check_last_query_didnt_burst(self,
                                      cluster,
                                      cursor,
                                      query,
                                      status=None):
        qid = self._get_query_id(cluster, cursor, query)
        errcode = self._check_query_didnt_burst(cluster, qid)
        if status:
            assert errcode == status, \
                            ("Expected cs_status = {}"
                             " but found {}").format(status, errcode)
        return errcode

    def base_test_burst_ctas(self, cluster, has_megavalues=False):
        session = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))
        cursor = session.cursor()
        bootstrap_cursor = self.db.cursor()
        for source_table in self._setup_source_tables(
                cluster, cursor, bootstrap_cursor, has_megavalues):
            cursor.execute("set session authorization '{}'".format(MY_USER))
            cursor.execute("set enable_result_cache_for_session to off")
            cursor.execute("set query_group to burst")
            ctas_table = "ctas_" + source_table
            cursor.execute(DROP_TABLE.format(ctas_table))
            ctas_query = CTAS.format(ctas_table, source_table)
            cursor.execute(ctas_query)
            self._check_last_query_didnt_burst(
                cluster, cursor, ctas_query, status=67)


@pytest.mark.cluster_only
@pytest.mark.custom_burst_gucs(
    gucs=dict(
        list(burst_write_basic_gucs.items()) + [(
            'burst_enable_write_user_ctas',
            'false'), ('burst_percent_threshold_to_trigger_backup', '100'), (
                'burst_cumulative_time_since_stale_backup_threshold_s',
                '86400'), ('enable_burst_async_acquire', 'false'), (
                    'burst_commit_refresh_check_frequency_seconds', '-1')]))
@pytest.mark.usefixtures("setup_teardown_burst")
class TestBurstWriteCTASCluster(TestBurstWriteCTASBase):
    def test_burst_ctas_cluster(self, cluster):
        self.base_test_burst_ctas(cluster)


@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=list(burst_write_basic_gucs.items()))
@pytest.mark.custom_local_gucs(gucs={'burst_enable_write_user_ctas': 'false'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteCTASSS(TestBurstWriteCTASBase):
    def test_burst_ctas_ss(self, cluster):
        self.base_test_burst_ctas(cluster)


@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=list(burst_write_basic_gucs.items()))
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteEmptyCTAS(TestBurstWriteCTASBase):
    def test_burst_empty_ctas(self, cluster):
        # DP-49002: Verify empty CTAS table inside a txn block won't
        # burst following queries within the same txn block
        session = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))
        cursor = session.cursor()
        bootstrap_cursor = self.db.cursor()
        for source_table in self._setup_source_tables(
                cluster, cursor, bootstrap_cursor):
            cursor.execute("set session authorization '{}'".format(MY_USER))
            cursor.execute("set enable_result_cache_for_session to off")
            ctas_table = "ctas_" + source_table
            cursor.execute(DROP_TABLE.format(ctas_table))
            cursor.execute("set query_group to burst")
            cursor.execute("BEGIN")
            # CTAS an empty table
            ctas_query = CTAS_EMPTY.format(ctas_table, source_table)
            cursor.execute(ctas_query)
            select_query = SELECT.format(ctas_table)
            cursor.execute(select_query)
            self._check_last_query_didnt_burst(cluster, cursor, select_query)
            cursor.execute("ABORT;")
            cursor.execute("set query_group to burst")
            cursor.execute("BEGIN")
            # CTAS an empty table
            ctas_query = CTAS_EMPTY.format(ctas_table, source_table)
            cursor.execute(ctas_query)
            cursor.execute(INSERT.format(ctas_table, self._values(10)))
            self._check_last_query_didnt_burst(
                cluster, cursor, "INSERT INTO {}".format(ctas_table))
            cursor.execute("COMMIT")
            # Refresh and verify
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst")
            cursor.execute("SELECT count(*) FROM {}".format(ctas_table))
            res = int(cursor.fetch_scalar())
            self._check_last_query_bursted(cluster, cursor)
            assert res == 10
