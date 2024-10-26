# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
import logging
import pytest

from itertools import product
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_test import setup_teardown_burst
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


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.load_tpcds("catalog_sales")
@pytest.mark.super_simulated_mode
@pytest.mark.encrypted_only
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_local_gucs(
    gucs={
        'selective_dispatch_level': '0',
        'multi_az_enabled': 'true',
        'is_multi_az_primary': 'true'
    })
@pytest.mark.custom_burst_gucs(
    gucs={
        'multi_az_enabled': 'true',
        'is_multi_az_secondary': 'true',
        'burst_enable_write': 'true',
    })
class TestMultiAzSsWriteQuery(TestBurstWriteMVBase):
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

    def _setup_multi_az(self, cluster, cursor, table):
        # Burst a query to ensure we have a cluster
        cursor.execute("set session authorization '{}'".format(MY_USER))
        cursor.execute("set enable_result_cache_for_session to off")
        cursor.execute("set query_group to burst")
        cursor.execute(INSERT.format(table, self._values(1)))
        self._check_last_query_bursted(cluster, cursor)

        # Modify the burst cluster to be a multiAZ cluster instead
        with self.db.cursor() as boot_cur:
            boot_cur.execute(
                "select cluster_arn from stv_burst_manager_cluster_info "
                "limit 1")
            rows = boot_cur.fetchall()
            if (len(rows) == 0):
                assert False, "No entries in stv_burst_manager_cluster_info"
            arn = rows[0][0]
            cluster.run_xpx("update_burst_cluster {} MultiAZ true".format(arn))

    def test_multi_az_ss_write_query(self, cluster, db_session):
        """
        This test verifies that insert, update, delete
        can run on multiAZ secondary clusters
        """
        session = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))
        cursor = session.cursor()
        tables = self._setup_source_tables(cluster, cursor)
        self._setup_multi_az(cluster, cursor, tables[0])
        for table in tables:
            cursor.execute("set session authorization '{}'".format(MY_USER))
            cursor.execute("set enable_result_cache_for_session to off")
            cursor.execute("set query_group to burst")

            cursor.execute(INSERT.format(table, self._values(1)))
            self.check_last_query_ran_on_multi_az(cluster, cursor)
            cursor.execute(UPDATE.format(table))
            self.check_last_query_ran_on_multi_az(cluster, cursor)
            cursor.execute(SELECT.format(table))
            self.check_last_query_ran_on_multi_az(cluster, cursor)
            cursor.execute(DELETE_ALL.format(table))
            self.check_last_query_ran_on_multi_az(cluster, cursor)
