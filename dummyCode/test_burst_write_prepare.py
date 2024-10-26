# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import logging

from raff.common.db.session import DbSession
from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import burst_write_basic_gucs
from raff.burst.burst_test import setup_teardown_burst
from raff.common.host_type import HostType

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst]

DROP_TABLE = "DROP TABLE IF EXISTS public.{}{}{}"
CREATE_STMT = "CREATE TABLE public.{}{}{} (c0 int, c1 int) {} {}"
GRANT_STMT = "GRANT ALL ON public.{}{}{} TO PUBLIC;"
INSERT_SELECT_CMD = "INSERT INTO {} SELECT * FROM {}"
INSERT_CMD = "INSERT INTO {} values (1,2), (3, 4), (5, 6), (7, 8), (9, 10)"
DELETE_CMD = "delete from {} where c0 < 3"
UPDATE_CMD_1 = "update {} set c0 = c0*2 where c0 > 5"
UPDATE_CMD_2 = "update {} set c0 = c0 + 2"


class TestBurstWritePrepareBase(BurstWriteTest):
    def _setup_tables(self, cluster, cursor, base_tbl_name):
        diststyles=['diststyle even', 'distkey(c0)']
        sortkeys=['', 'sortkey(c0)']
        tbl_index = 0
        for diststyle in diststyles:
            for sortkey in sortkeys:
                self._run_bootstrap_sql(cluster,
                                        DROP_TABLE.format(
                                            base_tbl_name, "_not_burst_",
                                            tbl_index))
                self._run_bootstrap_sql(cluster,
                                        DROP_TABLE.format(
                                            base_tbl_name, "_burst_",
                                            tbl_index))
                cursor.execute(CREATE_STMT.format(
                    base_tbl_name, "_not_burst_", tbl_index, diststyle,
                    sortkey))
                cursor.execute(CREATE_STMT.format(
                    base_tbl_name, "_burst_", tbl_index, diststyle, sortkey))
                cursor.execute(GRANT_STMT.format(
                    base_tbl_name, "_not_burst_", tbl_index))
                cursor.execute(GRANT_STMT.format(
                    base_tbl_name, "_burst_", tbl_index))
                tbl_index = tbl_index + 1
        return tbl_index

    def run_query_under_prepare_stmt(self, cluster, cursor, query, is_burst):
        if is_burst:
            cursor.execute("set query_group to burst;")
        else:
            cursor.execute("set query_group to metrics;")
        cursor.execute("PREPARE tmp_test_plan AS {};".format(query))
        cursor.execute("EXECUTE tmp_test_plan;")
        if is_burst:
            self._check_last_query_bursted(cluster, cursor)
        else:
            self._check_last_query_didnt_burst(cluster, cursor)
        cursor.execute("DEALLOCATE tmp_test_plan;")

    def _insert_tables(self, cluster, cursor, base_tbl_name, tbl_index):
        # Insert when set not bursted.
        query = INSERT_CMD.format(
            base_tbl_name + "_not_burst_" + str(tbl_index))
        self.run_query_under_prepare_stmt(cluster, cursor, query, False)
        # Insert when set bursted.
        query = INSERT_CMD.format(base_tbl_name + "_burst_" + str(tbl_index))
        self.run_query_under_prepare_stmt(cluster, cursor, query, True)

    def _insert_select_tables(self, cluster, cursor, base_tbl_name, tbl_index,
                              num=1):
        for i in range(num):
            tbl_name = base_tbl_name + "_not_burst_" + str(tbl_index)
            # Insert when set not bursted.
            query = INSERT_SELECT_CMD.format(tbl_name, tbl_name)
            self.run_query_under_prepare_stmt(cluster, cursor, query, False)
            # Insert when set bursted.
            tbl_name = base_tbl_name + "_burst_" + str(tbl_index)
            query = INSERT_SELECT_CMD.format(tbl_name, tbl_name)
            self.run_query_under_prepare_stmt(cluster, cursor, query, True)

    def _delete_tables(self, cluster, cursor, base_tbl_name, tbl_index):
        # Delete when set not bursted.
        query = DELETE_CMD.format(
            base_tbl_name + "_not_burst_" + str(tbl_index))
        self.run_query_under_prepare_stmt(cluster, cursor, query, False)
        # Delete when set bursted.
        query = DELETE_CMD.format(base_tbl_name + "_burst_" + str(tbl_index))
        self.run_query_under_prepare_stmt(cluster, cursor, query, True)

    def _update_tables_1(self, cluster, cursor, base_tbl_name, tbl_index):
        # Update when set not bursted.
        query = UPDATE_CMD_1.format(
            base_tbl_name + "_not_burst_" + str(tbl_index))
        self.run_query_under_prepare_stmt(cluster, cursor, query, False)
        # Update when set bursted.
        query = UPDATE_CMD_1.format(base_tbl_name + "_burst_" + str(tbl_index))
        self.run_query_under_prepare_stmt(cluster, cursor, query, True)

    def _update_tables_2(self, cluster, cursor, base_tbl_name, tbl_index):
        # Update when set not bursted.
        query = UPDATE_CMD_2.format(
            base_tbl_name + "_not_burst_" + str(tbl_index))
        self.run_query_under_prepare_stmt(cluster, cursor, query, False)
        # Update when set bursted.
        query = UPDATE_CMD_2.format(base_tbl_name + "_burst_" + str(tbl_index))
        self.run_query_under_prepare_stmt(cluster, cursor, query, True)

    def base_test_burst_write_prepare_stmt(self, cluster, txn_mode):
        """
        Test: test prepare statement can be burst write correctly.
        The test includes:
        1. Run insert/delete/update within a prepare statement on the burst
            culster.
        2. Validate tables content at last.
        """
        cluster.run_xpx('auto_worker disable both')
        base_tbl_name = "BW_prepare"
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        with db_session_master.cursor() as cursor:
            num_tbls = self._setup_tables(cluster, cursor, base_tbl_name)
            self._start_and_wait_for_refresh(cluster)
            if txn_mode == "commit" or "abort":
                cursor.execute("BEGIN;")
            for tbl_index in range(num_tbls):
                self._insert_tables(cluster, cursor, base_tbl_name, tbl_index)
                self._insert_select_tables(cluster, cursor, base_tbl_name,
                                           tbl_index, 10)
                self._delete_tables(cluster, cursor, base_tbl_name, tbl_index)
                self._update_tables_1(
                    cluster, cursor, base_tbl_name, tbl_index)
                self._update_tables_2(
                    cluster, cursor, base_tbl_name, tbl_index)
            if txn_mode == "abort":
                cursor.execute("ABORT;")
                self._start_and_wait_for_refresh(cluster)
            else:
                cursor.execute("COMMIT;")
            for tbl_index in range(num_tbls):
                tbl_gold = base_tbl_name + "_not_burst_" + str(tbl_index)
                tbl_bursted = base_tbl_name + "_burst_" + str(tbl_index)
                self._validate_content_equivalence(cluster, cursor, tbl_gold,
                                                   tbl_bursted)


@pytest.mark.serial_only
@pytest.mark.custom_burst_gucs(gucs=burst_write_basic_gucs)
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.cluster_only
class TestBurstWritePrepareCluster(TestBurstWritePrepareBase):
    def test_burst_write_prepare_stmt_cluster(self, cluster):
        self.base_test_burst_write_prepare_stmt(cluster, "no_txn")
        self.base_test_burst_write_prepare_stmt(cluster, "abort")
        self.base_test_burst_write_prepare_stmt(cluster, "commit")


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs={'burst_enable_write': 'true'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWritePrepareSS(TestBurstWritePrepareBase):
    def test_burst_write_prepare_stmt_ss(self, cluster):
        self.base_test_burst_write_prepare_stmt(cluster, "no_txn")
        self.base_test_burst_write_prepare_stmt(cluster, "abort")
        self.base_test_burst_write_prepare_stmt(cluster, "commit")
