# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.burst.burst_write import burst_write_basic_gucs
from raff.burst.burst_write import burst_write_mv_gucs
from raff.burst.burst_test import setup_teardown_burst
from raff.storage.storage_test import disable_all_autoworkers
from test_burst_mv_refresh import TestBurstWriteMVBase
from test_burst_mv_refresh import MV_QUERIES, MAIN_UNDO, BURST_OWN

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst, disable_all_autoworkers]

DELETE = "delete from {} where c0 > 1 and c0 < 9"
DELETE_ALL = "delete from {}"
INSERT = "insert into {} values {}"
REFRESH_MV = "refresh materialized view {0}"
SELECT = "select * from {0} order by 1,2"
UPDATE = "update {} set c0 = c0 + 2 where c0 > 4"
MY_SCHEMA = "test_schema"
MY_USER = "test_user"

diststyle = ['diststyle even', 'distkey(c0)']
sortkey = ['sortkey(c0)', '']


class TestBurstWriteMVParallelTxnBase(TestBurstWriteMVBase):
    def _run_a_dml(self, cluster, mv_config, cursors, dml):
        main_cur = cursors[0]
        tables, _, _, _, _, _, _ = mv_config

        # For each base table of MV, run a dml
        for tbl in tables:
            if dml == "INSERT":
                main_cur.execute(INSERT.format(tbl, self._values(5)))
            elif dml == "UPDATE":
                main_cur.execute(UPDATE.format(tbl))
            elif dml == "DELETE":
                main_cur.execute(DELETE.format(tbl))
            elif dml == "COPY":
                self._do_copy(main_cur, tbl)

            if dml == "COPY":
                self._check_last_copy_bursted(cluster, main_cur)
            else:
                self._check_last_query_bursted(cluster, main_cur)

    def _run_parallel_txns(self, cluster, mv_configs, cursors):
        main_cur1, main_cur2, check_cur, bs_cur = cursors
        # For each MV, run parallel txns
        for mv_config in mv_configs:
            log.info("=== TEST: burst = {}, {}".format(self.run_on_burst,
                                                       mv_config))
            # cursor 1
            main_cur1.execute('BEGIN')

            # cursor 2
            main_cur2.execute('BEGIN')
            self._run_a_dml(cluster, mv_config, [main_cur2], "INSERT")
            self._run_a_dml(cluster, mv_config, [main_cur2], "UPDATE")
            self._run_a_dml(cluster, mv_config, [main_cur2], "COPY")
            # mv_stale_before_refresh = False, check_burst = True
            self._mv_refresh_and_check(cluster, cursors, mv_config, False,
                                       True)
            self._end_txn(cluster, mv_config, [main_cur2], "COMMIT")

            # cursor 1
            self._run_a_dml(cluster, mv_config, [main_cur1], "INSERT")
            self._run_a_dml(cluster, mv_config, [main_cur1], "UPDATE")
            self._run_a_dml(cluster, mv_config, [main_cur1], "COPY")

            # cursor 2
            main_cur2.execute('BEGIN')

            # cursor 1
            # mv_stale_before_refresh = True, check_burst = True
            # mv should be stale on main since cursor 2 committed above
            self._mv_refresh_and_check(cluster, cursors, mv_config, True, True)
            self._end_txn(cluster, mv_config, [main_cur1], "COMMIT")
            # mv_stale_before_refresh = True, check_burst = True
            self._mv_refresh_and_check(cluster, cursors, mv_config, True, True)

            # cursor 2
            self._run_a_dml(cluster, mv_config, [main_cur2], "INSERT")
            self._run_a_dml(cluster, mv_config, [main_cur2], "UPDATE")
            self._run_a_dml(cluster, mv_config, [main_cur2], "COPY")
            self._run_a_dml(cluster, mv_config, [main_cur2], "DELETE")
            # mv_stale_before_refresh = False, check_burst = True
            self._mv_refresh_and_check(cluster, cursors, mv_config, False,
                                       True)
            self._end_txn(cluster, mv_config, [main_cur2], "ABORT")
            # mv_stale_before_refresh = False, check_burst = False
            # The base-tables of MV are in main-undo state after abort. So we
            # cannot check burst cluster because we cannot burst any queries on
            # them.
            self._mv_refresh_and_check(cluster, cursors, mv_config, False,
                                       False)

    def _mv_refresh_and_check(self, cluster, cursors, mv_config,
                              mv_stale_before, check_burst):
        """
            This method refreshes the MV and checks that data is same across
            both main and burst clusters.
            If check_burst = True, then it checks that MV and its tables are
            same on both clusters. We can do this if MV and base tables are
            in burst-owned state.
            If check_burst = False, then it checks MV and its base tables are
            same only on main clusters. We need this when either MV or one of
            its base tables is not burst-owned.
        """
        main_cur1, main_cur2, check_cur, bs_cur = cursors
        base_tables, mv, mv_query, _, _, _, _ = mv_config
        self._check_mv_stale_on_main(cluster, check_cur, mv, mv_stale_before)
        check_cur.execute(REFRESH_MV.format(mv))
        # SIM: https://issues.amazon.com/RedshiftDP-32343
        # This check is flaky. The MV can be falsely marked stale even
        # though its not because of inflight transactions on other random
        # tables. We check for data equality below anyways.
        # self._check_mv_stale_on_main(cluster, check_cur, mv, False)
        if check_burst:
            self._check_last_mv_refresh_bursted(cluster, bs_cur, mv)
        else:
            self._check_last_mv_refresh_didnt_burst(cluster, bs_cur, mv)
        self._check_data_is_same(cluster,
                                 check_cur,
                                 base_tables,
                                 mv,
                                 mv_query,
                                 check_burst=check_burst)

    def _end_txn(self, cluster, mv_config, cursors, txn_end):
        main_cur = cursors[0]
        base_tables, mv, _, _, _, _, _ = mv_config
        mv_internal_table = "mv_tbl__{}__0".format(mv)
        if txn_end == "ABORT":
            main_cur.execute("ABORT;")
            self._verify_tables(cluster, MY_SCHEMA,
                                base_tables + [mv_internal_table],
                                mv_config.t_dist)
            self._verify_owner(cluster, MY_SCHEMA, base_tables, MAIN_UNDO)
        else:
            main_cur.execute("COMMIT")
            self._verify_tables(cluster, MY_SCHEMA,
                                base_tables + [mv_internal_table],
                                mv_config.t_dist)
            self._verify_owner(cluster, MY_SCHEMA, base_tables, BURST_OWN)

    def base_test_burst_mv_parallel_txn(self, cluster):
        """
            This test ensures that REFRESH MV bursts correctly when there are
            two in-flight transactions on a table.

            Main_cur1               Main_cur2               check_cur
            BEGIN                   BEGIN
                                    INSERT/UPDATE/COPY      REFRESH_MV
                                    COMMIT                  REFRESH_MV
            INSERT/UPDATE/COPY      BEGIN                   REFRESH_MV
            COMMIT                                          REFRESH_MV
                                    INSERT/UPDATE/COPY
                                    DELETE                  REFRESH_MV
                                    ABORT                   REFRESH_MV
            Bug(s) found:
            - https://sim.amazon.com/issues/RedshiftDP-30070
              This file has been renamed & the code reworked for clarity since
              the time SIM was filed but the test case has been covered here.
        """
        relprefix = "parallel_txn"
        session1 = DbSession(cluster.get_conn_params(),
                             session_ctx=SessionContext(username=MY_USER,
                                                        schema=MY_SCHEMA))
        session2 = DbSession(cluster.get_conn_params(),
                             session_ctx=SessionContext(username=MY_USER,
                                                        schema=MY_SCHEMA))
        session3 = DbSession(cluster.get_conn_params(),
                             session_ctx=SessionContext(username=MY_USER,
                                                        schema=MY_SCHEMA))
        main_cur1 = session1.cursor()
        main_cur2 = session2.cursor()
        check_cur = session3.cursor()
        bs_cur = self.db.cursor() 
        with self.setup_mv(cluster, check_cur, relprefix, diststyle, sortkey,
                           MV_QUERIES[:1], False) as mv_configs:
            # Note that we don't need to examine all MV_QUERIES in this test;
            # one MV query is sufficient to validate MV refresh in the presence
            # of inflight txns touching the base table.
            main_cur1.execute("set query_group to burst")
            main_cur2.execute("set query_group to burst")
            check_cur.execute("set query_group to burst")
            bs_cur.execute("set search_path to {}".format(MY_SCHEMA))

            self._run_parallel_txns(cluster, mv_configs,
                                    [main_cur1, main_cur2, check_cur, bs_cur])


@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=list(burst_write_basic_gucs.items()) +
                               [('slices_per_node', '3')])
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteMVParallelTxnSS(TestBurstWriteMVParallelTxnBase):
    def test_burst_mv_parallel_txn_ss(self, cluster):
        self.base_test_burst_mv_parallel_txn(cluster)
