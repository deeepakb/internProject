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

DELETE = "delete from {} where c0 > 5 and c0 < 9"
DELETE_ALL = "delete from {}"
INSERT = "insert into {} values {}"
REFRESH_MV = "refresh materialized view {}"
SELECT = "select * from {} order by 1,2"
UPDATE = "update {} set c0 = c0 + 2 where c0 > 4"
MY_SCHEMA = "test_schema"
MY_USER = "test_user"

diststyle = ['diststyle even', 'distkey(c0)']
sortkey = ['sortkey(c0)', '']


@pytest.mark.serial_only
@pytest.mark.skip_load_data
@pytest.mark.usefixtures("disable_all_autoworkers")
class TestBurstWriteMVSBFlushBase(TestBurstWriteMVBase):

    def _run_dmls(self, cluster, mv_configs, cursors, txn_end):

        main_cur1, main_cur2, check_cur, syscur = cursors
        shared_base_tables = mv_configs.tables
        self._do_sql("copy update select delete insert", True, True,
                     shared_base_tables, cluster, [main_cur1])

        # Begin xact
        main_cur1.execute('BEGIN')
        # DML on all base-tables
        # cursor 1
        self._do_sql("copy update select delete insert", True, True,
                     shared_base_tables, cluster, [main_cur1])
        # Flush SB in the middle of txn
        for i in range(5):
            self.run_directly_on_burst(cluster, "xpx 'hello'")
        # cursor 2
        self._refresh_all_mv(
            # try_burst
            True,
            # should_burst
            True,
            mv_configs,
            cluster,
            [main_cur2, check_cur, syscur],
            BURST_OWN,
            check_data_on_burst=True,
            is_stale=True)

        is_commit = (txn_end == 'commit')
        main_cur1.execute(txn_end)
        # cursor 2
        self._refresh_all_mv(
            # try_burst
            True,
            # should_burst
            is_commit,
            mv_configs,
            cluster,
            [main_cur2, check_cur, syscur],
            BURST_OWN,
            check_data_on_burst=is_commit,
            is_stale=is_commit)

    def base_test_burst_mv_sb_commit(self, cluster):
        """
            This tests checks if a superblock flush in the middle of a
            transaction affects MV refresh on the burst cluster. (X) means
            auto-commit statement. It does the following:

            Session 1               Session 2               Burst-cluster

            (SELECT FROM TBL1..N)
            BEGIN
            DML on TBL1..N
                                                            Flush Superblock
                                    (REFRESH MV)
                                    (SELECT FROM MV)
            COMMIT/ABORT


            The SELECT from session 1 and session 2 must return the same
            contents. The REFRESH and SELECT in session 2 must not be affected
            by transaction in session 1 and xpx in burst-cluster session.
        """
        relprefix = "sb_commit"
        session1 = DbSession(cluster.get_conn_params(),
                             session_ctx=SessionContext(username=MY_USER,
                                                        schema=MY_SCHEMA))
        session2 = DbSession(cluster.get_conn_params(),
                             session_ctx=SessionContext(username=MY_USER,
                                                        schema=MY_SCHEMA))
        session3 = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))

        main_cur1 = session1.cursor()
        main_cur2 = session2.cursor()
        check_cur = session3.cursor()
        syscur = self.db.cursor()
        with self.setup_views_with_shared_tables(
                cluster, main_cur1, relprefix, diststyle, sortkey, MV_QUERIES,
                False) as mv_configs:
            main_cur1.execute("set query_group to burst")
            main_cur2.execute("set query_group to burst")
            check_cur.execute("set query_group to burst")
            syscur.execute("set search_path to {}".format(MY_SCHEMA))
            cursors = [main_cur1, main_cur2, check_cur, syscur]
            self._run_dmls(cluster, mv_configs, cursors, "commit")
            self._run_dmls(cluster, mv_configs, cursors, "abort")


@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=list(burst_write_basic_gucs.items()) +
                               [('slices_per_node', '3')])
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteMVSS(TestBurstWriteMVSBFlushBase):
    def test_burst_mv_sb_commit_ss(self, cluster):
        self.base_test_burst_mv_sb_commit(cluster)
