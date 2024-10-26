# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import logging

from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions
from raff.storage.alter_table_suite import AlterTableSuite
from raff.burst.burst_write import BurstWriteTest
from raff.storage.storage_test import create_thread
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode

logger = logging.getLogger(__name__)
__all__ = ["super_simulated_mode"]

TABLE_DEF = "create table dp15423_target_tbl(c0 int, c1 int) {} {};"

BASIC_INSERT = ("insert into dp15423_target_tbl values"
                "(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7);")

INSERT_SELECT_CMD = ("insert into dp15423_target_tbl "
                     "select * from dp15423_target_tbl;")

BASIC_UPDATE = ("update dp15423_target_tbl set c0 = c0 * 2, c1 = c1 * 3;")

BASIC_DEL = ("DELETE FROM dp15423_target_tbl WHERE c0 = 1;")

STEP_ONE_GUARD = 'alterdiststyle:start_redist_iter'

STEP_TWO_GUARD = 'alterdiststyle:finish_redist_iter'


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs={
    'burst_enable_write': 'true',
    'vacuum_auto_worker_enable': 'false',
    'shadow_table_persist_undo_aborted_txn': "false"
})
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.session_ctx(user_type='bootstrap')
class TestAlterDistSortBGBurstWriteUndo(AlterTableSuite, BurstWriteTest):

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(diststyle=['diststyle even', 'distkey(c1)'],
                 sortkey=['', 'sortkey(c0, c1)']))

    def do_background(self, schema, cmd):
        with self.db.cursor() as cursor:
            cursor.execute(
                'SET SEARCH_PATH TO "{}", "$user", public;'.format(schema))
            cursor.execute(cmd)

    def verify_table_content(self, cursor, res):
        cursor.execute(
            "select count(*), sum(c0), sum(c1) from dp15423_target_tbl;")
        assert cursor.fetchall() == res

    def basic_dml(self, cluster, bursted, cursor, num=1):
        cursor.execute(BASIC_INSERT)
        if bursted:
            self._check_last_query_bursted(cluster, cursor)
        for i in range(num):
            cursor.execute(INSERT_SELECT_CMD)
            if bursted:
                self._check_last_query_bursted(cluster, cursor)
        cursor.execute(BASIC_UPDATE)
        if bursted:
            self._check_last_query_bursted(cluster, cursor)
        cursor.execute(BASIC_DEL)
        if bursted:
            self._check_last_query_bursted(cluster, cursor)

    def setup_table(self, cursor, diststyle, sortkey):
        cursor.execute("begin;")
        cursor.execute(TABLE_DEF.format(diststyle, sortkey))
        self.basic_dml(None, False, cursor, 10)
        cursor.execute("commit;")
        self.verify_table_content(cursor, [(7168, 57344, 86016)])

    def validate_distall_content(self, cursor):
        ctas_stmt = ("create temp table {} as select *, oid as rowid,"
                     "insertxid as insxid, deletexid as delxid "
                     "from dp15423_target_tbl;")
        cursor.execute("set distributed_table_scan_node = 0;")
        cursor.execute(ctas_stmt.format("tt0"))
        cursor.execute("set distributed_table_scan_node = 1;")
        cursor.execute(ctas_stmt.format("tt1"))
        cursor.execute("set distributed_table_scan_node = 2;")
        cursor.execute(ctas_stmt.format("tt2"))
        cursor.execute("select * from tt0 except select * from tt1 union "
                       "select * from tt0 except select * from tt2 union "
                       "select * from tt1 except select * from tt0 union "
                       "select * from tt2 except select * from tt0")
        assert cursor.fetchall() == []

    @pytest.mark.super_simulated_precommit
    def test_alter_distkey_with_bg_undo_burst_write(self, cluster, db_session,
                                                    vector):
        """
        Test: abort burst write on table with concurrent alter distsort.
        1. Run alter distkey and stop at different xen_guard position.
        2. Run concurrent burst write DML and abort the DML during alter distkey
        3. Interate step 1 and 2 multiple times.
        4. Verify table content and properties.
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        with db_session_master.cursor() as cursor, \
                db_session.cursor() as cursor_bs:
            schema = db_session_master.session_ctx.schema
            cursor_bs.execute(
                'SET SEARCH_PATH TO "{}", "$user", public;'.format(schema))
            cursor.execute("set query_group to metrics;")
            self.setup_table(cursor, vector.diststyle, vector.sortkey)
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")
            # Trigger a dummy select to make sure SS mode is up
            # Then the test would fail at this point rather than hung in the
            # next step because of xen_guard.
            cursor.execute("select count(*) from dp15423_target_tbl;")
            self._check_last_query_bursted(cluster, cursor)

            start_redist_guard = self.create_xen_guard(STEP_ONE_GUARD)
            finish_redist_guard = self.create_xen_guard(STEP_TWO_GUARD)

            cmd = "alter table dp15423_target_tbl alter distkey c0;"
            with create_thread(self.do_background, (schema, cmd)) as thread:
                logger.info("Alter distkey round 1 begin")
                with start_redist_guard, finish_redist_guard:
                    start_redist_guard.enable()
                    thread.start()
                    start_redist_guard.wait_until_process_blocks(
                        timeout_secs=self.XEN_GUARD_WAIT_TIMEOUT)
                    finish_redist_guard.enable()
                    start_redist_guard.disable()
                    finish_redist_guard.wait_until_process_blocks(
                        timeout_secs=self.XEN_GUARD_WAIT_TIMEOUT)
                    cursor.execute("begin;")
                    self.basic_dml(cluster, True, cursor, 2)
                    self._check_last_query_bursted(cluster, cursor)
                    cursor.execute("abort")
                    finish_redist_guard.disable()
                logger.info("Alter distkey round 1 done")

            self.get_num_physical_rows(cursor_bs, 'dp15423_target_tbl', 57400)
            self.get_num_distinct_delxids(cursor_bs, 'dp15423_target_tbl', 1)
            self.verify_table_content_and_properties(cursor_bs, schema,
                                                     'dp15423_target_tbl',
                                                     [(7168, 57344, 86016)],
                                                     'distkey(c0)')

            self._start_and_wait_for_refresh(cluster)
            cmd = "alter table dp15423_target_tbl alter distkey c1;"
            with create_thread(self.do_background, (schema, cmd)) as thread:
                logger.info("Alter distkey round 2 begin")
                with start_redist_guard, finish_redist_guard:
                    thread.start()
                    start_redist_guard.wait_until_process_blocks(
                        timeout_secs=self.XEN_GUARD_WAIT_TIMEOUT)
                    finish_redist_guard.enable()
                    start_redist_guard.disable()
                    finish_redist_guard.wait_until_process_blocks(
                        timeout_secs=self.XEN_GUARD_WAIT_TIMEOUT)
                    cursor.execute("begin;")
                    self.basic_dml(cluster, True, cursor)
                    self._check_last_query_bursted(cluster, cursor)
                    cursor.execute("abort")
                    finish_redist_guard.disable()
                logger.info("Alter distkey round 2 done")

            self.get_num_physical_rows(cursor_bs, 'dp15423_target_tbl', 28700)
            self.get_num_distinct_delxids(cursor_bs, 'dp15423_target_tbl', 1)
            self.verify_table_content_and_properties(cursor_bs, schema,
                                                     'dp15423_target_tbl',
                                                     [(7168, 57344, 86016)],
                                                     'distkey(c1)')

            reloid = self.get_reloid(cursor_bs, schema, 'dp15423_target_tbl')
            log_state = [
                ('dp15423_target_tbl', 64568, 57400, 50232, 50232, 1),
                ('dp15423_target_tbl', 78932, 28700, 21532, 21532, 1)
            ]
            self.validate_alter_dist_sort_state(cursor_bs, reloid, log_state)
