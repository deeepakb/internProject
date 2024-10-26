# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from py_lib.common.xen_guard_control import XenGuardControl
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions
from raff.burst.burst_write import BurstWriteTest
from raff.storage.alter_table_suite import AlterTableSuite
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.storage.storage_test import create_thread

__all__ = ["super_simulated_mode"]

TABLE_DEF = "create table dp15251_target_tbl(c0 int, c1 int) {} {}"

BASIC_INSERT = ("insert into dp15251_target_tbl values "
                "(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7);")

BASIC_UPDATE = ("UPDATE dp15251_target_tbl values SET c1 = 3"
                "WHERE c1 = 2;")

BASIC_DEL = ("DELETE FROM dp15251_target_tbl WHERE c0 = 1;")

INSERT_SELECT_CMD = ("insert into dp15251_target_tbl "
                     "select * from dp15251_target_tbl;")

log = logging.getLogger(__name__)


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
class TestBurstWriteBGAlterDistSort(BurstWriteTest, AlterTableSuite):

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(diststyle=['diststyle even', 'distkey(c1)'],
                 guard_pos=[
                     'alterdiststyle:start_redist_data',
                     'alterdiststyle:finish_redist_iter',
                     'alterdiststyle:finish_read_redist'
                 ],
                 sortkey=['compound sortkey(c0, c1)']))

    def do_background(self, schema, cmd):
        with self.db.cursor() as cursor:
            cursor.execute(
                'SET SEARCH_PATH TO "{}", "$user", public;'.format(schema))
            cursor.execute(cmd)

    def verify_table_content(self, cursor, res):
        cmd = "select count(*), sum(c0), sum(c1) from dp15251_target_tbl;"
        cursor.execute(cmd)
        assert cursor.fetchall() == res

    def test_alter_distkey_with_bg_burst_write(self, cluster, db_session,
                                               vector):
        """
        Test: burst write on table with concurrent alter distsort.
        Burst should fail to be run on burst cluster.
        1. Run alter distkey and stop at different xen_guard position.
        2. Run concurrent burst write DML during alter distkey.
        3. Interate step 1 and 2 multiple times.
        4. Verify table content and properties.
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        with db_session_master.cursor() as cursor, \
                db_session.cursor() as cursor_bootstrap:
            schema = db_session_master.session_ctx.schema
            cursor_bootstrap.execute(
                'SET SEARCH_PATH TO "{}", "$user", public;'.format(schema))
            cursor.execute(TABLE_DEF.format(vector.diststyle, vector.sortkey))
            cursor.execute(BASIC_INSERT)
            for i in range(5):
                cursor.execute(INSERT_SELECT_CMD)
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")

            xen_guard = XenGuardControl(host=None,
                                        username=None,
                                        guardname=vector.guard_pos,
                                        debug=True,
                                        remote=False)
            log.info("Concurrent alter, burst write Round 1")
            # round one background insert
            cmd = "alter table dp15251_target_tbl alter distkey c0;"
            with create_thread(self.do_background, (schema, cmd)) as thread:
                with xen_guard:
                    thread.start()
                    xen_guard.wait_until_process_blocks(
                        timeout_secs=self.XEN_GUARD_WAIT_TIMEOUT)
                    cursor.execute(INSERT_SELECT_CMD)
                    self._check_last_query_bursted(cluster, cursor)
                    xen_guard.disable()

            # data validation
            self.verify_table_content_and_properties(cursor_bootstrap, schema,
                                                     'dp15251_target_tbl',
                                                     [(448, 1792, 1792)],
                                                     'distkey(c0)')
            self._start_and_wait_for_refresh(cluster)

            log.info("Concurrent alter, burst write Round 2")
            # round two background insert
            cmd = "alter table dp15251_target_tbl alter distkey c1;"
            with create_thread(self.do_background, (schema, cmd)) as thread:
                with xen_guard:
                    thread.start()
                    xen_guard.wait_until_process_blocks(
                        timeout_secs=self.XEN_GUARD_WAIT_TIMEOUT)
                    for i in range(10):
                        cursor.execute(INSERT_SELECT_CMD)
                        self._check_last_query_bursted(cluster, cursor)
                        cursor.execute(BASIC_UPDATE)
                        self._check_last_query_bursted(cluster, cursor)
                        cursor.execute(BASIC_DEL)
                        self._check_last_query_bursted(cluster, cursor)
                    xen_guard.disable()

            # data validation
            self.verify_table_content_and_properties(
                cursor_bootstrap, schema, 'dp15251_target_tbl',
                [(393216, 1769472, 1835008)], 'distkey(c1)')
            self._start_and_wait_for_refresh(cluster)

            cursor.execute(INSERT_SELECT_CMD)
            cursor.execute(INSERT_SELECT_CMD)

            expected_state = [('dp15251_target_tbl', 448, 448, 0, 0, 0),
                              ('dp15251_target_tbl', 393472, 393472, 256,
                               0, 0)]
            reloid = self.get_reloid(cursor_bootstrap, schema,
                                     'dp15251_target_tbl')
            self.validate_alter_dist_sort_state(cursor_bootstrap, reloid,
                                                expected_state)
            self.verify_table_content_and_properties(
                cursor_bootstrap, schema, 'dp15251_target_tbl',
                [(1572864, 7077888, 7340032)], 'distkey(c1)')
