# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest

from py_lib.common.xen_guard_control import XenGuardControl
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions
from raff.burst.burst_write import BurstWriteTest
from raff.storage.alter_table_suite import AlterTableSuite
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.storage.storage_test import create_thread

__all__ = ["super_simulated_mode"]

INSERT_SELECT = "insert into dp20365_tbl select * from dp20365_tbl;"


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.no_jdbc
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs={
    'burst_enable_write': 'true',
    'vacuum_auto_worker_enable': 'false'
})
@pytest.mark.session_ctx(user_type='bootstrap')
@pytest.mark.usefixtures("super_simulated_mode")
class TestAlterColumnEncodeBgBurstWrite(BurstWriteTest, AlterTableSuite):

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(diststyle=['diststyle even', 'distkey(c1)'],
                 guard_pos=[
                     'altercolencode:start_col_copy',
                     'altercolencode:finish_copy_phase1'
                 ],
                 sortkey=['', 'sortkey(c0)', 'compound sortkey(c0, c1)']))

    def _do_background(self, schema, cmd):
        with self.db.cursor() as cursor:
            cursor.execute(
                'SET SEARCH_PATH TO "{}", "$user", public;'.format(schema))
            cursor.execute(cmd)

    def verify_table_content(self, cursor, res):
        cmd = "select count(*), sum(c0), sum(c1) from dp20365_tbl;"
        cursor.execute(cmd)
        assert cursor.fetchall() == res

    def _setup_table(self, cursor, diststyle, sortkey):
        tbl_def = "create table dp20365_tbl(c0 int, c1 int) {} {}"
        basic_insert = ("insert into dp20365_tbl values "
                        "(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7);")
        cursor.execute("begin")
        cursor.execute(tbl_def.format(diststyle, sortkey))
        cursor.execute(basic_insert)
        for i in range(10):
            cursor.execute(INSERT_SELECT)
        cursor.execute("commit")

    def test_ace_with_bg_burst_write(self, cluster, db_session, vector):
        """
        Test: burst write on table with concurrent alter encode.
        Burst write should fail to be run on burst cluster.
        1. Run alter encode and stop at different xen_guard position.
        2. Run concurrent burst write DML during alter encode.
        3. Interate step 1 and 2 multiple times.
        4. Verify table content and properties.
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        with db_session_master.cursor() as cursor, \
                db_session.cursor() as cursor_bootstrap:
            schema = db_session_master.session_ctx.schema
            cursor_bootstrap.execute(
                'SET SEARCH_PATH TO "{}", "$user", public;'.format(schema))
            cursor.execute("set query_group to metrics;")
            self._setup_table(cursor, vector.diststyle, vector.sortkey)
            xen_guard = XenGuardControl(host=None,
                                        username=None,
                                        guardname=vector.guard_pos,
                                        debug=True,
                                        remote=False)
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")

            # round one background insert
            cmd = "alter table dp20365_tbl alter column c1 encode ZSTD;"
            with create_thread(self._do_background, (schema, cmd)) as thread, \
                    xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks(
                    timeout_secs=self.XEN_GUARD_WAIT_TIMEOUT)
                cursor.execute(INSERT_SELECT)
                self._check_last_query_bursted(cluster, cursor)
            # data validation
            self.verify_table_content_and_properties(cursor_bootstrap, schema,
                                                     'dp20365_tbl',
                                                     [(14336, 57344, 57344)],
                                                     vector.diststyle)
            cursor.execute(
                "alter table dp20365_tbl alter column c1 encode LZO;")

            # round two background insert
            self._start_and_wait_for_refresh(cluster)
            with create_thread(self._do_background, (schema, cmd)) as thread, \
                    xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks(
                    timeout_secs=self.XEN_GUARD_WAIT_TIMEOUT)
                cursor.execute(INSERT_SELECT)
                self._check_last_query_bursted(cluster, cursor)
            # data validation
            self.verify_table_content_and_properties(
                cursor_bootstrap, schema, 'dp20365_tbl',
                [(28672, 114688, 114688)], vector.diststyle)

            self._start_and_wait_for_refresh(cluster)
            cursor.execute(INSERT_SELECT)
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(INSERT_SELECT)
            self._check_last_query_bursted(cluster, cursor)
            self.verify_table_content_and_properties(
                cursor_bootstrap, schema, 'dp20365_tbl',
                [(114688, 458752, 458752)], vector.diststyle)
