# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest

from py_lib.common.xen_guard_control import XenGuardControl
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions
from raff.common.cluster.cluster_session import ClusterSession
from raff.storage.alter_table_suite import AlterTableSuite, ENCODE_VALUE
from raff.burst.burst_write import BurstWriteTest
from raff.storage.storage_test import create_thread
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode

INSERT_SELECT = "insert into dp20365_tbl select * from dp20365_tbl;"
DELETE_STMT = "delete from dp20365_tbl where c0 < 3"
UPDATE_STMT = "update dp20365_tbl set c0 = c0 + 1, c1 = c1 + 1;"
S3_PATH = 's3://cookie-monster-s3-ingestion/schema-quota/2-cols-21-rows.csv'
COPY_STMT = ("COPY dp20365_tbl "
             "FROM "
             "'{}' "
             "DELIMITER ',' "
             "CREDENTIALS "
             "'aws_iam_role=arn:aws:iam::467896856988:role/Redshift-S3';")


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.no_jdbc
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs={
    'burst_enable_write': 'true',
    'vacuum_auto_worker_enable': 'false',
    'max_num_auto_undo_workers': '0'
})
@pytest.mark.session_ctx(user_type='bootstrap')
@pytest.mark.usefixtures("super_simulated_mode")
class TestAlterColumnEncodeBurstWriteUndo(BurstWriteTest, AlterTableSuite):

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(diststyle=['diststyle even', 'distkey(c1)'],
                 guard_pos=[
                     'altercolencode:start_col_copy',
                     'altercolencode:finish_copy_phase1'
                 ],
                 sortkey=['', 'compound sortkey(c0, c1)'],
                 cases=[
                     {
                         "bg_cmd":
                         INSERT_SELECT,
                         "results": [[
                             (7168, 28672, 28672),
                         ], [(14336, 57344, 57344)], [(14336, 57344, 57344)],
                                     [(57344, 229376, 229376)]]
                     },
                     {
                         "bg_cmd":
                         DELETE_STMT,
                         "results": [[(7168, 28672, 28672)],
                                     [(5120, 25600, 25600)],
                                     [(5120, 25600, 25600)],
                                     [(20480, 102400, 102400)]]
                     },
                     {
                         "bg_cmd":
                         UPDATE_STMT,
                         "results": [[(7168, 28672, 28672)],
                                     [(7168, 35840, 35840)],
                                     [(7168, 35840, 35840)],
                                     [(28672, 143360, 143360)]]
                     },
                     {
                         "bg_cmd":
                         COPY_STMT.format(S3_PATH),
                         "results": [[(7168, 28672, 28672)],
                                     [(7189, 28714, 28714)],
                                     [(7189, 28714, 28714)],
                                     [(28756, 114856, 114856)]]
                     },
                 ]))

    def _run_cmd_on_schema(self, schema, cmd):
        with self.db.cursor() as cursor:
            cursor.execute(
                'SET SEARCH_PATH TO "{}", "$user", public;'.format(schema))
            cursor.execute(cmd)

    def verify_table_content(self, cursor, res):
        cmd = "select count(*), sum(c0), sum(c1) from dp20365_tbl;"
        cursor.execute(cmd)
        assert cursor.fetchall() == res

    def _setup_table(self, cursor, diststyle, sortkey):
        tbl_def = ("create table dp20365_tbl"
                   "(c0 int encode lzo, c1 int encode raw) {} {}")
        basic_insert = ("insert into dp20365_tbl values "
                        "(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7);")
        cursor.execute("begin")
        cursor.execute(tbl_def.format(diststyle, sortkey))
        cursor.execute(basic_insert)
        for i in range(10):
            cursor.execute(INSERT_SELECT)
        cursor.execute("commit")

    def test_alter_encode_with_bg_undo_burst_write(self, cluster, db_session,
                                                   vector):
        """
        Test: abort burst write on table with concurrent alter encode.
        1. Run alter encode and stop at different xen_guard position.
        2. Run concurrent burst write DML and abort the DML during alter encode.
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
            self._setup_table(cursor, vector.diststyle, vector.sortkey)
            xen_guard = XenGuardControl(host=None,
                                        username=None,
                                        guardname=vector.guard_pos,
                                        debug=True,
                                        remote=False)
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")

            # round one background DML
            cmd = "alter table dp20365_tbl alter column c1 encode az64;"
            bg_dml = vector.cases["bg_cmd"]
            res0 = vector.cases["results"][0]
            res1 = vector.cases["results"][1]
            res2 = vector.cases["results"][2]
            res3 = vector.cases["results"][3]
            with create_thread(self._run_cmd_on_schema, (schema, cmd)) as \
                    thread, xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks(
                    timeout_secs=self.XEN_GUARD_WAIT_TIMEOUT)
                cursor.execute("begin;")
                cursor.execute(bg_dml)
                self._check_last_query_bursted(cluster, cursor)
                cursor.execute("abort;")
            # data validation
            self.verify_table_content_and_properties(cursor_bs, schema,
                                                     'dp20365_tbl', res0,
                                                     vector.diststyle)
            self.validate_column_encode(cursor_bs, "dp20365_tbl", "c1",
                                        ENCODE_VALUE["AZ64"])
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(bg_dml)
            cursor_bs.execute(
                "alter table dp20365_tbl alter column c1 encode LZO;")
            self.validate_column_encode(cursor_bs, "dp20365_tbl", "c1",
                                        ENCODE_VALUE["LZO"])
            self.verify_table_content_and_properties(cursor_bs, schema,
                                                     'dp20365_tbl', res1,
                                                     vector.diststyle)

            # round two background DML
            self._start_and_wait_for_refresh(cluster)
            if bg_dml.find("delete") != -1:
                bg_dml = "delete from dp20365_tbl where c0 < 4"
            with create_thread(self._run_cmd_on_schema, (schema, cmd)) as \
                    thread, xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks(
                    timeout_secs=self.XEN_GUARD_WAIT_TIMEOUT)
                cursor.execute("begin;")
                cursor.execute(bg_dml)
                self._check_last_query_bursted(cluster, cursor)
                cursor.execute("abort;")
            # data validation
            self.verify_table_content_and_properties(cursor_bs, schema,
                                                     'dp20365_tbl', res2,
                                                     vector.diststyle)

            self._start_and_wait_for_refresh(cluster)
            cursor.execute(INSERT_SELECT)
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(INSERT_SELECT)
            self._check_last_query_bursted(cluster, cursor)
            self.verify_table_content_and_properties(cursor_bs, schema,
                                                     'dp20365_tbl', res3,
                                                     vector.diststyle)
