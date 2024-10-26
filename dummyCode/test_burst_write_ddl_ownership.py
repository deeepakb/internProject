# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode,\
                                                         get_burst_conn_params
from raff.common.db.redshift_db import RedshiftDb
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.common.dimensions import Dimensions
from raff.util.utils import run_bootstrap_sql
from raff.burst.burst_test import get_burst_cluster_name
from raff.common.burst_helper import (get_cluster_by_identifier,
                                      get_cluster_by_arn, identifier_from_arn)
from raff.common.host_type import HostType

from test_burst_write_mixed_workload import TestBurstWriteMixedWorkloadBase, INSERT_CMD
log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

INSERT_SELECT_CMD = "INSERT INTO {} SELECT * FROM {}"
CREATE_STMT = "CREATE TABLE {} (c0 int, c1 int) {} {}"


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '3',
        'mv_enable_refresh_to_burst': 'true',
        'selective_dispatch_level': '0',
        'query_group': 'burst',
        'enable_result_cache_for_session': 'false',
        'burst_enable_write': 'true',
        'burst_enable_write_copy': 'true',
        'burst_enable_write_insert': 'true',
        'burst_enable_write_update': 'true',
        'burst_enable_write_delete': 'true',
        'burst_enable_write_user_ctas': 'false',
        'enable_mirror_to_s3': '2',
        'enable_commits_to_dynamo': '2',
        'burst_allow_dirty_reads': 'true',
        'try_burst_first': 'true',
        'xen_guard_enabled': 'true',
        'vacuum_auto_worker_enable': 'false',
        's3commit_enable_delete_tag_throttling': 'false',
        'max_unthrottled_delete_tags_per_cluster': 1500,
        'min_unthrottled_delete_tags_per_cluster': 64,
        'max_tps_delete_tags': 128,
        'delete_tags_time_window': 1,
    })
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.burst
@pytest.mark.usefixtures("cluster", "host_type", "db", "request_config")
class TestBurstWriteDDL(TestBurstWriteMixedWorkloadBase):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['distkey(c0)'],
                sortkey=['sortkey(c0)'],
                ddl_cmd=[
                    'alter_distall', 'alter_disteven', 'alter_distauto',
                    'alter_distkey', 'alter_sortkey', 'alter_sortkey_no_sort',
                    'alter_sortkey_none', 'alter_sortkey_auto', 'alter_encode',
                    'alter_distsort_all', 'alter_distsort_key',
                    'alter_column_type', 'truncate', 'rename_column',
                    'add_column', 'drop_column', 'rename_table'
                ]))

    def _setup_tables(self, db_session, base_tbl_name, vector):
        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")

        with db_session.cursor() as cursor:
            cursor.execute(
                CREATE_STMT.format(base_tbl_name, vector.diststyle,
                                   vector.sortkey))
            cursor.execute(INSERT_CMD.format(base_tbl_name))
            for i in range(10):
                cursor.execute(
                    INSERT_SELECT_CMD.format(base_tbl_name, base_tbl_name))
        # Add an extra varchar column for diverse ddl testing.
        with db_session.cursor() as cursor:
            cursor.execute("alter table {} add column col_extra varchar(10) "
                           "default ('aaa');".format(base_tbl_name))

    def _generate_ddl_cmd(self, table, cmd_type):
        if cmd_type == 'alter_distall':
            cmd = "Alter Table {} Alter diststyle ALL;"
        elif cmd_type == 'alter_disteven':
            cmd = "Alter Table {} Alter diststyle EVEN;"
        elif cmd_type == 'alter_distauto':
            cmd = "Alter Table {} Alter diststyle AUTO;"
        elif cmd_type == 'alter_distkey':
            cmd = "Alter Table {} alter distkey c1"
        elif cmd_type == 'alter_sortkey':
            cmd = "Alter Table {} alter sortkey(c1)"
        elif cmd_type == 'alter_sortkey_no_sort':
            cmd = "Alter Table {} alter sortkey(c1) no sort"
        elif cmd_type == 'alter_encode':
            cmd = "Alter Table {} alter column c1 encode zstd;"
        elif cmd_type == 'alter_distsort_all':
            cmd = ("Alter Table {} alter diststyle all, alter sortkey(c1)")
        elif cmd_type == 'alter_distsort_key':
            cmd = ("Alter Table {} alter distkey c1, alter sortkey(c1)")
        elif cmd_type == 'alter_sortkey_none':
            cmd = "Alter Table {} Alter sortkey none;"
        elif cmd_type == 'alter_sortkey_auto':
            cmd = "Alter Table {} alter sortkey auto;"
        elif cmd_type == 'alter_column_type':
            cmd = "ALTER TABLE {} ALTER COLUMN col_extra TYPE varchar(512);"
        elif cmd_type == 'truncate':
            cmd = "TRUNCATE {};"
        elif cmd_type == 'rename_column':
            cmd = "ALTER TABLE {} RENAME COLUMN col_extra TO col_extra_1;"
        elif cmd_type == 'add_column':
            cmd = ("ALTER TABLE {} ADD COLUMN col_extra_2 varchar(10) "
                   "default ('test');")
        elif cmd_type == 'drop_column':
            cmd = "ALTER TABLE {} Drop COLUMN col_extra;"
        elif cmd_type == 'rename_table':
            cmd = "ALTER TABLE {} RENAME TO {}_rename;"
        return cmd.format(table) if cmd_type != 'rename_table' else cmd.format(
            table, table)

    def _get_target_diststyle(self, cmd_type, orig_diststyle):
        if cmd_type == 'alter_distall':
            target_diststyle = 'diststyle all'
        elif cmd_type == 'alter_disteven':
            target_diststyle = 'diststyle even'
        elif cmd_type == 'alter_distauto':
            target_diststyle = 'diststyle auto'
        elif cmd_type == 'alter_distkey':
            target_diststyle = 'distkey(c1)'
        elif cmd_type == 'alter_distsort_all':
            target_diststyle = 'diststyle all'
        elif cmd_type == 'alter_distsort_key':
            target_diststyle = 'distkey(c1)'
        else:
            target_diststyle = orig_diststyle
        return target_diststyle

    def _post_validate(self, cluster, dml_cursor, schema, tbl_name, vector):
        # iduc can be bursted
        # note that we don't support burst write on distall tables.
        if vector.ddl_cmd != 'alter_distall' and vector.ddl_cmd != 'alter_distsort_all':
            self._insert_tables_bursted(cluster, dml_cursor, tbl_name)
            self._update_tables_1_bursted(cluster, dml_cursor, tbl_name)
            self._copy_tables_bursted(cluster, dml_cursor, tbl_name)
            self._delete_tables_bursted(cluster, dml_cursor, tbl_name)

    def test_burst_after_alter_add_drop_column(self, cluster, vector):
        """
        Target issue: DP-32756
        The data blocks in burst cluster CN's memory could be out of synced
        if the datablocks were pinned in memory before ALTER add/drop column.
        The column_desc inside data blocks could be invalid.
        This test adds the scenario that after the burst refresh, using select
        query to bring datablocks into memory and perform ALTER add/drop column
        for the target burst cluster owned table.
        Refresh again, make sure refresh update the metablock chain and read
        query can be bursted.
        """
        if vector.ddl_cmd not in ['add_column', 'drop_column']:
            pytest.skip("Unsupported dimension")

        test_schema = 'test_ddl_schema_{}'.format(str(uuid.uuid4().hex))
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp32756"

        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")

        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1:
            dml_cursor = session1.cursor()
            with DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2:
                check_cursor = session2.cursor()
                check_cursor.execute(
                    "set search_path to {}".format(test_schema))
                table_def = type('table_def', (object, ),
                                 dict(
                                     diststyle='distkey(c1)',
                                     sortkey='sortkey(c1)'))
                self._setup_tables(session1, base_tbl_name, table_def())
                self._start_and_wait_for_refresh(cluster)
                # iduc burst
                self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._update_tables_1_bursted(cluster, dml_cursor,
                                              base_tbl_name)
                self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._delete_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Burst', 'Owned')])
                # Pinned datablocks in memory
                dml_cursor.execute("set query_group to burst;")
                dml_cursor.execute("select * from {};".format(base_tbl_name))
                dml_cursor.execute("set query_group to metric;")
                dml_cursor.execute("select * from {};".format(base_tbl_name))

                # conduct ddl
                log.info("begin to conduct ddl")
                ddl_cmd = self._generate_ddl_cmd(base_tbl_name, vector.ddl_cmd)
                if vector.ddl_cmd == 'drop_column':
                    ddl_cmd = "ALTER TABLE {} DROP COLUMN c0;".format(
                        base_tbl_name)
                dml_cursor.execute(ddl_cmd)

                self._start_and_wait_for_refresh(cluster)
                # Trigger the issue
                dml_cursor.execute("set query_group to burst;")
                dml_cursor.execute("select * from {};".format(base_tbl_name))
                dml_cursor.execute("set query_group to metric;")
                dml_cursor.execute("select * from {};".format(base_tbl_name))

    def test_burst_write_ddl_without_txn(self, cluster, vector):
        """
        Test-1:
        bursted write;
        ddl;
        write cannot be bursted;
        refresh;
        write can be bursted;
        without txn, covers insert/delete/udpate/copy.
        """
        test_schema = 'test_ddl_schema_{}'.format(str(uuid.uuid4().hex))
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp29357"

        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")

        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1:
            dml_cursor = session1.cursor()
            with DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2:
                check_cursor = session2.cursor()
                check_cursor.execute(
                    "set search_path to {}".format(test_schema))
                self._setup_tables(session1, base_tbl_name, vector)
                self._start_and_wait_for_refresh(cluster)
                # iduc burst
                self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._update_tables_1_bursted(cluster, dml_cursor,
                                              base_tbl_name)
                self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._delete_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Burst', 'Owned')])

                # conduct ddl
                log.info("begin to conduct ddl")
                ddl_one = self._generate_ddl_cmd(base_tbl_name, vector.ddl_cmd)
                dml_cursor.execute(ddl_one)
                tbl_name = base_tbl_name if vector.ddl_cmd != 'rename_table' \
                                         else base_tbl_name + "_rename"
                self._validate_ownership_state(test_schema, tbl_name, [])

                # write after ddl couldnt burst without refresh
                self._insert_tables_cannot_bursted(cluster, dml_cursor,
                                                   tbl_name)

                self._copy_tables_cannot_bursted(cluster, dml_cursor, tbl_name)
                # backup and refresh
                self._start_and_wait_for_refresh(cluster)
                self._post_validate(cluster, dml_cursor, test_schema, tbl_name,
                                    vector)

    def test_burst_write_ddl_commit_immediate_refresh(self, cluster, vector):
        """
        Test-2:
        begin;
        bursted write;
        commit;
        ddl;
        refresh;
        write can be bursted;
        """
        test_schema = 'test_ddl_schema_{}'.format(str(uuid.uuid4().hex))
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp29357"

        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")

        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1:
            dml_cursor = session1.cursor()
            with DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2:
                check_cursor = session2.cursor()
                check_cursor.execute(
                    "set search_path to {}".format(test_schema))
                self._setup_tables(session1, base_tbl_name, vector)
                self._start_and_wait_for_refresh(cluster)
                # iduc burst
                dml_cursor.execute("begin")
                self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._update_tables_1_bursted(cluster, dml_cursor,
                                              base_tbl_name)
                self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._delete_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Burst', 'Owned')])
                dml_cursor.execute("commit")

                # conduct ddl
                log.info("begin to conduct ddl")
                ddl_one = self._generate_ddl_cmd(base_tbl_name, vector.ddl_cmd)
                dml_cursor.execute(ddl_one)
                tbl_name = base_tbl_name if vector.ddl_cmd != 'rename_table' \
                                         else base_tbl_name + "_rename"
                self._validate_ownership_state(test_schema, tbl_name, [])

                # backup and refresh
                self._start_and_wait_for_refresh(cluster)
                self._post_validate(cluster, dml_cursor, test_schema, tbl_name,
                                    vector)

    def test_burst_write_ddl_commit(self, cluster, vector):
        """
        Test-3:
        begin;
        bursted write;
        commit;
        ddl;
        write cannot be bursted;
        refresh;
        write can be bursted;
        """
        test_schema = 'test_ddl_schema_{}'.format(str(uuid.uuid4().hex))
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp29357"

        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")

        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1:
            dml_cursor = session1.cursor()
            with DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2:
                check_cursor = session2.cursor()
                check_cursor.execute(
                    "set search_path to {}".format(test_schema))
                self._setup_tables(session1, base_tbl_name, vector)
                self._start_and_wait_for_refresh(cluster)
                # iduc burst
                dml_cursor.execute("begin")
                self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._update_tables_1_bursted(cluster, dml_cursor,
                                              base_tbl_name)
                self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._delete_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Burst', 'Owned')])
                dml_cursor.execute("commit")

                # conduct ddl
                log.info("begin to conduct ddl")
                ddl_one = self._generate_ddl_cmd(base_tbl_name, vector.ddl_cmd)
                dml_cursor.execute(ddl_one)
                tbl_name = base_tbl_name if vector.ddl_cmd != 'rename_table' \
                                         else base_tbl_name + "_rename"
                self._validate_ownership_state(test_schema, tbl_name, [])

                self._insert_tables_cannot_bursted(cluster, dml_cursor,
                                                   tbl_name)

                self._copy_tables_cannot_bursted(cluster, dml_cursor, tbl_name)

                # backup and refresh
                self._start_and_wait_for_refresh(cluster)
                self._post_validate(cluster, dml_cursor, test_schema, tbl_name,
                                    vector)

    def test_burst_write_abort_ddl_immediate_refresh(self, cluster, vector):
        """
        Test-4:
        begin;
        bursted write;
        abort;
        ddl;
        refresh;
        write can be bursted;
        """
        test_schema = 'test_ddl_schema_{}'.format(str(uuid.uuid4().hex))
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp29357"

        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")

        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1:
            dml_cursor = session1.cursor()
            with DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2:
                check_cursor = session2.cursor()
                check_cursor.execute(
                    "set search_path to {}".format(test_schema))
                self._setup_tables(session1, base_tbl_name, vector)
                self._start_and_wait_for_refresh(cluster)
                # iduc burst
                dml_cursor.execute("begin")
                self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._update_tables_1_bursted(cluster, dml_cursor,
                                              base_tbl_name)
                self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._delete_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Burst', 'Owned')])
                dml_cursor.execute("abort")
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Main', 'Undo')])

                # conduct ddl
                log.info("begin to conduct ddl")
                ddl_one = self._generate_ddl_cmd(base_tbl_name, vector.ddl_cmd)
                dml_cursor.execute(ddl_one)
                tbl_name = base_tbl_name if vector.ddl_cmd != 'rename_table' else base_tbl_name + "_rename"
                self._validate_ownership_state(test_schema, tbl_name,
                                               [('Main', 'Undo')])
                # backup and refresh
                self._start_and_wait_for_refresh(cluster)
                self._validate_ownership_state(test_schema, tbl_name, [])
                self._post_validate(cluster, dml_cursor, test_schema, tbl_name,
                                    vector)

    def test_burst_write_abort_ddl(self, cluster, vector):
        """
        Test-5:
        begin;
        bursted write;
        abort;
        ddl;
        write cannot be bursted;
        refresh;
        write can be bursted;
        """
        test_schema = 'test_ddl_schema_{}'.format(str(uuid.uuid4().hex))
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp29357"

        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")

        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1:
            dml_cursor = session1.cursor()
            with DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2:
                check_cursor = session2.cursor()
                check_cursor.execute(
                    "set search_path to {}".format(test_schema))
                self._setup_tables(session1, base_tbl_name, vector)
                self._start_and_wait_for_refresh(cluster)
                # iduc burst
                dml_cursor.execute("begin")
                self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._update_tables_1_bursted(cluster, dml_cursor,
                                              base_tbl_name)
                self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._delete_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Burst', 'Owned')])
                dml_cursor.execute("abort")
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Main', 'Undo')])

                # conduct ddl
                log.info("begin to conduct ddl")
                ddl_one = self._generate_ddl_cmd(base_tbl_name, vector.ddl_cmd)
                dml_cursor.execute(ddl_one)
                tbl_name = base_tbl_name if vector.ddl_cmd != 'rename_table' \
                                         else base_tbl_name + "_rename"
                self._validate_ownership_state(test_schema, tbl_name,
                                               [('Main', 'Undo')])
                self._insert_tables_cannot_bursted(cluster, dml_cursor,
                                                   tbl_name)
                self._copy_tables_cannot_bursted(cluster, dml_cursor, tbl_name)

                # backup and refresh
                self._start_and_wait_for_refresh(cluster)
                self._validate_ownership_state(test_schema, tbl_name, [])
                self._post_validate(cluster, dml_cursor, test_schema, tbl_name,
                                    vector)

    @pytest.mark.super_simulated_no_stable_rc
    def test_ddl_abort_burst_write(self, cluster, vector):
        """
        Test-6:
        begin;
        ddl;
        abort;
        write cannot be bursted;
        refresh;
        write can be bursted;
        """
        # These cmds doesn't support txn.
        if vector.ddl_cmd == 'alter_sortkey_no_sort' or vector.ddl_cmd == 'alter_sortkey_none' or vector.ddl_cmd == 'alter_column_type':
            return
        test_schema = 'test_ddl_schema_{}'.format(str(uuid.uuid4().hex))
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp29357"

        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")

        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1:
            dml_cursor = session1.cursor()
            with DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2:
                check_cursor = session2.cursor()
                check_cursor.execute(
                    "set search_path to {}".format(test_schema))
                self._setup_tables(session1, base_tbl_name, vector)
                self._start_and_wait_for_refresh(cluster)

                # conduct ddl
                log.info("begin to conduct ddl")
                ddl_one = self._generate_ddl_cmd(base_tbl_name, vector.ddl_cmd)
                dml_cursor.execute("begin")
                dml_cursor.execute(ddl_one)
                dml_cursor.execute("abort")
                # truncate is auto-commit.
                if vector.ddl_cmd != 'truncate':
                    self._validate_ownership_state(test_schema, base_tbl_name,
                                                   [('Main', 'Undo')])
                else:
                    self._validate_ownership_state(test_schema, base_tbl_name,
                                                   [])
                self._insert_tables_cannot_bursted(cluster, dml_cursor,
                                                   base_tbl_name)

                self._copy_tables_cannot_bursted(cluster, dml_cursor,
                                                 base_tbl_name)

                # backup and refresh
                self._start_and_wait_for_refresh(cluster)
                self._validate_ownership_state(test_schema, base_tbl_name, [])
                self._post_validate(cluster, dml_cursor, test_schema,
                                    base_tbl_name, vector)

    def test_ddl_commit_burst_write(self, cluster, vector):
        """
        Test-7:
        begin;
        ddl;
        commit;
        write cannot be bursted;
        refresh;
        write can be bursted;
        """
        # These cmds doesn't support txn.
        if vector.ddl_cmd == 'alter_sortkey_no_sort' or vector.ddl_cmd == 'alter_sortkey_none' or vector.ddl_cmd == 'alter_column_type':
            return
        test_schema = 'test_ddl_schema_{}'.format(str(uuid.uuid4().hex))
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp29357"

        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")

        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1:
            dml_cursor = session1.cursor()
            with DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2:
                check_cursor = session2.cursor()
                check_cursor.execute(
                    "set search_path to {}".format(test_schema))
                self._setup_tables(session1, base_tbl_name, vector)
                self._start_and_wait_for_refresh(cluster)

                # conduct ddl
                log.info("begin to conduct ddl")
                ddl_one = self._generate_ddl_cmd(base_tbl_name, vector.ddl_cmd)
                dml_cursor.execute("begin")
                dml_cursor.execute(ddl_one)
                # truncate auto-commit
                if vector.ddl_cmd != 'truncate':
                    self._validate_ownership_state(test_schema, base_tbl_name,
                                                   [('Main', 'Owned')])
                else:
                    self._validate_ownership_state(test_schema, base_tbl_name,
                                                   [])
                dml_cursor.execute("commit")
                tbl_name = base_tbl_name if vector.ddl_cmd != 'rename_table' \
                                         else base_tbl_name + "_rename"
                self._validate_ownership_state(test_schema, tbl_name, [])

                self._insert_tables_cannot_bursted(cluster, dml_cursor,
                                                   tbl_name)

                self._copy_tables_cannot_bursted(cluster, dml_cursor, tbl_name)
                # backup and refresh
                self._start_and_wait_for_refresh(cluster)

                self._validate_ownership_state(test_schema, tbl_name, [])

                self._post_validate(cluster, dml_cursor, test_schema, tbl_name,
                                    vector)

    def test_bursted_write_ddl_same_txn_commit(self, cluster, vector):
        """
        Test-8:
        begin;
        bursted write;
        ddl;
        read & write cannot be bursted;
        commit;
        backup and refresh;
        write can be bursted;
        """
        # These cmds doesn't support txn.
        if vector.ddl_cmd == 'alter_sortkey_no_sort' or vector.ddl_cmd == 'alter_sortkey_none' or vector.ddl_cmd == 'alter_column_type':
            return
        test_schema = 'test_ddl_schema_{}'.format(str(uuid.uuid4().hex))
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp29357"

        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")

        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1:
            dml_cursor = session1.cursor()
            with DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2:
                check_cursor = session2.cursor()
                check_cursor.execute(
                    "set search_path to {}".format(test_schema))
                self._setup_tables(session1, base_tbl_name, vector)
                self._start_and_wait_for_refresh(cluster)

                # conduct bursted write and ddl in same txn
                log.info("begin to conduct ddl")
                ddl_one = self._generate_ddl_cmd(base_tbl_name, vector.ddl_cmd)
                tbl_name = base_tbl_name if vector.ddl_cmd != 'rename_table' \
                                         else base_tbl_name + "_rename"
                dml_cursor.execute("begin")
                self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._update_tables_1_bursted(cluster, dml_cursor,
                                              base_tbl_name)
                self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._delete_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Burst', 'Owned')])
                dml_cursor.execute(ddl_one)
                # truncate auto-commit
                if vector.ddl_cmd != 'truncate':
                    self._validate_ownership_state(test_schema, base_tbl_name,
                                                   [('Burst', 'Dirty'),
                                                    ('Main', 'Owned')])
                else:
                    self._validate_ownership_state(test_schema, base_tbl_name,
                                                   [('Burst', 'Dirty')])
                self._insert_tables_cannot_bursted(cluster, dml_cursor,
                                                   tbl_name)

                self._copy_tables_cannot_bursted(cluster, dml_cursor, tbl_name)
                self._read_cannot_bursted(cluster, dml_cursor, tbl_name)

                if vector.ddl_cmd != 'truncate':
                    self._validate_ownership_state(test_schema, base_tbl_name,
                                                   [('Burst', 'Dirty'),
                                                    ('Main', 'Owned')])
                else:
                    self._validate_ownership_state(test_schema, base_tbl_name,
                                                   [('Burst', 'Dirty')])
                dml_cursor.execute("commit")

                self._validate_ownership_state(test_schema, tbl_name,
                                               [('Burst', 'Dirty')])
                # backup and refresh
                self._start_and_wait_for_refresh(cluster)
                self._validate_ownership_state(test_schema, tbl_name, [])
                self._post_validate(cluster, dml_cursor, test_schema, tbl_name,
                                    vector)

    def test_bursted_write_ddl_same_txn_abort(self, cluster, vector):
        """
        Test-9:
        begin;
        bursted write;
        ddl;
        read & write cannot be bursted;
        abort;
        backup and refresh;
        write can be bursted;
        """
        # These cmds doesn't support txn.
        if vector.ddl_cmd == 'alter_sortkey_no_sort' or vector.ddl_cmd == 'alter_sortkey_none' or vector.ddl_cmd == 'alter_column_type':
            return
        test_schema = 'test_ddl_schema_{}'.format(str(uuid.uuid4().hex))
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp29357"

        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")

        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1:
            dml_cursor = session1.cursor()
            with DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2:
                check_cursor = session2.cursor()
                check_cursor.execute(
                    "set search_path to {}".format(test_schema))
                self._setup_tables(session1, base_tbl_name, vector)
                self._start_and_wait_for_refresh(cluster)

                # conduct bursted write and ddl in same txn
                log.info("begin to conduct ddl")
                ddl_one = self._generate_ddl_cmd(base_tbl_name, vector.ddl_cmd)
                tbl_name = base_tbl_name if vector.ddl_cmd != 'rename_table' \
                                         else base_tbl_name + "_rename"

                dml_cursor.execute("begin")
                self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._update_tables_1_bursted(cluster, dml_cursor,
                                              base_tbl_name)
                self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._delete_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Burst', 'Owned')])
                dml_cursor.execute(ddl_one)

                if vector.ddl_cmd != 'truncate':
                    self._validate_ownership_state(test_schema, base_tbl_name,
                                                   [('Burst', 'Dirty'),
                                                    ('Main', 'Owned')])
                else:
                    self._validate_ownership_state(test_schema, base_tbl_name,
                                                   [('Burst', 'Dirty')])
                self._insert_tables_cannot_bursted(cluster, dml_cursor,
                                                   tbl_name)

                self._copy_tables_cannot_bursted(cluster, dml_cursor, tbl_name)
                self._read_cannot_bursted(cluster, dml_cursor, tbl_name)

                if vector.ddl_cmd != 'truncate':
                    self._validate_ownership_state(test_schema, base_tbl_name,
                                                   [('Burst', 'Dirty'),
                                                    ('Main', 'Owned')])
                else:
                    self._validate_ownership_state(test_schema, base_tbl_name,
                                                   [('Burst', 'Dirty')])
                dml_cursor.execute("abort")
                if vector.ddl_cmd != 'truncate':
                    self._validate_ownership_state(test_schema, base_tbl_name,
                                                   [('Main', 'Undo')])
                else:
                    self._validate_ownership_state(test_schema, base_tbl_name,
                                                   [('Burst', 'Dirty')])
                # backup and refresh
                self._start_and_wait_for_refresh(cluster)
                self._validate_ownership_state(test_schema, base_tbl_name, [])
                self._post_validate(cluster, dml_cursor, test_schema,
                                    base_tbl_name, vector)
