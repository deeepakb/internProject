# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions
from raff.burst.burst_temp_write import burst_user_temp_support_gucs, \
    BurstTempWrite
from raff.util.utils import PropagatingThread

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

OWNERSHIP_STATE_CHECK = ("select state from stv_burst_tbl_ownership "
                         "where id='{}.{}'::regclass::oid and arn like 'arn%'")
OWNERSHIP_TBL_CHECK = ("select * from stv_burst_tbl_ownership "
                       "where id='{}.{}'::regclass::oid")
# User Temp tables don't support the below DDL's
TEMP_SKIP_DIMENSION = ['alter_distall', 'alter_disteven', 'alter_distauto',
                       'alter_distkey', 'alter_sortkey', 'alter_sortkey_no_sort',
                       'alter_sortkey_none', 'alter_sortkey_auto', 'alter_encode',
                       'alter_distsort_all', 'alter_distsort_key']
VALIDATE_SQL = "SELECT * FROM {}.{} ORDER BY 1 limit 5;"
DROP_TABLE = 'DROP TABLE IF EXISTS {};'


@pytest.mark.ssm_perm_or_temp_config
@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_no_stable_rc
@pytest.mark.custom_local_gucs(gucs=burst_user_temp_support_gucs)
@pytest.mark.custom_burst_gucs(
    gucs=dict(list(burst_user_temp_support_gucs.items()) + [
        ('burst_enable_write', 'true'),
        ('burst_enable_write_copy', 'true'),
        ('burst_enable_write_insert', 'true'),
        ('vacuum_auto_worker_enable', 'false'),
    ]))
@pytest.mark.usefixtures("super_simulated_mode")
class TestDDLDisqualifyBurstWrite(BurstTempWrite):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['distkey'],
                sortkey=['sortkey'],
                ddl_cmd=[
                    'alter_distall', 'alter_disteven', 'alter_distauto',
                    'alter_distkey', 'alter_sortkey', 'alter_sortkey_no_sort',
                    'alter_sortkey_none', 'alter_sortkey_auto', 'alter_encode',
                    'alter_distsort_all', 'alter_distsort_key',
                    'alter_column_type', 'truncate', 'rename_column',
                    'add_column', 'add_column_collate', 'drop_column',
                    'rename_table'
                ]))

    def _setup_tables(self, db_session, schema, is_temp, vector):
        diststyle_one = diststyle_two = vector.diststyle
        sortkey_one = sortkey_two = vector.sortkey
        if vector.diststyle == 'distkey':
            diststyle_one = 'key distkey cr_returned_date_sk'
            diststyle_two = 'key distkey cc_call_center_id'
        if vector.sortkey == 'sortkey':
            sortkey_one = 'cr_item_sk'
            sortkey_two = 'cc_call_center_id'
        self._setup_table(db_session, schema, 'catalog_returns', 'tpcds', '1',
                          diststyle_one, sortkey_one, '_burst',
                          is_temp=is_temp)
        self._setup_table(db_session, schema, 'call_center', 'tpcds', '1',
                          diststyle_two, sortkey_two, '_burst',
                          is_temp=is_temp)
        # Add an extra varchar column for diverse ddl testing.
        with db_session.cursor() as cursor:
            schema = db_session.session_ctx.schema if not is_temp else \
                self._get_temp_table_schema(cursor, 'catalog_returns_burst')
            cursor.execute(
                "alter table {}.{} add column col_extra varchar(10) "
                "default ('aaa');".format(schema, "catalog_returns_burst"))
            cursor.execute(
                "alter table {}.{} add column col_extra varchar(10) "
                "default ('aaa');".format(schema, "call_center_burst"))

    def _should_skip(self, is_temp, vector):
        if is_temp and vector.ddl_cmd in TEMP_SKIP_DIMENSION:
            return True

    def _generate_ddl_cmd(self, schema, table, cmd_type):
        if cmd_type == 'alter_distall':
            cmd = "Alter Table {}.{} Alter diststyle ALL;"
        elif cmd_type == 'alter_disteven':
            cmd = "Alter Table {}.{} Alter diststyle EVEN;"
        elif cmd_type == 'alter_distauto':
            cmd = "Alter Table {}.{} Alter diststyle AUTO;"
        elif cmd_type == 'alter_sortkey_none':
            cmd = "Alter Table {}.{} Alter sortkey none;"
        elif cmd_type == 'alter_sortkey_auto':
            cmd = "Alter Table {}.{} alter sortkey auto;"
        elif cmd_type == 'alter_column_type':
            cmd = "ALTER TABLE {}.{} ALTER COLUMN col_extra TYPE varchar(512);"
        elif cmd_type == 'truncate':
            cmd = "TRUNCATE {}.{};"
        elif cmd_type == 'rename_column':
            cmd = "ALTER TABLE {}.{} RENAME COLUMN col_extra TO col_extra_1;"
        elif cmd_type == 'add_column':
            cmd = ("ALTER TABLE {}.{} ADD COLUMN col_extra_2 varchar(10) "
                   "default ('test');")
        elif cmd_type == 'add_column_collate':
            cmd = ("ALTER TABLE {}.{} ADD COLUMN col_extra_2 varchar(10) "
                   "COLLATE case_sensitive default ('test');")
        elif cmd_type == 'drop_column':
            cmd = "ALTER TABLE {}.{} Drop COLUMN col_extra;"
        elif cmd_type == 'rename_table':
            cmd = "ALTER TABLE {}.{} RENAME TO {}_rename;"

        # For testing large table specific cmds.
        elif table == 'catalog_returns_burst' and cmd_type == 'alter_distkey':
            cmd = "Alter Table {}.{} Alter distkey cr_item_sk;"
        elif table == 'catalog_returns_burst' and cmd_type == 'alter_sortkey':
            cmd = "Alter Table {}.{} Alter sortkey(cr_call_center_sk);"
        elif table == 'catalog_returns_burst' and \
                cmd_type == 'alter_sortkey_no_sort':
            cmd = "Alter Table {}.{} alter sortkey(cr_call_center_sk) no sort;"
        elif table == 'catalog_returns_burst' and cmd_type == 'alter_encode':
            cmd = "Alter table {}.{} alter column cr_reason_sk encode lzo;"
        elif table == 'catalog_returns_burst' and \
                cmd_type == 'alter_distsort_all':
            cmd = ("Alter Table {}.{} alter diststyle all, "
                   "alter sortkey(cr_call_center_sk);")
        elif table == 'catalog_returns_burst' and \
                cmd_type == 'alter_distsort_key':
            cmd = ("Alter Table {}.{} alter distkey cr_item_sk, "
                   "alter sortkey(cr_call_center_sk);")

        # For testing small table specific cmds.
        elif table == 'call_center_burst' and cmd_type == 'alter_distkey':
            cmd = "Alter Table {}.{} Alter distkey cc_call_center_sk;"
        elif table == 'call_center_burst' and cmd_type == 'alter_sortkey':
            cmd = "Alter Table {}.{} Alter sortkey(cc_call_center_sk);"
        elif table == 'call_center_burst' and \
                cmd_type == 'alter_sortkey_no_sort':
            cmd = "Alter Table {}.{} alter sortkey(cc_call_center_sk) no sort;"
        elif table == 'call_center_burst' and cmd_type == 'alter_encode':
            cmd = "Alter table {}.{} alter column cc_manager encode TEXT32K;"
        elif table == 'call_center_burst' and cmd_type == 'alter_distsort_all':
            cmd = ("Alter Table {}.{} alter diststyle all, "
                   "alter sortkey(cc_call_center_sk);")
        elif table == 'call_center_burst' and cmd_type == 'alter_distsort_key':
            cmd = ("Alter Table {}.{} alter distkey cc_company_name, "
                   "alter sortkey(cc_call_center_sk);")

        return cmd.format(schema,
                          table) if cmd_type != 'rename_table' else cmd.format(
                              schema, table, table)

    def _get_target_diststyle(self, cmd_type, orig_diststyle):
        if cmd_type == 'alter_distall':
            target_diststyle = 'diststyle all'
        elif cmd_type == 'alter_disteven':
            target_diststyle = 'diststyle even'
        elif cmd_type == 'alter_distauto':
            target_diststyle = 'diststyle auto'
        elif cmd_type == 'alter_distkey':
            target_diststyle = 'distkey'
        elif cmd_type == 'alter_distsort_all':
            target_diststyle = 'diststyle all'
        elif cmd_type == 'alter_distsort_key':
            target_diststyle = 'distkey'
        else:
            target_diststyle = orig_diststyle
        return target_diststyle

    def _get_event_name(self, vector):
        if vector.ddl_cmd == 'rename_column' or \
                vector.ddl_cmd == 'rename_table':
            event_name = 'EtSimSlowRename'
        else:
            event_name = 'EtSimSlowAlter'
        return event_name

    def test_ddl_disqualify_burst_insert_outtxn(self, cluster, is_temp, vector):
        """
        This test first create a large table catalog_returns_burst and
        a small table call_center_burst separately, then conduct some
        select and insert on these two tables and verify the queries
        are bursted. Then conduct ddl on these two tables and check
        insert query after ddl will not be bursted.
        """
        if self._should_skip(is_temp, vector):
            pytest.skip("Unsupported dimension {} for temp tables"
                        .format(vector.ddl_cmd))
        db_session = DbSession(cluster.get_conn_params(user='master'))
        target_diststyle = self._get_target_diststyle(vector.ddl_cmd,
                                                      vector.diststyle)
        with db_session.cursor() as cursor:
            # schema is required for creating perm/temp tables, since
            # we don't know schema before temp table creation, we pass
            # perm table schema
            schema = db_session.session_ctx.schema
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, schema, is_temp, vector)
            schema = db_session.session_ctx.schema if not is_temp else \
                self._get_temp_table_schema(cursor, 'catalog_returns_burst')

            self._start_and_wait_for_refresh(cluster)

            log.info("Testing {}".format(vector.ddl_cmd))
            ddl_one = self._generate_ddl_cmd(schema, 'catalog_returns_burst',
                                             vector.ddl_cmd)
            ddl_two = self._generate_ddl_cmd(schema, 'call_center_burst',
                                             vector.ddl_cmd)

            # select on large table burst
            cursor.execute("select count(*) from catalog_returns_burst;")
            catalog_returns_size = cursor.fetch_scalar()
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(
                "select * from catalog_returns_burst order by 1,2,3 limit 100;"
            )
            catalog_returns_content = cursor.fetchall()
            self._check_last_query_bursted(cluster, cursor)
            # select on small table burst
            cursor.execute("select count(*) from call_center_burst;")
            call_center_size = cursor.fetch_scalar()
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("select * from call_center_burst order by 1;")
            call_center_content = cursor.fetchall()
            self._check_last_query_bursted(cluster, cursor)

            # # insert on large table burst
            insert_stmt_one = ("insert into catalog_returns_burst "
                               "select * from catalog_returns_burst;")
            cursor.execute(insert_stmt_one)
            self._check_last_query_bursted(cluster, cursor)
            self._check_ownership_state(schema, "catalog_returns_burst",
                                        'Owned')
            cursor.execute("select count(*) from catalog_returns_burst;")
            assert cursor.fetch_scalar() == catalog_returns_size * 2
            cursor.execute(
                "select * from catalog_returns_burst order by 1,2,3 limit 200;"
            )
            catalog_returns_content = sorted(catalog_returns_content * 2)
            assert catalog_returns_content == cursor.fetchall()

            # insert on small table burst
            insert_stmt_two = ("insert into call_center_burst "
                               "select * from call_center_burst;")
            cursor.execute(insert_stmt_two)
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("select count(*) from call_center_burst;")
            assert cursor.fetch_scalar() == call_center_size * 2
            cursor.execute("select * from call_center_burst order by 1;")
            call_center_content = call_center_content * 2
            call_center_content.sort()
            assert call_center_content == cursor.fetchall()

            # validate after insert on both table
            self._validate_table(cluster, schema, 'catalog_returns_burst',
                                 vector.diststyle)
            self._validate_table(cluster, schema, 'call_center_burst', vector.diststyle)

            # conduct ddl on large table
            log.info("begin to conduct ddl on large table")
            cursor.execute(ddl_one)
            tbl_name = "catalog_returns_burst" if vector.ddl_cmd != 'rename_table' \
                else "catalog_returns_burst_rename"
            self._check_ownership_state(schema, tbl_name, None)
            log.info("begin insert after ddl on large table")
            # insert one more time after ddl, should not burst
            if vector.ddl_cmd != 'rename_table':
                cursor.execute(insert_stmt_one)
            else:
                insert_stmt_one_rename = (
                    "insert into catalog_returns_burst_rename "
                    "select * from catalog_returns_burst_rename;")
                cursor.execute(insert_stmt_one_rename)
            if is_temp:
                self._check_last_query_bursted(cluster, cursor)
            else:
                self._check_last_query_didnt_burst(cluster, cursor)
            # validate again
            self._validate_table(cluster, schema, tbl_name, target_diststyle)

            # conduct ddl on small table
            log.info("begin to conduct ddl on small table")
            cursor.execute(ddl_two)
            # table ownership is transferred to main after ddl
            tbl_name = "call_center_burst" if vector.ddl_cmd != 'rename_table' \
                else "call_center_burst_rename"

            self._check_ownership_state(schema, tbl_name, None)
            # insert one more time after ddl, should not burst
            log.info("begin insert after ddl on small table")
            if vector.ddl_cmd != 'rename_table':
                cursor.execute(insert_stmt_two)
            else:
                insert_stmt_two_rename = (
                    "insert into call_center_burst_rename "
                    "select * from call_center_burst_rename;")
                cursor.execute(insert_stmt_two_rename)
            if is_temp:
                self._check_last_query_bursted(cluster, cursor)
            else:
                self._check_last_query_didnt_burst(cluster, cursor)
            # validate again
            self._validate_table(cluster, schema, tbl_name, target_diststyle)
            # clean-up the tables
            if vector.ddl_cmd != 'rename_table':
                cursor.execute(DROP_TABLE.format('catalog_returns_burst'))
                cursor.execute(DROP_TABLE.format('call_center_burst'))
            else:
                cursor.execute(DROP_TABLE.format('catalog_returns_burst_rename'))
                cursor.execute(DROP_TABLE.format('call_center_burst_rename'))

    def test_ddl_disqualify_burst_insert_intxn(self, cluster, is_temp, vector):
        """
        This test first create a large table catalog_returns_burst and
        a small table call_center_burst separately and conducts insert
        and ddl in txn, commit and abort. Check insert after ddl is not
        bursted and table ownership is transferred.
        """
        # These cmds doesn't support txn.
        if vector.ddl_cmd == 'alter_sortkey_no_sort' or \
           vector.ddl_cmd == 'alter_sortkey_none' or \
           vector.ddl_cmd == 'alter_column_type':
            return
        if self._should_skip(is_temp, vector):
            pytest.skip("Unsupported dimension {} for temp tables"
                        .format(vector.ddl_cmd))
        db_session = DbSession(cluster.get_conn_params(user='master'))
        target_diststyle = self._get_target_diststyle(vector.ddl_cmd,
                                                      vector.diststyle)
        with db_session.cursor() as cursor:
            # schema is required for creating perm/temp tables, since
            # we don't know schema before temp table creation, we pass
            # perm table schema
            schema = db_session.session_ctx.schema
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, schema, is_temp, vector)
            schema = db_session.session_ctx.schema if not is_temp else \
                self._get_temp_table_schema(cursor, 'catalog_returns_burst')

            self._start_and_wait_for_refresh(cluster)

            log.info("Testing {}".format(vector.ddl_cmd))
            ddl_one = self._generate_ddl_cmd(schema, 'catalog_returns_burst',
                                             vector.ddl_cmd)
            ddl_two = self._generate_ddl_cmd(schema, 'call_center_burst',
                                             vector.ddl_cmd)

            # select on large table burst
            cursor.execute("select count(*) from catalog_returns_burst;")
            catalog_returns_size = cursor.fetch_scalar()
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(
                "select * from catalog_returns_burst order by 1,2,3;")
            catalog_returns_content_base = cursor.fetchall()
            self._check_last_query_bursted(cluster, cursor)
            # select on small table burst
            cursor.execute("select count(*) from call_center_burst;")
            call_center_size = cursor.fetch_scalar()
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("select * from call_center_burst order by 1;")
            call_center_content_base = cursor.fetchall()
            self._check_last_query_bursted(cluster, cursor)

            cursor.execute("begin")
            # insert on large table burst
            insert_stmt_one = ("insert into catalog_returns_burst "
                               "select * from catalog_returns_burst;")
            cursor.execute(insert_stmt_one)
            self._check_last_query_bursted(cluster, cursor)
            self._check_ownership_state(schema, "catalog_returns_burst",
                                        'Owned')

            cursor.execute("select count(*) from catalog_returns_burst;")
            assert cursor.fetch_scalar() == catalog_returns_size * 2
            cursor.execute(
                "select * from catalog_returns_burst order by 1,2,3;")
            catalog_returns_content = sorted(catalog_returns_content_base * 2)
            assert catalog_returns_content == cursor.fetchall()

            # conduct ddl on large table
            log.info("begin to conduct ddl on large table")
            cursor.execute(ddl_one)
            tbl_name = "catalog_returns_burst" if vector.ddl_cmd != 'rename_table' \
                else "catalog_returns_burst_rename"
            if is_temp:
                self._check_ownership_state(schema, "catalog_returns_burst",
                                            None)
            else:
                self._check_ownership_state(schema, "catalog_returns_burst",
                                            'Dirty')
            log.info("begin insert after ddl on large table")
            # insert one more time after ddl, should not burst
            if vector.ddl_cmd != 'rename_table':
                cursor.execute(insert_stmt_one)
            else:
                insert_stmt_one_rename = (
                    "insert into catalog_returns_burst_rename "
                    "select * from catalog_returns_burst_rename;")
                cursor.execute(insert_stmt_one_rename)
            if is_temp:
                self._check_last_query_bursted(cluster, cursor)
                self._check_ownership_state(schema, "catalog_returns_burst",
                                            'Owned')
            else:
                self._check_last_query_didnt_burst(cluster, cursor)
                self._check_ownership_state(schema, "catalog_returns_burst",
                                            'Dirty')
            cursor.execute("commit")
            if is_temp:
                self._check_ownership_state(schema, tbl_name,
                                            'Owned')
            else:
                self._check_ownership_state(schema, tbl_name,
                                            'Dirty')
            # insert one more time after txn, should not burst
            if vector.ddl_cmd != 'rename_table':
                cursor.execute(insert_stmt_one)
            else:
                insert_stmt_one_rename = (
                    "insert into catalog_returns_burst_rename "
                    "select * from catalog_returns_burst_rename;")
                cursor.execute(insert_stmt_one_rename)
            if is_temp:
                self._check_last_query_bursted(cluster, cursor)
            else:
                self._check_last_query_didnt_burst(cluster, cursor)
            cursor.execute("select count(*) from {};".format(tbl_name))
            if vector.ddl_cmd != 'truncate':
                assert cursor.fetch_scalar() == catalog_returns_size * 8
            else:
                assert cursor.fetch_scalar() == 0
            if is_temp:
                self._check_ownership_state(schema, tbl_name,
                                            'Owned')
            else:
                self._check_ownership_state(schema, tbl_name,
                                            'Dirty')

            # insert on small table burst
            insert_stmt_two = ("insert into call_center_burst "
                               "select * from call_center_burst;")
            cursor.execute("begin")
            cursor.execute(insert_stmt_two)
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("select count(*) from call_center_burst;")
            assert cursor.fetch_scalar() == call_center_size * 2
            cursor.execute("select * from call_center_burst order by 1;")
            call_center_content = call_center_content_base * 2
            call_center_content.sort()
            assert call_center_content == cursor.fetchall()
            self._check_ownership_state(schema, "call_center_burst", 'Owned')
            # conduct ddl on small table
            log.info("begin to conduct ddl on small table")
            cursor.execute(ddl_two)
            tbl_name = "call_center_burst" if vector.ddl_cmd != 'rename_table' \
                else "call_center_burst_rename"

            if is_temp:
                self._check_ownership_state(schema, "call_center_burst",
                                            None)
            else:
                self._check_ownership_state(schema, "call_center_burst",
                                            'Dirty')
            # insert one more time after ddl, should not burst
            log.info("begin insert after ddl on small table")
            if vector.ddl_cmd != 'rename_table':
                cursor.execute(insert_stmt_two)
            else:
                insert_stmt_two_rename = (
                    "insert into call_center_burst_rename "
                    "select * from call_center_burst_rename;")
                cursor.execute(insert_stmt_two_rename)
            if is_temp:
                self._check_last_query_bursted(cluster, cursor)
            else:
                self._check_last_query_didnt_burst(cluster, cursor)
            cursor.execute("abort")
            if vector.ddl_cmd != 'truncate':
                self._check_ownership_state(schema, "call_center_burst", None)
            elif is_temp:
                self._check_ownership_state(schema, "call_center_burst", 'Owned')
            else:
                self._check_ownership_state(schema, "call_center_burst", 'Dirty')
            # insert one more time after txn, should not burst
            log.info("begin insert after ddl on small table")
            cursor.execute(insert_stmt_two)
            if is_temp:
                self._check_last_query_bursted(cluster, cursor)
                self._check_ownership_state(schema, "call_center_burst", 'Owned')
            else:
                self._check_last_query_didnt_burst(cluster, cursor)
                if vector.ddl_cmd != 'truncate':
                    self._check_ownership_state(schema, "call_center_burst", None)
                else:
                    self._check_ownership_state(schema, "call_center_burst", 'Dirty')
            cursor.execute("select count(*) from call_center_burst;")
            if vector.ddl_cmd != 'truncate':
                assert cursor.fetch_scalar() == call_center_size * 2
            else:
                assert cursor.fetch_scalar() == 0
            cursor.execute("select * from call_center_burst order by 1;")

            if vector.ddl_cmd != 'truncate':
                call_center_content = sorted(call_center_content_base * 2)
                assert call_center_content == cursor.fetchall()
            else:
                assert cursor.fetchall() == []

            # validate both table
            if vector.ddl_cmd != 'rename_table':
                self._validate_table(cluster, schema, 'catalog_returns_burst',
                                     target_diststyle)
            else:
                self._validate_table(cluster, schema, 'catalog_returns_burst_rename',
                                     target_diststyle)
            self._validate_table(cluster, schema, 'call_center_burst', vector.diststyle)
            # clean-up the tables
            if vector.ddl_cmd != 'rename_table':
                cursor.execute(DROP_TABLE.format('catalog_returns_burst'))
                cursor.execute(DROP_TABLE.format('call_center_burst'))
            else:
                cursor.execute(DROP_TABLE.format('catalog_returns_burst_rename'))
                cursor.execute(DROP_TABLE.format('call_center_burst_rename'))

    def _alter_column(self, cursor, table, schema, vector):
        try:
            cmd_type = vector.ddl_cmd
            alter_ddl_cmd = self._generate_ddl_cmd(schema, table,
                                                   cmd_type)
            cursor.execute(alter_ddl_cmd)
        # exception caused by pg_cancel_backend()
        except Exception as e:
            ignore_errors = ["system requested abort",
                             "Query cancelled"]
            if not any(ignore in str(e) for ignore in ignore_errors):
                raise e

    def test_alter_column_abort(self, cluster, cursor, is_temp, vector):
        """
        Test temp table with alter column while aborting a transaction
        1. Run ALTER commands and terminate ALTER via abort.
        2. Validate temp table state reverts back to original
           and is correct when we burst queries referencing or writing to temp.
        3. Validate data integrity and table metadata properties
           (identity, row-id, sortedness) of the temp table.
        """
        # These cmds doesn't support txn.
        if vector.ddl_cmd == 'alter_sortkey_no_sort' or \
           vector.ddl_cmd == 'alter_sortkey_none' or \
           vector.ddl_cmd == 'alter_column_type' or \
           vector.ddl_cmd == 'truncate':
            return
        if self._should_skip(is_temp, vector):
            pytest.skip("Unsupported dimension {} for temp tables"
                        .format(vector.ddl_cmd))
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor:
            # schema is required for creating perm/temp tables, since
            # we don't know schema before temp table creation, we pass
            # perm table schema
            schema = db_session.session_ctx.schema
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, schema, is_temp, vector)
            schema = db_session.session_ctx.schema if not is_temp else \
                self._get_temp_table_schema(cursor, 'catalog_returns_burst')
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(
                VALIDATE_SQL.format(schema, "catalog_returns_burst"))
            result_1 = cursor.fetchall()
            cursor.execute(
                VALIDATE_SQL.format(schema, "call_center_burst"))
            result_2 = cursor.fetchall()
            cursor.execute("begin;")
            self._alter_column(cursor, "catalog_returns_burst", schema,
                               vector)
            self._alter_column(cursor, "call_center_burst", schema,
                               vector)
            cursor.execute("abort;")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(
                VALIDATE_SQL.format(schema, "catalog_returns_burst"))
            result_3 = cursor.fetchall()
            self._check_last_query_bursted(cluster, cursor)
            assert result_1 == result_3
            self._validate_table(cluster, schema, 'catalog_returns_burst',
                                 vector.diststyle)
            cursor.execute(
                VALIDATE_SQL.format(schema, "call_center_burst"))
            result_4 = cursor.fetchall()
            self._check_last_query_bursted(cluster, cursor)
            assert result_2 == result_4
            self._validate_table(cluster, schema, 'call_center_burst',
                                 vector.diststyle)
            # clean-up the tables
            cursor.execute(DROP_TABLE.format('catalog_returns_burst'))
            cursor.execute(DROP_TABLE.format('call_center_burst'))

    def test_alter_column_cancel(self, cluster, cursor, is_temp, vector):
        """
        Test temp table with alter column while aborting a transaction
        1. Run ALTER commands and terminate ALTER via pg_cancel_backend().
        2. Validate temp table state reverts back to original
           and is correct when we burst queries referencing or writing to temp.
        3. Validate data integrity and table metadata properties
           (identity, row-id, sortedness) of the temp table.
        """
        if vector.ddl_cmd == 'truncate':
            pytest.skip("Unsupported dimension {} for cancel query"
                        .format(vector.ddl_cmd))
        if self._should_skip(is_temp, vector):
            pytest.skip("Unsupported dimension {} for temp tables"
                        .format(vector.ddl_cmd))
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor, \
                self.db.cursor() as boot_cursor:
            # schema is required for creating perm/temp tables, since
            # we don't know schema before temp table creation, we pass
            # perm table schema
            schema = db_session.session_ctx.schema
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, schema, is_temp, vector)
            schema = db_session.session_ctx.schema if not is_temp else \
                self._get_temp_table_schema(cursor, 'catalog_returns_burst')
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(
                VALIDATE_SQL.format(schema, "catalog_returns_burst"))
            result_1 = cursor.fetchall()
            self._check_last_query_bursted(cluster, cursor)
            event_name = self._get_event_name(vector)
            cursor.execute('select pg_backend_pid()')
            pid = cursor.fetch_scalar()
            cancel_sql = 'select pg_cancel_backend({})'.format(pid)
            try:
                alter_thread1 = PropagatingThread(
                            target=self._alter_column,
                            args=(cursor, "catalog_returns_burst",
                                  schema, vector))
                with cluster.event(event_name, "timeout=30"):
                    alter_thread1.start()
                    # Wait for the thread to reach cancellation point
                    # and cancel it.
                    boot_cursor.execute(cancel_sql)
            finally:
                alter_thread1.join()
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(
                VALIDATE_SQL.format(schema, "catalog_returns_burst"))
            result_2 = cursor.fetchall()
            self._check_last_query_bursted(cluster, cursor)
            assert result_1 == result_2
            # validate tables after cancelling ALTER DDL
            self._validate_table(cluster, schema, "catalog_returns_burst",
                                 vector.diststyle)
            # clean-up the tables
            cursor.execute(DROP_TABLE.format('catalog_returns_burst'))
            cursor.execute(DROP_TABLE.format('call_center_burst'))
