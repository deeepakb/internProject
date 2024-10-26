# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.dimensions import Dimensions
from raff.storage.alter_table_backup_restore import AlterTableAutoBackupRestore
from raff.burst.burst_temp_write import (burst_user_temp_support_gucs,
                                         BurstTempWrite)

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]


create_stmt = "create {} table dp28724(c0 int,c1 varchar(120),c2 date,c3 bigint){}"
insert_stmt = "insert into dp28724 select c0+1, c1+'a', c2, c3+1 from dp28724;"
insert_values = ("insert into dp28724 values"
                 "(1,'1','2020-01-01',2),(2,'2','2020-01-02',2),"
                 "(3,'3','2020-01-03',2),(4,'4','2020-01-04',4),"
                 "(5,'5','2020-01-05',5),(6,'6','2020-01-06',6),"
                 "(7,'7','2020-01-06',7),(8,'8','2020-01-08',8),"
                 "(9,'9','2020-01-09',9),(10,'10','2020-01-10',10);")
read_stmt = "select count(*) from dp28724;"
delete_stmt = "delete from dp28724 where c0 = {};"
update_stmt = "update dp28724 set c0=c0+1,c1=c1+'a',c3=c3+1;"


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.ssm_perm_or_temp_config
@pytest.mark.super_simulated_precommit
@pytest.mark.custom_burst_gucs(gucs=dict(list(burst_user_temp_support_gucs.items()) +
                                         [('burst_enable_write', 'true')]))
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstReadWriteOwnership(BurstTempWrite, AlterTableAutoBackupRestore):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(diststyle=['distkey(c0)'],
                 init_state=['non-pristine',],
                 query=['insert select', 'insert value']))

    def _setup_tables(self, db_session, vector, is_temp):
        self.cleanup_s3()
        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable both';")
        with db_session.cursor() as cursor:
            table_type = 'temp' if is_temp else ''
            cursor.execute("DROP TABLE IF EXISTS dp28724;")
            cursor.execute(create_stmt.format(table_type, vector.diststyle))
            if vector.init_state == 'non-pristine':
                cursor.execute('begin;')
                cursor.execute(insert_values)
                for i in range(16):
                    cursor.execute(insert_stmt)
                cursor.execute('commit;')

    def _get_query(self, query, counter):
        if query == 'insert select':
            return insert_stmt
        elif query == 'insert value':
            return insert_values
        elif query == 'update':
            return update_stmt
        else:
            return delete_stmt.format(counter)

    def _burst_read_write(self, cluster, cursor, vector, counter):
        cursor.execute(read_stmt)
        self._check_last_query_bursted(cluster, cursor)
        cursor.execute(self._get_query(vector.query, counter))
        self._check_last_query_bursted(cluster, cursor)

    def _non_burst_read_write(self, cluster, cursor, vector, counter):
        cursor.execute(read_stmt)
        self._check_last_query_didnt_burst(cluster, cursor)
        cursor.execute(self._get_query(vector.query, counter))
        self._check_last_query_didnt_burst(cluster, cursor)

    def _burst_read_only(self, cluster, cursor, vector, counter):
        cursor.execute(read_stmt)
        self._check_last_query_bursted(cluster, cursor)
        cursor.execute(self._get_query(vector.query, counter))
        self._check_last_query_didnt_burst(cluster, cursor)

    def test_main_read_write_txn_commit(self, cluster, db_session, vector,
                                        is_temp):
        """
        Test: Run read-only and write query on main cluster, commit txn,
              then subsequent query can not burst until refresh.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, vector, is_temp)
            if is_temp:
                schema = self._get_temp_table_schema(cursor, "dp28724")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("begin;")
            # Force to run on main cluster
            cursor.execute("set query_group to metrics;")
            self._non_burst_read_write(cluster, cursor, vector, 1)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Owned')])
            self._non_burst_read_write(cluster, cursor, vector, 2)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Owned')])
            cursor.execute("commit;")
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
            # Since write query ran on main, then subsequent query can not
            # burst until next refresh unless table is temp. Since temp table
            # queries can still burst, expect it to be 'Owned'
            cursor.execute("set query_group to burst;")
            if is_temp:
                self._burst_read_write(cluster, cursor, vector, 3)
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Burst', 'Owned')])
            else:
                self._non_burst_read_write(cluster, cursor, vector, 3)
                self._validate_ownership_state(schema, 'dp28724', [])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
            self._start_and_wait_for_refresh(cluster)
            # After cluster is refreshed, then both read and write query can
            # be supported.
            self._burst_read_write(cluster, cursor, vector, 4)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)

    def test_main_read_write_txn_commit_immediate_backup_refresh(
            self, cluster, db_session, vector, is_temp):
        """
        Test: Run read-only and write query on main cluster, commit txn,
              backup and refresh, then subsequent query can burst.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, vector, is_temp)
            if is_temp:
                schema = self._get_temp_table_schema(cursor, "dp28724")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("begin;")
            # Force to run on main cluster
            cursor.execute("set query_group to metrics;")
            self._non_burst_read_write(cluster, cursor, vector, 1)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Owned')])
            self._non_burst_read_write(cluster, cursor, vector, 2)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Owned')])
            cursor.execute("commit;")
            self._validate_ownership_state(schema, 'dp28724', [])
            # Backup and refresh immediately.
            self._start_and_wait_for_refresh(cluster)
            # The subsequent query can burst now.
            cursor.execute("set query_group to burst;")
            self._burst_read_write(cluster, cursor, vector, 3)
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._burst_read_write(cluster, cursor, vector, 4)
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])

    def test_main_read_write_txn_abort(self, cluster, db_session, vector,
                                       is_temp):
        """
        Test: Run read-only and write query on main cluster, abort txn,
              then subsequent query can not burst until refresh.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, vector, is_temp)
            if is_temp:
                schema = self._get_temp_table_schema(cursor, "dp28724")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("begin;")
            # Force to run on main cluster
            cursor.execute("set query_group to metrics;")
            self._non_burst_read_write(cluster, cursor, vector, 1)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Owned')])
            self._non_burst_read_write(cluster, cursor, vector, 2)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Owned')])
            cursor.execute("abort;")
            self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Undo')])
            # Since write query s aborted, table enters Undo state,
            # then subsequent query cann't burst unless executed on temp
            cursor.execute("set query_group to burst;")
            if is_temp:
                self._burst_read_write(cluster, cursor, vector, 3)
                self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned'), ('Main', 'Undo')])
            else:
                self._non_burst_read_write(cluster, cursor, vector, 3)
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Main', 'Undo')])

            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
            # After cluster is refreshed, then both read and write query can
            # be supported.
            self._start_and_wait_for_refresh(cluster)
            self._burst_read_write(cluster, cursor, vector, 4)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)

    def test_main_read_write_txn_abort_immediate_backup_refresh(
            self, cluster, db_session, vector, is_temp):
        """
        Test: Run read-only and write query on main cluster, abort txn,
              backup and refresh, then subsequent query can burst.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, vector, is_temp)
            if is_temp:
                schema = self._get_temp_table_schema(cursor, "dp28724")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("begin;")
            # Force to run on main cluster
            cursor.execute("set query_group to metrics;")
            self._non_burst_read_write(cluster, cursor, vector, 1)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Owned')])
            self._non_burst_read_write(cluster, cursor, vector, 2)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Owned')])
            cursor.execute("abort;")
            self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Undo')])
            # Backup and refresh immediately.
            self._start_and_wait_for_refresh(cluster)

            # Refreshing should not influence the temp table ownership state
            self._validate_ownership_state(schema, 'dp28724', [])
            # Then subsequent query can burst.
            cursor.execute("set query_group to burst;")
            self._burst_read_write(cluster, cursor, vector, 3)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
            self._burst_read_write(cluster, cursor, vector, 4)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)

    def test_burst_read_write_txn_commit(self, cluster, db_session, vector,
                                         is_temp):
        """
        Test: Run read-only and write queries on burst cluster, commit txn,
              the subsequent queries can burst.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, vector, is_temp)
            if is_temp:
                schema = self._get_temp_table_schema(cursor, "dp28724")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("begin;")
            # Force to run on burst cluster.
            self._burst_read_write(cluster, cursor, vector, 1)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._burst_read_write(cluster, cursor, vector, 2)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            cursor.execute("commit;")
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            # Since write query ran on burst, the burst cluster owns target
            # table, then subsequent query can burst.
            cursor.execute("set query_group to burst;")
            self._burst_read_write(cluster, cursor, vector, 3)
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            # After cluster is refreshed, then both read and write query should
            # still be able to burst.
            self._start_and_wait_for_refresh(cluster)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._burst_read_write(cluster, cursor, vector, 4)
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])

    def test_burst_read_write_txn_commit_immediately_backup_refresh(
            self, cluster, db_session, vector, is_temp):
        """
        Test: Run read-only and write queries on burst cluster, commit txn,
              the subsequent queries can burst.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, vector, is_temp)
            if is_temp:
                schema = self._get_temp_table_schema(cursor, "dp28724")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("begin;")
            # Force to run on burst cluster.
            self._burst_read_write(cluster, cursor, vector, 1)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._burst_read_write(cluster, cursor, vector, 2)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            cursor.execute("commit;")
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            # Backup and refresh immediately.
            self._start_and_wait_for_refresh(cluster)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            # Since write query ran on burst, the burst cluster owns target
            # table, then subsequent query can burst.
            cursor.execute("set query_group to burst;")
            self._burst_read_write(cluster, cursor, vector, 3)
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._burst_read_write(cluster, cursor, vector, 4)
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])

    def test_burst_read_write_txn_abort(self, cluster, db_session, vector,
                                        is_temp):
        """
        Test: Run read-only and write queries on burst cluster, abort txn,
              then subsequent queries can not burst until backup and refresh.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, vector, is_temp)
            if is_temp:
                schema = self._get_temp_table_schema(cursor, "dp28724")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("begin;")
            # Force to run on burst cluster.
            self._burst_read_write(cluster, cursor, vector, 1)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._burst_read_write(cluster, cursor, vector, 2)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            cursor.execute("abort;")
            self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Undo')])
            # Since the transaction is aborted, the table is dirty on target
            # table, then both read and write is not valid for this table to
            # be bursted if table is perm type
            if is_temp:
                self._burst_read_write(cluster, cursor, vector, 3)
                self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned'), ('Main', 'Undo')])
            else:
                self._non_burst_read_write(cluster, cursor, vector, 3)
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Main', 'Undo')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
            # After cluster is refreshed, then both read and write query should
            # still be able to burst.
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")
            if is_temp:
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Burst', 'Owned')])
            else:
                self._validate_ownership_state(schema, 'dp28724', [])
            self._burst_read_write(cluster, cursor, vector, 4)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)

    def test_burst_read_write_txn_abort_immediately_backup_refresh(
            self, cluster, db_session, vector, is_temp):
        """
        Test: Run read-only and write queries on burst cluster, abort txn,
              immediately backup and refresh, the subsequent queries can burst.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, vector, is_temp)
            if is_temp:
                schema = self._get_temp_table_schema(cursor, "dp28724")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("begin;")
            # Force to run on burst cluster.
            self._burst_read_write(cluster, cursor, vector, 1)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._burst_read_write(cluster, cursor, vector, 2)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            cursor.execute("abort;")
            self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Undo')])
            # Since the transaction is aborted, the table is dirty on target
            # table, then both read and write is not valid for this table to
            # be bursted.
            # We do a dummy disk commit to erase owned by main state, then
            # subsequent query can be bursted after refresh.
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")
            self._validate_ownership_state(schema, 'dp28724', [])
            self._burst_read_write(cluster, cursor, vector, 3)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
            cursor.execute("set query_group to burst;")
            self._burst_read_write(cluster, cursor, vector, 4)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)

    def test_burst_transfer_to_main_commit(self, cluster, db_session, vector,
                                           is_temp):
        """
        Test: Burst read-only and write on same transaction, while ownership is
              transferred to main cluster, commit transaction. The subsequent
              read and write queries can not burst, until backup and refresh.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, vector, is_temp)
            if is_temp:
                schema = self._get_temp_table_schema(cursor, "dp28724")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("begin;")
            # Force to run on burst cluster.
            cursor.execute("set query_group to burst;")
            self._burst_read_write(cluster, cursor, vector, 1)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            # Force to run on main cluster. Busrt cluster is dirty and
            # main cluster owned this table.
            cursor.execute("set query_group to metrics;")
            self._non_burst_read_write(cluster, cursor, vector, 2)
            if is_temp:
                self._validate_ownership_state(
                        schema, 'dp28724', [('Main', 'Owned')])
            else:
                self._validate_ownership_state(
                        schema, 'dp28724', [('Burst', 'Dirty'), ('Main', 'Owned')])
            cursor.execute("commit;")
            # Since burst cluster is dirty and main owns this table, both
            # read and write can not be bursted.
            cursor.execute("set query_group to burst;")
            if is_temp:
                self._burst_read_write(cluster, cursor, vector, 3)
                self._validate_ownership_state(schema, 'dp28724', [('Burst', 'Owned')])
            else:
                self._non_burst_read_write(cluster, cursor, vector, 3)
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Burst', 'Dirty')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
            # After refresh, the burst cluster cleans dirty and main cluster
            # erases the its ownership, thus burst can handle both read and
            # write query.
            self._start_and_wait_for_refresh(cluster)
            if is_temp:
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Burst', 'Owned')])
            else:
                self._validate_ownership_state(schema, 'dp28724', [])
            self._burst_read_write(cluster, cursor, vector, 4)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)

    def test_burst_transfer_to_main_commit_immediately_backup_refresh(
            self, cluster, db_session, vector, is_temp):
        """
        Test: Burst read-only and write on same transaction, while ownership is
              transferred to main cluster, commit transaction. Immediately
              backup and refresh, the subsequent read and write queries can
              burst.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, vector, is_temp)
            if is_temp:
                schema = self._get_temp_table_schema(cursor, "dp28724")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("begin;")
            # Force to run on burst cluster.
            cursor.execute("set query_group to burst;")
            self._burst_read_write(cluster, cursor, vector, 1)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            # Force to run on main cluster. Busrt cluster is dirty and
            # main cluster owned this table.
            cursor.execute("set query_group to metrics;")
            self._non_burst_read_write(cluster, cursor, vector, 2)
            if is_temp:
                self._validate_ownership_state(
                        schema, 'dp28724', [('Main', 'Owned')])
            else:
                self._validate_ownership_state(
                        schema, 'dp28724', [('Burst', 'Dirty'), ('Main', 'Owned')])
            cursor.execute("commit;")
            if is_temp:
                self._validate_ownership_state(
                    schema, 'dp28724', [])
            else:
                self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Dirty')])
            # Immediately backup and refresh.
            self._start_and_wait_for_refresh(cluster)
            self._validate_ownership_state(schema, 'dp28724', [])
            cursor.execute("set query_group to burst;")
            self._burst_read_write(cluster, cursor, vector, 3)
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._burst_read_write(cluster, cursor, vector, 4)
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])

    def test_burst_transfer_to_main_abort(self, cluster, db_session, vector,
                                          is_temp):
        """
        Test: Burst read-only and write queries on same transaction, while
              ownership is transferred to main cluster and abort transaction.
              Try burst read and write before and after burst cluster refresh.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, vector, is_temp)
            if is_temp:
                schema = self._get_temp_table_schema(cursor, "dp28724")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("begin;")
            # Force to run on burst cluster.
            cursor.execute("set query_group to burst;")
            self._burst_read_write(cluster, cursor, vector, 1)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            # Force to run on main cluster. Busrt cluster is dirty and
            # main cluster owned this table.
            cursor.execute("set query_group to metrics;")
            self._non_burst_read_write(cluster, cursor, vector, 2)
            if is_temp:
                self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Owned')])
            else:
                self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Dirty'), ('Main', 'Owned')])
            cursor.execute("abort;")
            self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Undo')])
            # Since burst cluster is dirty and main owns this table, both
            # read and write can not be bursted.
            cursor.execute("set query_group to burst;")
            if is_temp:
                self._burst_read_write(cluster, cursor, vector, 3)
                self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned'), ('Main', 'Undo')])
            else:
                self._non_burst_read_write(cluster, cursor, vector, 3)
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Main', 'Undo')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
            # After refresh, the burst cluster cleans dirty and main cluster
            # erases the its ownership, thus burst can handle both read and
            # write query.
            self._start_and_wait_for_refresh(cluster)
            if is_temp:
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Burst', 'Owned')])
            else:
                self._validate_ownership_state(schema, 'dp28724', [])
            self._burst_read_write(cluster, cursor, vector, 4)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)

    def test_burst_transfer_to_main_abort_immediately_backup_refresh(
            self, cluster, db_session, vector, is_temp):
        """
        Test: Burst read-only and write queries on same transaction, while
              ownership is transferred to main cluster and abort transaction.
              Immediately, backup and refresh cluster, then subsequent
              read-only and write queries can burst.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, vector, is_temp)
            if is_temp:
                schema = self._get_temp_table_schema(cursor, "dp28724")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("begin;")
            # Force to run on burst cluster.
            cursor.execute("set query_group to burst;")
            self._burst_read_write(cluster, cursor, vector, 1)
            self._validate_ownership_state(
                schema, 'dp28724', [('Burst', 'Owned')])
            # Force to run on main cluster. Busrt cluster is dirty and
            # main cluster owned this table.
            cursor.execute("set query_group to metrics;")
            self._non_burst_read_write(cluster, cursor, vector, 2)
            if is_temp:
                self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Owned')])
            else:
                self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Dirty'), ('Main', 'Owned')])
            cursor.execute("abort;")
            self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Undo')])
            # Immediately backup and refresh.
            self._start_and_wait_for_refresh(cluster)
            self._validate_ownership_state(schema, 'dp28724', [])
            self._burst_read_write(cluster, cursor, vector, 3)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)

            self._burst_read_write(cluster, cursor, vector, 4)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)

    def test_main_read_write_and_try_burst_commit(self, cluster, db_session, vector,
                                                  is_temp):
        """
        Test: Burst read-only and write queries on same transaction, while
              ownership is transferred to main cluster initially and commit
              transaction. Then, the read-only and write queries can not
              burst until next backup and refresh.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, vector, is_temp)
            if is_temp:
                schema = self._get_temp_table_schema(cursor, "dp28724")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("begin;")
            # Force to run on main cluster.
            cursor.execute("set query_group to metrics;")
            self._non_burst_read_write(cluster, cursor, vector, 1)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Owned')])
            # Since write runs on main, the subsequent queries can not run
            # on burst cluster. Temp should be able to burst write
            cursor.execute("set query_group to burst;")
            if is_temp:
                self._burst_read_write(cluster, cursor, vector, 2)
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Burst', 'Owned')])
            else:
                self._non_burst_read_write(cluster, cursor, vector, 2)
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Main', 'Owned')])
            cursor.execute("commit;")
            # Since main cluster write query blocks subsequent query to burst,
            # both read and write query can not burst until refresh.
            cursor.execute("set query_group to burst;")
            if is_temp:
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Burst', 'Owned')])
            else:
                self._validate_ownership_state(schema, 'dp28724', [])

            if is_temp:
                self._burst_read_write(cluster, cursor, vector, 3)
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Burst', 'Owned')])
            else:
                self._non_burst_read_write(cluster, cursor, vector, 3)
                self._validate_ownership_state(schema, 'dp28724', [])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
            # After refresh is done, the burst cluster should have latest
            # sb_version, thus should be able to handle both read and write
            # query.
            self._start_and_wait_for_refresh(cluster)
            self._burst_read_write(cluster, cursor, vector, 4)
            self._validate_ownership_state(schema, 'dp28724', [('Burst', 'Owned')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)

    def test_main_read_write_and_try_burst_commit_immediately_backup_refresh(
            self, cluster, db_session, vector, is_temp):
        """
        Test: Burst read-only and write queries on same transaction, while
              ownership is transferred to main cluster initially and commit
              transaction. Immediately, backup and refresh cluster. Then,
              subsequent read-only and write queries can burst.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, vector, is_temp)
            if is_temp:
                schema = self._get_temp_table_schema(cursor, "dp28724")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("begin;")
            # Force to run on main cluster.
            cursor.execute("set query_group to metrics;")
            self._non_burst_read_write(cluster, cursor, vector, 1)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Owned')])
            # Since write runs on main, the subsequent queries can not run
            # on burst cluster.
            cursor.execute("set query_group to burst;")
            if is_temp:
                self._burst_read_write(cluster, cursor, vector, 2)
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Burst', 'Owned')])
            else:
                self._non_burst_read_write(cluster, cursor, vector, 2)
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Main', 'Owned')])
            cursor.execute("commit;")
            if is_temp:
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Burst', 'Owned')])
            else:
                self._validate_ownership_state(schema, 'dp28724', [])
            # Immediately backup and refresh.
            self._start_and_wait_for_refresh(cluster)
            if is_temp:
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Burst', 'Owned')])
            else:
                self._validate_ownership_state(schema, 'dp28724', [])
            # After refresh is done, the burst cluster should have latest
            # sb_version, thus should be able to handle both read and write
            # query.
            self._burst_read_write(cluster, cursor, vector, 3)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
            self._burst_read_write(cluster, cursor, vector, 4)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)

    def test_burst_read_write_and_try_burst_abort(self, cluster, db_session, vector,
                                                  is_temp):
        """
        Test: Run read-only and write queries on main cluster, then the
              subsequent read-only or write queries can not burst in
              same transaction. After transaction abort, both read-only
              and write query can not burst until refresh.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, vector, is_temp)
            if is_temp:
                schema = self._get_temp_table_schema(cursor, "dp28724")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("begin;")
            # Force to run on main cluster
            cursor.execute("set query_group to metrics;")
            self._non_burst_read_write(cluster, cursor, vector, 1)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Owned')])
            # Since write runs on main, the subsequent queries can not run
            # on burst cluster.
            cursor.execute("set query_group to burst;")

            if is_temp:
                self._burst_read_write(cluster, cursor, vector, 2)
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Burst', 'Owned')])
            else:
                self._non_burst_read_write(cluster, cursor, vector, 2)
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Main', 'Owned')])
            cursor.execute("abort;")
            self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Undo')])
            # Since transaction doesn't is aborted, the table written is
            # Undo, thus both read and write query can still be bursted.
            cursor.execute("set query_group to burst;")
            if is_temp:
                self._burst_read_write(cluster, cursor, vector, 3)
                self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned'), ('Main', 'Undo')])
            else:
                self._non_burst_read_write(cluster, cursor, vector, 3)
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Main', 'Undo')])
            # After refresh is done, the burst cluster should have latest
            # sb_version, thus should be able to handle both read and write
            # query.
            self._start_and_wait_for_refresh(cluster)
            if is_temp:
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Burst', 'Owned')])
            else:
                self._validate_ownership_state(schema, 'dp28724', [])
            self._burst_read_write(cluster, cursor, vector, 4)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
            self._burst_read_write(cluster, cursor, vector, 5)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)

    def test_burst_read_write_and_try_burst_abort_immediately_backup_refresh(
            self, cluster, db_session, vector, is_temp):
        """
        Test: Run read-only and write queries on main cluster, then the
              subsequent read-only or write queries can not burst in
              same transaction. After transaction abort, backup and refresh
              cluster. Both read-only and write queries can burst.
        """
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_tables(db_session, vector, is_temp)
            if is_temp:
                schema = self._get_temp_table_schema(cursor, "dp28724")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("begin;")
            # Force to run on main cluster
            cursor.execute("set query_group to metrics;")
            self._non_burst_read_write(cluster, cursor, vector, 1)
            # Since write runs on main, the subsequent queries can not run
            # on burst cluster.
            cursor.execute("set query_group to burst;")
            if is_temp:
                self._burst_read_write(cluster, cursor, vector, 2)
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Burst', 'Owned')])
            else:
                self._non_burst_read_write(cluster, cursor, vector, 2)
                self._validate_ownership_state(schema, 'dp28724',
                                               [('Main', 'Owned')])
            cursor.execute("abort;")
            self._validate_ownership_state(
                    schema, 'dp28724', [('Main', 'Undo')])
            # Immediately backup and refresh.
            self._start_and_wait_for_refresh(cluster)
            # After refresh is done, the burst cluster should have latest
            # sb_version, thus should be able to handle both read and write
            # query.
            self._validate_ownership_state(schema, 'dp28724', [])
            self._burst_read_write(cluster, cursor, vector, 3)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            # Now, there is a disk-commit, so the owned by main cluster entry
            # is erased and both read and write query can burst after refresh.
            self._start_and_wait_for_refresh(cluster)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._burst_read_write(cluster, cursor, vector, 4)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
            # Takes backup and refresh one more time.
            self._start_and_wait_for_refresh(cluster)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._burst_read_write(cluster, cursor, vector, 5)
            self._validate_ownership_state(
                    schema, 'dp28724', [('Burst', 'Owned')])
            self._validate_table(cluster, schema, 'dp28724', vector.diststyle)
