# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.storage.alter_table_suite import AlterTableSuite
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs={'burst_enable_write': 'true',
                                     'vacuum_auto_worker_enable': 'false'})
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.session_ctx(user_type='bootstrap')
class TestBurstWriteAlterDistSort(AlterTableSuite, BurstWriteTest):
    def _setup_tables(self, cursor, vector):
        diststyle = vector.diststyle
        sortkey = vector.sortkey
        tbl_def = ("create table dp20365_tbl(c0 int, c1 bigint) {} {}")
        tbl_def_burst = (
            "create table dp20365_tbl_burst(c0 int, c1 bigint) {} {}")
        tbl_def_commit = (
            "create table dp20365_tbl_commit(c0 int)")
        cursor.execute(tbl_def.format(diststyle, sortkey))
        cursor.execute(tbl_def_burst.format(diststyle, sortkey))
        cursor.execute(tbl_def_commit)

    def _alter_distsort(self, cursor, table, distkey, sortkey):
        alter_ddl = ("ALTER TABLE {} ALTER DISTKEY {}, "
                     "ALTER SORTKEY ({});").format(table, distkey, sortkey)
        cursor.execute("set query_group to metrics;")
        cursor.execute(alter_ddl)

    def _dmls_commit(self, cluster, cursor, iteration_num, main_table,
                              burst_table):
        basic_insert = ("insert into {}(c0, c1) values({}, 111111111);")
        basic_insert_2 = ("insert into {}(c0, c1) values({}, 22222222222);")
        # Burst query
        cursor.execute("set query_group to burst;")
        cursor.execute("begin;")
        for i in range(iteration_num):
            log.info("Burst insert iteration {} {}".format(burst_table, i))
            cursor.execute(basic_insert.format(burst_table, i))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(basic_insert_2.format(burst_table, i))
            self._check_last_query_bursted(cluster, cursor)
        cursor.execute("commit;")
        # Normal query
        cursor.execute("set query_group to metrics;")
        cursor.execute("begin;")
        for i in range(iteration_num):
            log.info("Main insert iteration {} {}".format(main_table, i))
            cursor.execute(basic_insert.format(main_table, i))
            self._check_last_query_didnt_burst(cluster, cursor)
            cursor.execute(basic_insert_2.format(main_table, i))
            self._check_last_query_didnt_burst(cluster, cursor)
        cursor.execute("commit;")

    def _dmls_abort(self, cluster, cursor, iteration_num,
                                    main_table, burst_table):
        basic_insert = ("insert into {}(c0, c1) values({}, 111111111);")
        basic_insert_2 = ("insert into {}(c0, c1) values({}, 22222222222);")
        # Normal query
        cursor.execute("set query_group to metrics;")
        for i in range(iteration_num):
            log.info("Main insert iteration {} {}".format(main_table, i))
            cursor.execute("begin;")
            cursor.execute(basic_insert.format(main_table, i))
            self._check_last_query_didnt_burst(cluster, cursor)
            cursor.execute(basic_insert_2.format(main_table, i))
            self._check_last_query_didnt_burst(cluster, cursor)
            if i % 2 == 0:
                cursor.execute("commit;")
            else:
                cursor.execute("abort;")
        # Burst query
        cursor.execute("set query_group to burst;")
        for i in range(iteration_num):
            log.info("Burst insert iteration {} {}".format(burst_table, i))
            cursor.execute("begin;")
            cursor.execute(basic_insert.format(burst_table, i))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(basic_insert_2.format(burst_table, i))
            self._check_last_query_bursted(cluster, cursor)
            if i % 2 == 0:
                cursor.execute("commit;")
            else:
                cursor.execute("abort;")
                # Trigger local commit and backup for next burst after abort
                cursor.execute("set query_group to metrics;")
                cursor.execute("insert into dp20365_tbl_commit values(1);")
                self._start_and_wait_for_refresh(cluster)
                cursor.execute("set query_group to burst;")

    def _assert_tables_diff(self, cursor, query_group, res_1, res_2):
        try:
            select_sql = "SELECT * FROM {} ORDER BY 1,2;"
            cursor.execute(select_sql.format("dp20365_tbl"))
            res = cursor.fetchall()
            log.info("{} Cluster dp20365_tbl = {}".format(
                query_group, str(res)))
            cursor.execute(select_sql.format("dp20365_tbl_burst"))
            res = cursor.fetchall()
            log.info("{} Cluster dp20365_tbl_burst = {}".format(
                query_group, str(res)))
            assert len(res_1) == 0
            assert len(res_2) == 0
            log.info("{} Cluster result check done".format(query_group))
        except AssertionError:
            log.info("{} Cluster RESULT_1: {}".format(query_group, str(res_1)))
            log.info("{} Cluster RESULT_2: {}".format(query_group, str(res_2)))
            assert len(res_1) == 0
            assert len(res_2) == 0

    def _validate_table_data(self, cluster, cursor):
        validate_stmt_1 = ("select * from dp20365_tbl except"
                           " select * from dp20365_tbl_burst order by 1,2;")
        validate_stmt_2 = ("select * from dp20365_tbl_burst except"
                           " select * from dp20365_tbl order by 1,2;")
        # Validate tables diff on main cluster
        cursor.execute("set query_group to metrics;")
        cursor.execute(validate_stmt_1)
        res_1 = cursor.fetchall()
        self._check_last_query_didnt_burst(cluster, cursor)
        cursor.execute(validate_stmt_2)
        res_2 = cursor.fetchall()
        self._check_last_query_didnt_burst(cluster, cursor)
        self._assert_tables_diff(cursor, "Main", res_1, res_2)
        # Validate tables diff on burst cluster
        self._start_and_wait_for_refresh(cluster)
        cursor.execute("set query_group to burst;")
        cursor.execute(validate_stmt_1)
        res_1 = cursor.fetchall()
        self._check_last_query_bursted(cluster, cursor)
        cursor.execute(validate_stmt_2)
        res_2 = cursor.fetchall()
        self._check_last_query_bursted(cluster, cursor)
        self._assert_tables_diff(cursor, "Burst", res_1, res_2)

    def _validate_table_properties(self, cursor, schema, table, expect_distkey):
        cursor.execute(
                'SET SEARCH_PATH TO "{}", "$user", public;'.format(schema))
        self.validate_distkey(cursor, table, expect_distkey)
        cursor.execute("xpx 'check_distribution {}';".format(table))
        # validate sortedness of table
        cursor.execute("xpx 'sortedness_checker {} 0 1';".format(table))
        # validate encoding types
        self.validate_encoding_type(cursor, schema, table)

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['diststyle even', 'distkey(c1)'],
                sortkey=['sortkey(c0, c1)']))

    def test_burst_write_alter_distsort_commit(self, cluster, db_session,
                                               vector):
        """
        Test: burst read and write on table with alter distsort.
        The test create two tables with the same identity columns.
        And then insert same data in two tables but one on main cluster and
        the other one on burst cluster. They should contain same data at last.
        1. setup tables that are ready for burst.
        2. alter diststyle/sortkey on table.
        3. backup and refresh.
        4. insert data to tables on burst and main.
        5. validate consistency of tables' content.
        6. alter diststyle/sortkey on table.
        7. validate consistency of tables' content.
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session_master.session_ctx.schema
        with db_session_master.cursor() as cursor, \
                db_session.cursor() as cursor_bs:
            cursor.execute("set query_group to burst;")
            self._setup_tables(cursor, vector)
            # alter distsort before burst write
            self._alter_distsort(cursor, "dp20365_tbl", "c0", "c0")
            self._alter_distsort(cursor, "dp20365_tbl_burst", "c0", "c0")
            self._start_and_wait_for_refresh(cluster)
            self._dmls_commit(
                cluster, cursor, 10, "dp20365_tbl", "dp20365_tbl_burst")
            self._validate_table_data(cluster, cursor)
            # alter distsort again after burst write
            self._alter_distsort(cursor, "dp20365_tbl", "c1", "c1")
            self._alter_distsort(cursor, "dp20365_tbl_burst", "c1", "c1")
            self._validate_table_data(cluster, cursor)
            self._validate_table_properties(
                cursor_bs, schema, "dp20365_tbl", "distkey(c1)")
            self._validate_table_properties(
                cursor_bs, schema, "dp20365_tbl_burst", "distkey(c1)")

    def test_burst_write_alter_distsort_interleave_commit(self, cluster,
                                                          db_session, vector):
        """
        Test: burst read and write on identity column.
        The test create two tables with the same identity columns.
        Insert data on each table interleaved on main and burst clusters.
        These tables should contain same data at last.
        1. setup tables that are ready for burst.
        2. alter diststyle/sortkey on table.
        3. backup and refresh.
        4. insert data to tables on burst and main multiple times.
        5. validate consistency of tables' content.
        6. alter diststyle/sortkey on table.
        7. validate consistency of tables' content.
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session_master.session_ctx.schema
        with db_session_master.cursor() as cursor, \
                db_session.cursor() as cursor_bs:
            cursor.execute("set query_group to burst;")
            self._setup_tables(cursor, vector)
            # alter distsort before burst write
            self._alter_distsort(cursor, "dp20365_tbl", "c0", "c0")
            self._alter_distsort(cursor, "dp20365_tbl_burst", "c0", "c0")
            # begin burst write
            self._start_and_wait_for_refresh(cluster)
            self._dmls_commit(
                cluster, cursor, 6, "dp20365_tbl", "dp20365_tbl_burst")
            self._validate_table_data(cluster, cursor)
            self._start_and_wait_for_refresh(cluster)
            self._dmls_commit(
                cluster, cursor, 6, "dp20365_tbl_burst", "dp20365_tbl")
            self._validate_table_data(cluster, cursor)
            # Alter distsort in between
            self._alter_distsort(cursor, "dp20365_tbl", "c1", "c1")
            self._alter_distsort(cursor, "dp20365_tbl_burst", "c1", "c1")
            self._start_and_wait_for_refresh(cluster)
            self._dmls_commit(
                cluster, cursor, 6, "dp20365_tbl", "dp20365_tbl_burst")
            self._validate_table_data(cluster, cursor)
            # alter distsort again after burst write
            self._alter_distsort(cursor, "dp20365_tbl", "c0", "c0")
            self._alter_distsort(cursor, "dp20365_tbl_burst", "c0", "c0")
            self._validate_table_data(cluster, cursor)
            self._validate_table_properties(
                cursor_bs, schema, "dp20365_tbl", "distkey(c0)")
            self._validate_table_properties(
                cursor_bs, schema, "dp20365_tbl_burst", "distkey(c0)")

    def test_burst_write_alter_distsort_abort(self, cluster, db_session,
                                              vector):
        """
        Test: burst read and write on identity column with some aborted txn.
        The test create two tables with the same identity columns.
        And then insert same data in two tables but one on main cluster and
        the other one on burst cluster. They should contain same data at last.
        1. setup tables that are ready for burst.
        2. alter diststyle/sortkey on table.
        3. backup and refresh.
        4. insert data to tables on burst and main clusters with abort txns.
        5. validate consistency of tables' content.
        6. alter diststyle/sortkey on table.
        7. validate consistency of tables' content.
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session_master.session_ctx.schema
        with db_session_master.cursor() as cursor, \
                db_session.cursor() as cursor_bs:
            cursor.execute("set query_group to burst;")
            self._setup_tables(cursor, vector)
            # alter distsort before burst write
            self._alter_distsort(cursor, "dp20365_tbl", "c0", "c0")
            self._alter_distsort(cursor, "dp20365_tbl_burst", "c0", "c0")
            # begin burst write
            self._start_and_wait_for_refresh(cluster)
            self._dmls_abort(
                cluster, cursor, 10, "dp20365_tbl", "dp20365_tbl_burst")
            self._validate_table_data(cluster, cursor)
            # alter distsort again after burst write
            self._alter_distsort(cursor, "dp20365_tbl", "c1", "c1")
            self._alter_distsort(cursor, "dp20365_tbl_burst", "c1", "c1")
            self._validate_table_data(cluster, cursor)
            self._validate_table_properties(
                cursor_bs, schema, "dp20365_tbl", "distkey(c1)")
            self._validate_table_properties(
                cursor_bs, schema, "dp20365_tbl_burst", "distkey(c1)")

    def test_burst_write_alter_distsort_interleave_abort(self, cluster,
                                                         db_session, vector):
        """
        Test: burst read and write on identity column.
        The test create two tables with the same identity columns.
        Insert data on each table interleaved on main and burst clusters.
        These tables should contain same data at last.
        1. setup tables that are ready for burst.
        2. alter diststyle/sortkey on table.
        3. backup and refresh.
        4. insert data to tables on burst and main clusters with abort txns
           multiple times.
        5. validate consistency of tables' content.
        6. alter diststyle/sortkey on table.
        7. validate consistency of tables' content.
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session_master.session_ctx.schema
        with db_session_master.cursor() as cursor, \
                db_session.cursor() as cursor_bs:
            cursor.execute("set query_group to burst;")
            self._setup_tables(cursor, vector)
            # alter distsort before burst write
            self._alter_distsort(cursor, "dp20365_tbl", "c0", "c0")
            self._alter_distsort(cursor, "dp20365_tbl_burst", "c0", "c0")
            # begin burst write
            self._start_and_wait_for_refresh(cluster)
            self._dmls_abort(
                cluster, cursor, 6, "dp20365_tbl", "dp20365_tbl_burst")
            self._validate_table_data(cluster, cursor)
            self._start_and_wait_for_refresh(cluster)
            self._dmls_abort(
                cluster, cursor, 6, "dp20365_tbl_burst", "dp20365_tbl")
            self._validate_table_data(cluster, cursor)
            # alter distsort in between
            self._alter_distsort(cursor, "dp20365_tbl", "c1", "c1")
            self._alter_distsort(cursor, "dp20365_tbl_burst", "c1", "c1")
            self._start_and_wait_for_refresh(cluster)
            self._dmls_abort(
                cluster, cursor, 6, "dp20365_tbl", "dp20365_tbl_burst")
            self._validate_table_data(cluster, cursor)
            # alter distsort again after burst write
            self._alter_distsort(cursor, "dp20365_tbl", "c0", "c0")
            self._alter_distsort(cursor, "dp20365_tbl_burst", "c0", "c0")
            self._validate_table_data(cluster, cursor)
            self._validate_table_properties(
                cursor_bs, schema, "dp20365_tbl", "distkey(c0)")
            self._validate_table_properties(
                cursor_bs, schema, "dp20365_tbl_burst", "distkey(c0)")
