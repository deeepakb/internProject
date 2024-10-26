# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest

from raff.common.base_test import BaseTest
from raff.common.base_test import run_priviledged_query_scalar,\
    run_priviledged_query_scalar_int
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext

CREATE_DIST_EVEN = "CREATE TEMP TABLE dist_even (col1 int) diststyle even"
CREATE_DIST_KEY = "CREATE TEMP TABLE dist_key (col1 int) diststyle " \
                  "key distkey (col1)"
CREATE_DIST_ALL = "CREATE TEMP TABLE dist_all (col1 int) diststyle all"
CREATE_DIST_AUTO = "CREATE TEMP TABLE dist_auto (col1 int)"

INSERT = "INSERT INTO {} VALUES ({})"
GET_ID = "SELECT DISTINCT id FROM stv_tbl_perm WHERE id = '{}'::regclass::oid"
STV_BLOCK_COUNT = "SELECT SUM(block_count) FROM stv_tbl_perm " \
                  "WHERE id = '{}'::regclass::oid"
STV_DIST_STYLE = "SELECT DISTINCT dist_style FROM stv_tbl_perm " \
                 "WHERE id = '{}'::regclass::oid"
STL_BLOCK_COUNT = "SELECT SUM(num_blocks) FROM stl_tbl_perm_at_drop WHERE " \
                  "table_id = {}"
STL_DIST_STYLE = "SELECT DISTINCT dist_style FROM stl_tbl_perm_at_drop " \
                 "WHERE table_id = {}"
STL_TABLE_NAME = "SELECT DISTINCT btrim(name) FROM stl_tbl_perm_at_drop " \
                 "WHERE table_id = {}"

dist_dict = {"dist_even", "dist_key", "dist_all", "dist_auto"}


@pytest.mark.session_ctx(user_type='bootstrap')
@pytest.mark.localhost_only
@pytest.mark.serial_only
class TestSessionID(BaseTest):
    def _insert_values(self, cursor, dist_style):
        """
        Insert 100 values into the tables
        """
        for val in range(100):
            cursor.execute(INSERT.format(dist_style, val))

    def _setup_temp_tables(self, cursor):
        """
        Create temp tables with 4 different distribution styles EVEN, KEY,
        ALL and AUTO. Also insert values into the tables
        """
        cursor.execute(CREATE_DIST_EVEN)
        cursor.execute(CREATE_DIST_KEY)
        cursor.execute(CREATE_DIST_ALL)
        cursor.execute(CREATE_DIST_AUTO)

        for dist_style in list(dist_dict):
            self._insert_values(cursor, dist_style)

    def _get_stv_table_data(self, cursor, cluster):
        """
        Query the stv_tbl_perm to get the table id, block count and dist_style
        to verify with the entry in the stl_tbl_perm_at_drop
        """
        table_id_in_stv = []
        block_count_in_stv = []
        dist_style_in_stv = []
        for dist_style in list(dist_dict):
            table_id_in_stv.append(
                run_priviledged_query_scalar_int(cluster, cursor,
                                                 GET_ID.format(dist_style)))
            block_count_in_stv.append(
                run_priviledged_query_scalar_int(
                    cluster, cursor, STV_BLOCK_COUNT.format(dist_style)))
            dist_style_in_stv.append(
                run_priviledged_query_scalar_int(
                    cluster, cursor, STV_DIST_STYLE.format(dist_style)))
        return table_id_in_stv, block_count_in_stv, dist_style_in_stv

    def _drop_temp_tables(self, cursor):
        """
        Drop all the temp tables
        """
        for dist_style in list(dist_dict):
            cursor.execute("DROP TABLE IF EXISTS {}".format(dist_style))

    def test_stl_tbl_perm_at_drop_fields(self, cluster):
        """
        Test if the stl_tbl_perm_at_drop logs the user-created temp table id,
        block count and distribution style when they are dropped. Testing two
        cases here:
        1. User drops temp table
        2. The session closes and the temp tables are dropped automatically.
        """
        """
        Case - 1 User Drops temp table
        """
        conn_params = cluster.get_conn_params()
        session_ctx1 = SessionContext(user_type='bootstrap')

        with DbSession(conn_params, session_ctx=session_ctx1) as session1:
            with session1.cursor() as cursor:
                self._setup_temp_tables(cursor)
                table_id_in_stv, block_count_expected, dist_style_expected \
                    = self._get_stv_table_data(cursor, cluster)
                self._drop_temp_tables(cursor)
                for i, dist_style in enumerate(dist_dict):
                    verify_count_in_stl = run_priviledged_query_scalar_int(
                        cluster, cursor,
                        STL_BLOCK_COUNT.format(table_id_in_stv[i]))
                    assert verify_count_in_stl == block_count_expected[i]
                    verify_dist_style_in_stl = run_priviledged_query_scalar_int(
                        cluster, cursor,
                        STL_DIST_STYLE.format(table_id_in_stv[i]))
                    assert verify_dist_style_in_stl == dist_style_expected[i]
                    verify_table_name = run_priviledged_query_scalar(
                        cluster, cursor,
                        STL_TABLE_NAME.format(table_id_in_stv[i]))
                    assert verify_table_name == dist_style
                """
                Case 2 - The session closes and the temp tables are
                dropped automatically.
                """
                self._setup_temp_tables(cursor)
                table_id_in_stv, block_count_expected, dist_style_expected \
                    = self._get_stv_table_data(cursor, cluster)
            session1.close()
        with DbSession(conn_params, session_ctx=session_ctx1) as session2:
            with session2.cursor() as cursor2:
                for i, dist_style in enumerate(dist_dict):
                    verify_count_in_stl = run_priviledged_query_scalar_int(
                        cluster, cursor2,
                        STL_BLOCK_COUNT.format(table_id_in_stv[i]))
                    assert verify_count_in_stl == block_count_expected[i]
                    verift_dist_style_in_stl = run_priviledged_query_scalar_int(
                        cluster, cursor2,
                        STL_DIST_STYLE.format(table_id_in_stv[i]))
                    assert verift_dist_style_in_stl == dist_style_expected[i]
                    verify_table_name = run_priviledged_query_scalar(
                        cluster, cursor2,
                        STL_TABLE_NAME.format(table_id_in_stv[i]))
                    assert verify_table_name == dist_style