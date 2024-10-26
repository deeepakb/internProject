# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.common.dimensions import Dimensions
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.burst.burst_temp_write import burst_user_temp_support_gucs, \
    BurstTempWrite
from raff.burst.burst_write import burst_write_mv_gucs
from test_burst_mv_refresh import TestBurstWriteMVBase
from test_burst_mv_refresh import MV_QUERIES

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

DROP_TABLE = "DROP TABLE IF EXISTS {};"
CREATE_TEMPORARY_TABLE_SQL = (
    "CREATE TEMPORARY TABLE {} as (SELECT * FROM {}.{})")
INSERT_SELECT_CMD = "INSERT INTO {}.{} SELECT * FROM {}.{}"
INSERT_CMD = "INSERT INTO {}.{} VALUES {}"
DELETE_CMD = "DELETE FROM {}.{}"
UPDATE_CMD_1 = "UPDATE {}.{} SET c0 = c0 + 2;"
UPDATE_CMD_2 = "UPDATE {}.{} SET c0 = 'new';"
REFRESH_MV = "REFRESH MATERIALIZED VIEW {}"
MY_SCHEMA = "test_schema"
MY_USER = "test_user"

# If a distkey/sortkey is unspecified, then padb picks distall & auto sortkey
# by default. This info doesn't appear in svv_table_info until the first insert
# sql on the table.
diststyle = ['distkey(c0)']
# keep the sortkey c0 else some create-mv fail with this error
# err: sortkey c1 must be included in the select list
sortkey = ['sortkey(c0)']


@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.custom_local_gucs(gucs=burst_user_temp_support_gucs)
@pytest.mark.custom_burst_gucs(
    gucs=list(burst_write_mv_gucs.items()) +
    list(burst_user_temp_support_gucs.items()) + [('slices_per_node', '3')])
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.serial_only
@pytest.mark.skip_load_data
class TestBurstTempTableMV(TestBurstWriteMVBase, BurstTempWrite):
    def _create_temp_mv(self, cursor, mvs, suffix):
        '''
        Create temp table by cloning MV.
        Return: 1. list of temp tables.
        2. dict of <key: temp table : value: temp table schema>
        '''
        temp_table_dict = {}
        temp_table_list = []
        for mv in mvs:
            temp_table = mv + "_temp" + suffix
            cursor.execute(DROP_TABLE.format(temp_table))
            cursor.execute(
                CREATE_TEMPORARY_TABLE_SQL.format(temp_table, MY_SCHEMA, mv))
            temp_tbl_schema = \
                self._get_temp_table_schema(cursor, temp_table)
            temp_table_dict[temp_table] = temp_tbl_schema
            temp_table_list.append(temp_table)
        return (temp_table_list, temp_table_dict)

    def _do_sql_on_main_and_burst(self, cluster, cursor, stmt_no_burst,
                                  stmt_burst):
        '''
        Run Insert/Update/Delete on 2 temp tables tbl_noburst and tbl_burst.
        Queries on tbl_burst should be bursted and tbl_noburst should not burst.
        Check burstness and validate dat in temp tables.
        stmt_no_burst: query running on tbl_noburst
        stmt_burst: query running on tbl_burstt
        '''
        cursor.execute("set query_group to burst;")
        cursor.execute(stmt_burst)
        self._check_last_query_bursted(cluster, cursor)
        cursor.execute("set query_group to metrics;")
        cursor.execute(stmt_no_burst)
        self._check_last_query_didnt_burst(cluster, cursor)

    def _idu_on_base_table(self, cursor, mv_configs):
        '''
        Helper function to run delete, insert and update on base
        table and refresh MV.
        '''
        base_tables = mv_configs.tables
        mvs = mv_configs.mvs
        cursor.execute("set search_path to {}".format(MY_SCHEMA))
        cursor.execute("set query_group to burst")
        cursor.execute("BEGIN;")
        for table in base_tables:
            cursor.execute(DELETE_CMD.format(MY_SCHEMA, table))
            cursor.execute(INSERT_CMD.format(MY_SCHEMA, table, self._values()))
            cursor.execute(UPDATE_CMD_1.format(MY_SCHEMA, table))
        for mv in mvs:
            cursor.execute(REFRESH_MV.format(mv))

    def _idu_on_mv_temp(self, cluster, cursor, base_mvs, in_txn):
        '''
        Helper function to run IDU on temp tables. This function
        creates temp table by cloning MV, and run delete, insert
        and update within/without transaction block.
        base_mvs: MVs created from base tables.
        in_txn: within/without transaction block
        '''
        # Create temp table from MV on burst and main
        cursor.execute("set search_path to public;")
        temp_table_list_burst, temp_table_dict_burst = self._create_temp_mv(
            cursor, base_mvs, "_burst")
        temp_table_list_noburst, temp_table_dict_noburst = self._create_temp_mv(
            cursor, base_mvs, "_noburst")
        cursor.execute("set query_group to burst")
        self._start_and_wait_for_refresh(cluster)
        if in_txn:
            cursor.execute("BEGIN")
        # DELETE

        for i in range(len(base_mvs)):
            stmt_no_burst = DELETE_CMD.format(
                temp_table_dict_noburst[temp_table_list_noburst[i]],
                temp_table_list_noburst[i])
            stmt_burst = DELETE_CMD.format(
                temp_table_dict_burst[temp_table_list_burst[i]],
                temp_table_list_burst[i])
            self._do_sql_on_main_and_burst(cluster, cursor, stmt_no_burst,
                                           stmt_burst)

        # INSERT
        for i in range(len(base_mvs)):
            stmt_no_burst = INSERT_SELECT_CMD.format(
                temp_table_dict_noburst[temp_table_list_noburst[i]],
                temp_table_list_noburst[i], MY_SCHEMA, base_mvs[i])
            stmt_burst = INSERT_SELECT_CMD.format(
                temp_table_dict_burst[temp_table_list_burst[i]],
                temp_table_list_burst[i], MY_SCHEMA, base_mvs[i])
            self._do_sql_on_main_and_burst(cluster, cursor, stmt_no_burst,
                                           stmt_burst)

        # UPDATE
        for i in range(len(base_mvs)):
            if i < 8 or i == 10:
                stmt_no_burst = UPDATE_CMD_1.format(
                    temp_table_dict_noburst[temp_table_list_noburst[i]],
                    temp_table_list_noburst[i])
                stmt_burst = UPDATE_CMD_1.format(
                    temp_table_dict_burst[temp_table_list_burst[i]],
                    temp_table_list_burst[i])
            else:
                stmt_no_burst = UPDATE_CMD_2.format(
                    temp_table_dict_noburst[temp_table_list_noburst[i]],
                    temp_table_list_noburst[i])
                stmt_burst = UPDATE_CMD_2.format(
                    temp_table_dict_burst[temp_table_list_burst[i]],
                    temp_table_list_burst[i])
            self._do_sql_on_main_and_burst(cluster, cursor, stmt_no_burst,
                                           stmt_burst)

        cursor.execute("COMMIT")

        for i in range(len(base_mvs)):
            # Validate main table and burst table have same content.
            self._validate_two_table_content(
                cursor, temp_table_list_noburst[i], temp_table_list_noburst[i])
            # Data validation for temp table on burst cluster.
            self._validate_table(
                cluster, temp_table_dict_noburst[temp_table_list_noburst[i]],
                temp_table_list_noburst[i], diststyle[0])

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(dict(in_txn=[True, False]))

    def test_burst_temp_table_mv(self, cluster, vector):
        '''
        This function test temp tables from MV.
        1. Create temp tables by cloning MV.
        2. Run IDU on temp tables within/without transcation block,
        check burstness and validate data.
        3. IDU on base table and refresh MVs.
        4. Repeat step 2.
        '''
        relprefix = "ddl"
        session = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))
        cursor = session.cursor()
        with self.setup_views_with_shared_tables(
                cluster, cursor, relprefix, diststyle, sortkey, MV_QUERIES,
                False) as mv_configs:
            base_mvs = mv_configs.mvs

            # Craete temp table on MV, do Insert/Delete/Update on temp table
            self._idu_on_mv_temp(cluster, cursor, base_mvs, vector.in_txn)
            # Do Insert/Delete/Update on base table, refresh MV,
            # then repeat steps above
            self._idu_on_base_table(cursor, mv_configs)
            self._idu_on_mv_temp(cluster, cursor, base_mvs, vector.in_txn)
