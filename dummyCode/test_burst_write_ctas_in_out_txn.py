# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
import uuid
import getpass


from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.data_loaders.common import load_min_all_shapes_data
from raff.burst.burst_temp_write import burst_user_temp_support_gucs, BurstTempWrite
from raff.common.db_connection import ConnectionInfo


log = logging.getLogger(__name__)

__all__ = ["super_simulated_mode"]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
DUPLICATE_AS_TEMP = "CREATE TEMP TABLE {} AS SELECT * FROM {}"
DROP_TABLE = "DROP TABLE IF EXISTS {}"
CTAS = "CREATE {} TABLE {} {} {} AS (SELECT * from {})"
SELECT_INTO = "SELECT * INTO {} {} {} {} FROM {}"
INSERT = "INSERT INTO {} SELECT * FROM {}"
DISTSTYLE_QUERY = "SELECT DISTSTYLE, \"table\" FROM SVV_TABLE_INFO "\
                  "WHERE table_id='{}'::regclass::oid"
# Randomly pick a column to use as sortkey/distkey.
RANDOM_COLUMN = """SELECT attname AS type
                    FROM   pg_attribute
                    WHERE  attrelid = '{}'::regclass
                    AND    attnum > 0
                    AND    NOT attisdropped
                    ORDER  BY RANDOM() limit 1;"""
GUCS = dict(
    slices_per_node='2',
    burst_max_idle_time_seconds='1800',
    burst_enable_write='true',
    burst_enable_write_user_ctas='true',
    burst_enable_write_user_temp_ctas='true',
    enable_burst_async_acquire='false')
SAME_SLICE_BURST_GUCS = dict(slices_per_node='2',
                             burst_enable_write='true',
                             burst_enable_write_user_ctas='true')
DIFF_SLICE_BURST_GUCS = dict(slices_per_node='4',
                             burst_enable_write='true',
                             burst_enable_write_user_ctas='true')
GUCS.update(burst_user_temp_support_gucs)


@pytest.mark.ssm_perm_or_temp_config
class TestBurstWriteCTASInTxn(BurstTempWrite):
    def _execute_ddl_and_return_result(self, cluster, cursor, base_table,
                                       is_burst, is_temp, vector):
        diststyle = ''
        if vector.diststyle and 'even' not in vector.diststyle:
            cursor.execute(RANDOM_COLUMN.format(base_table))
            rand_col1 = cursor.fetchall()[0][0]
            diststyle = vector.diststyle if 'even' in vector.diststyle \
                else vector.diststyle.format(
                    rand_col1)
        if vector.sortkey != '':
            cursor.execute(RANDOM_COLUMN.format(base_table))
            rand_col2 = cursor.fetchall()[0][0]
        if is_burst:
            ctas_table = "{}_{}_burst".format(vector.ddl_cmd, base_table)
            cursor.execute("set query_group to burst;")
        else:
            ctas_table = "{}_{}".format(vector.ddl_cmd, base_table)
            cursor.execute("set query_group to noburst;")
        cursor.execute(DROP_TABLE.format(ctas_table))
        if vector.in_txn:
            cursor.execute("begin")
        cursor.execute(
            (CTAS if vector.ddl_cmd == 'ctas' else SELECT_INTO).format(
                'TEMP'
                if is_temp else '', ctas_table, diststyle, vector.sortkey
                if 'sortkey' not in vector.sortkey else
                vector.sortkey.format(rand_col2), base_table))
        cursor.execute(INSERT.format(ctas_table, base_table))
        if vector.in_txn:
            cursor.execute("commit")
        cursor.execute(INSERT.format(ctas_table, base_table))
        if is_burst:
            self._check_last_ctas_bursted(cluster)
        else:
            self._check_last_ctas_didnt_burst(cluster)
        cursor.execute("select * from {} order by 1;".format(ctas_table))
        return [cursor.result_fetchall(), ctas_table, diststyle]

    def base_bw_ctas_in_out_txn(self, db_session, cluster, vector, is_temp):
        """
            Run ctas on burst and main
            and verify the results are the same.
        """
        if vector.ddl_cmd == 'select_into' and vector.sortkey:
            pytest.skip("Specifying distribution style and \
                            sortkey not supported for select into.")
        conn_params = cluster.get_conn_params(user='master')
        connection_info = ConnectionInfo(**conn_params)
        base_table = load_min_all_shapes_data(
            connection_info,
            force=True,
            create_unsorted_region=True,
            create_alternate_sort_tables=False,
            filter_table_list=True,
            max_tables_to_load=1  # We only need one test table.
        )[0]
        if vector.temp_referenced:
            table = 'temp_' + base_table
        else:
            table = base_table
        main_table = "{}_{}".format(vector.ddl_cmd, table)
        burst_table = "{}_{}_burst".format(vector.ddl_cmd, table)

        self._start_and_wait_for_refresh(cluster)
        # Randomly choose an all shapes table since going
        # through all of them would take too much time
        with db_session.cursor() as cursor, \
                self.db.cursor() as boot_cursor:
            schema = 'public'
            cursor.execute("SET search_path TO {}".format(schema))
            boot_cursor.execute("set search_path to {}".format(schema))
            if vector.temp_referenced:
                cursor.execute(DUPLICATE_AS_TEMP.format(table, base_table))
            burst_result, burst_table, diststyle = self._execute_ddl_and_return_result(
                cluster,
                cursor,
                table,
                True,  # ddl runs on burst
                is_temp,
                vector)
            main_result, main_table, diststyle = self._execute_ddl_and_return_result(
                cluster,
                cursor,
                table,
                False,  # ddl runs on main
                is_temp,
                vector)
            if is_temp:
                schema = self._get_temp_table_schema(cursor, burst_table)
            else:
                schema = 'public'
            # Validate table metadata.
            self._validate_table(cluster, schema, burst_table, diststyle)
            self._validate_table(cluster, schema, main_table, diststyle)
        # Table content check.
        assert burst_result == main_result
