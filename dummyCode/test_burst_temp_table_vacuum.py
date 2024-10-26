# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid
import getpass

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.system_tests.system_test_helpers import SystemTestHelper
from raff.storage.alter_table_suite import AlterTableSuite
from raff.burst.burst_temp_write import burst_user_temp_support_gucs, \
    BurstTempWrite

log = logging.getLogger(__name__)
sys_util = SystemTestHelper()
__all__ = [super_simulated_mode]
SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
INSERT_SELECT = '''
    INSERT INTO {}.{} (SELECT * FROM {}.{}
    WHERE col_int_raw > 0 AND col_int_raw < 10)
    '''
INSERT_SELECT_ALL = "INSERT INTO {}.{} (SELECT * FROM {}.{});"
QUERY = "SELECT * FROM {}.{} WHERE col_int_raw > 0 AND col_int_raw < 10;"
QUERY_ALL = "SELECT * FROM {}.{};"
DELETE = "DELETE FROM {}.{} WHERE col_int_raw > 0 AND col_int_raw < 10;"
UPDATE = '''UPDATE {}.{}
            SET col_smallint_raw = 1
            WHERE col_int_raw > 0 AND col_int_raw < 10;'''

VACUUM_DELETE_CMD = "VACUUM DELETE ONLY {}.{} TO 100 PERCENT;"
VACUUM_SORT_CMD = "VACUUM SORT ONLY {}.{} TO 100 PERCENT;"
VACUUM_FULL_CMD = "VACUUM FULL {}.{} TO 100 PERCENT;"
NUM_COL_SAMPLE = [
    'col_smallint_raw', 'col_int_bytedict', 'col_bigint_delta',
    'col_decimal_1_0_delta32k', 'col_decimal_18_18_lzo', 'col_float8_zstd'
]


@pytest.mark.serial_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=burst_user_temp_support_gucs)
@pytest.mark.custom_local_gucs(gucs=burst_user_temp_support_gucs)
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstTempTableVacuum(BurstTempWrite, AlterTableSuite):
    def _do_dml_check(self, cluster, cursor, temp_table_schema, temp_table,
                      base_table_schema, base_table):
        '''
        Helper function to run READ/INSERT/UPDATE/DELETE after VACUUM
        and check query bursted.
        '''
        cursor.execute(
            QUERY_ALL.format(temp_table_schema, temp_table, base_table_schema,
                             base_table))
        self._check_last_query_bursted(cluster, cursor)
        # INSERT rows with 0 < col_int_raw < 10 from base table
        cursor.execute(
            INSERT_SELECT.format(temp_table_schema, temp_table,
                                 base_table_schema, base_table))
        self._check_last_query_bursted(cluster, cursor)

        # UPDATE rows with 0 < col_int_raw < 10
        cursor.execute(
            UPDATE.format(temp_table_schema, temp_table, base_table_schema,
                          base_table))
        self._check_last_query_bursted(cluster, cursor)

        # DELETE rows with 0 < col_int_raw < 10
        cursor.execute(
            DELETE.format(temp_table_schema, temp_table, base_table_schema,
                          base_table))
        self._check_last_query_bursted(cluster, cursor)

    def _validate_bursted_table_content_all_shapes_smallint(
            self, cursor, cluster, temp_table_schema, table_bursted):
        '''
        Helper function only used to check bursted table content
        for all_shapes_dist_key_int_sortkey_smallint table after vacuum.
        1. Get SUM of some numeric column on main cluster and burst cluster.
        2. Compare results.
        '''
        col_sum = ""
        for i in range(0, len(NUM_COL_SAMPLE)):
            col_sum += "SUM({}), ".format(NUM_COL_SAMPLE[i])
        col_sum = col_sum[:-2]
        content_check = "SELECT {} FROM {}.{}".format(
            col_sum, temp_table_schema, table_bursted)
        cursor.execute("set query_group to metrics;")
        cursor.execute(content_check)
        self._check_last_query_didnt_burst(cluster, cursor)
        res_main = cursor.fetchall()
        # Get content on burst.
        cursor.execute("set query_group to burst;")
        cursor.execute(content_check)
        self._check_last_query_bursted(cluster, cursor)
        res_burst = cursor.fetchall()
        try:
            assert res_main == res_burst
        except AssertionError:
            log.info(
                "compared content between main and burst, main: {}, burst:{}".
                format(res_main, res_burst))
            assert res_main == res_burst

    def test_burst_temp_table_vacuum(self, cluster):
        conn_params = cluster.get_conn_params(user='master')
        db_session = DbSession(conn_params)
        with db_session.cursor() as cursor:
            # Set up base and temp table
            (temp_table_dict, perm_table_list) = \
                self._setup_temp_all_shapes_tables(cluster, cursor,
                                                   max_tables_to_load=1)
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")
            # READ/WRITE temp table
            for base_table in perm_table_list:
                base_table_schema = "public"
                temp_table = base_table + "_temp"
                temp_table_schema = temp_table_dict[temp_table]
                # VACUUM SORT
                cursor.execute(
                    VACUUM_SORT_CMD.format(temp_table_schema, temp_table))
                self._check_last_query_didnt_burst(cluster, cursor)
                self._start_and_wait_for_refresh(cluster)
                self._validate_bursted_table_content_all_shapes_smallint(
                    cursor, cluster, temp_table_schema, temp_table)
                # Data validation for temp table on burst cluster
                self._validate_table(cluster, temp_table_schema, temp_table,
                                     '')
                self._do_dml_check(cluster, cursor, temp_table_schema,
                                   temp_table, base_table_schema, base_table)

                # Some rows has been deleted in _do_dml_check()
                # We can run VACUUM DELETE
                cursor.execute(
                    VACUUM_DELETE_CMD.format(temp_table_schema, temp_table))
                self._check_last_query_didnt_burst(cluster, cursor)
                # Check if rows with 0 < col_int_raw < 10 are deleted
                cursor.execute(
                    QUERY.format(temp_table_schema, temp_table,
                                 base_table_schema, base_table))
                res = cursor.fetchall()
                assert len(res) == 0
                self._check_last_query_bursted(cluster, cursor)
                self._start_and_wait_for_refresh(cluster)
                self._validate_bursted_table_content_all_shapes_smallint(
                    cursor, cluster, temp_table_schema, temp_table)
                # Data validation for temp table on burst cluster
                self._validate_table(cluster, temp_table_schema, temp_table,
                                     '')
                self._do_dml_check(cluster, cursor, temp_table_schema,
                                   temp_table, base_table_schema, base_table)

                # INSERT AND DELETE
                cursor.execute(
                    INSERT_SELECT.format(temp_table_schema, temp_table,
                                         base_table_schema, base_table))
                self._check_last_query_bursted(cluster, cursor)
                cursor.execute(DELETE.format(temp_table_schema, temp_table))
                self._check_last_query_bursted(cluster, cursor)
                # VACUUM FULL
                cursor.execute(
                    VACUUM_FULL_CMD.format(temp_table_schema, temp_table))
                self._check_last_query_didnt_burst(cluster, cursor)
                # Check if rows with 0 < col_int_raw < 10 are deleted
                cursor.execute(
                    QUERY.format(temp_table_schema, temp_table,
                                 base_table_schema, base_table))
                res = cursor.fetchall()
                assert len(res) == 0
                self._check_last_query_bursted(cluster, cursor)
                self._start_and_wait_for_refresh(cluster)
                self._validate_bursted_table_content_all_shapes_smallint(
                    cursor, cluster, temp_table_schema, temp_table)
                # Data validation for temp table on burst cluster
                self._validate_table(cluster, temp_table_schema, temp_table,
                                     '')
                self._do_dml_check(cluster, cursor, temp_table_schema,
                                   temp_table, base_table_schema, base_table)

                # INSERT rows with 0 < col_int_raw < 10 from base table
                # Check if INSERT can be bursted
                cursor.execute(
                    INSERT_SELECT.format(temp_table_schema, temp_table,
                                         base_table_schema, base_table))
                self._check_last_query_bursted(cluster, cursor)
                # Data validation for temp table on burst cluster
                self._validate_table(cluster, temp_table_schema, temp_table,
                                     '')
                # Validate table equivalence between base and temp
                tbl1 = '{}.{}'.format(temp_table_schema, temp_table)
                tbl2 = '{}.{}'.format(base_table_schema, base_table)
                self._validate_two_table_content(cursor, tbl1, tbl2)
