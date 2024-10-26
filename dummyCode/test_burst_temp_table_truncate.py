# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid
import getpass

from raff.common.dimensions import Dimensions
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.db_connection import ConnectionInfo
from raff.burst.burst_temp_write import burst_user_temp_support_gucs, \
    BurstTempWrite
from raff.data_loaders.common import load_min_all_shapes_data
from raff.system_tests.system_test_helpers import SystemTestHelper
from raff.storage.alter_table_suite import AlterTableSuite


log = logging.getLogger(__name__)
sys_util = SystemTestHelper()
__all__ = [super_simulated_mode]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
INSERT_SELECT_CMD = "INSERT INTO {}.{} (SELECT * FROM {}.{})"
INSERT_SELECT_CMD_LIMIT = "INSERT INTO {}.{} (SELECT * FROM {}.{} LIMIT 5)"
SELECT_COUNT = "SELECT COUNT(*) FROM {}.{}"
TRUNCATE = "TRUNCATE {}.{}"
DISTSTYLE_QUERY = "SELECT DISTSTYLE, \"table\" FROM SVV_TABLE_INFO "\
                  "WHERE table_id='{}'::regclass::oid"
CTAS = "CREATE TABLE {} AS (SELECT * from {}.{})"
DROP_TABLE = "DROP TABLE IF EXISTS {};"
DATA_VALIDATION_TABLE_SQL = ("SELECT * FROM {}.{}_{}_validation_checksum")


@pytest.mark.serial_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=burst_user_temp_support_gucs)
@pytest.mark.custom_local_gucs(gucs=burst_user_temp_support_gucs)
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.ssm_perm_or_temp_config
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstTempTableTruncate(BurstTempWrite, AlterTableSuite):
    def _setup_clone_perm_tables(self, cursor, schema, to_clone_table_list):
        for table in to_clone_table_list:
            log.info("Creating perm table clone for: {}.{}".format(
                schema, table))
            clone_temp_table = table + "_perm"
            cursor.execute(DROP_TABLE.format(clone_temp_table))
            cursor.execute(CTAS.format(clone_temp_table, schema, table))

    @classmethod
    def modify_test_dimensions(cls):
        """
        List of in/out of transcations
        """
        return Dimensions({"in_txn": [True, False]})

    def test_burst_temp_table_truncate(self, cluster, vector, is_temp):
        '''
        This function tests truncate on burst user temp table.
        1. Load all shape data.
        2. Create temp table by cloning all shape data.
        3. Run read/write query on temp table and check burstness.
        4. Run truncate.
        5. Run read/write query on temp table after truncate.
           and check burstness.
        '''
        conn_params = cluster.get_conn_params(user='master')
        connection_info = ConnectionInfo(**conn_params)
        db_session = DbSession(conn_params)
        # Load and fetch the list of tables from the allshapes dataset.
        to_clone_table_list = load_min_all_shapes_data(
            connection_info,
            force=True,
            create_unsorted_region=True,
            create_alternate_sort_tables=False,
            filter_table_list=True,
            max_tables_to_load=1)
        with db_session.cursor() as cursor:
            base_table_schema = "public"
            cursor.execute("set search_path to public")
            if is_temp:
                # Create temp table clones from the original tables.
                temp_table_dict = self._setup_clone_temp_tables(
                    cursor, base_table_schema, to_clone_table_list)
            else:
                # Create perm table clones from the original tables.
                self._setup_clone_perm_tables(cursor, base_table_schema,
                                              to_clone_table_list)
                self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")
            # READ/WRITE temp table
            for base_table in to_clone_table_list:
                clone_suffix = "_temp" if is_temp else "_perm"
                clone_table = base_table + clone_suffix
                clone_table_schema = temp_table_dict[clone_table] if is_temp \
                    else base_table_schema
                cursor.execute(
                    SELECT_COUNT.format(clone_table_schema, clone_table))
                res = cursor.fetchall()
                total_row = res[0][0]
                assert total_row != 0
                self._check_last_query_bursted(cluster, cursor)
                if vector.in_txn:
                    cursor.execute("BEGIN")
                    if not is_temp:
                        self._start_and_wait_for_refresh(cluster)
                cursor.execute(
                    INSERT_SELECT_CMD_LIMIT.format(
                        clone_table_schema, clone_table, base_table_schema,
                        base_table))
                cursor.execute(
                    TRUNCATE.format(clone_table_schema, clone_table))
                if vector.in_txn:
                    cursor.execute("COMMIT")
                if not is_temp:
                    self._start_and_wait_for_refresh(cluster)
                # Re-run READ/WRITE to insure truncate works
                cursor.execute(
                    SELECT_COUNT.format(clone_table_schema, clone_table))
                res = cursor.fetchall()
                assert res[0][0] == 0
                self._check_last_query_bursted(cluster, cursor)
                # Reload all shape data
                cursor.execute(
                    INSERT_SELECT_CMD.format(clone_table_schema, clone_table,
                                             base_table_schema, base_table))
                if not vector.in_txn and not is_temp:
                    self._start_and_wait_for_refresh(cluster)
                cursor.execute(
                    SELECT_COUNT.format(clone_table_schema, clone_table))
                res = cursor.fetchall()
                assert res[0][0] == total_row
                self._check_last_query_bursted(cluster, cursor)
                # Data validation for temp table on burst cluster
                self._validate_table(cluster, clone_table_schema, clone_table,
                                     '')
                tbl1 = '{}.{}'.format(clone_table_schema, clone_table)
                tbl2 = '{}.{}'.format(base_table_schema, base_table)
                self._validate_two_table_content(cursor, tbl1, tbl2)
