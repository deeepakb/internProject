import logging
import pytest
import uuid
import getpass

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.incremental_resize.data_load_utils import build_min_max_query
from raff.system_tests.system_test_helpers import SystemTestHelper
from raff.burst.burst_temp_write import burst_user_temp_support_gucs, \
    BurstTempWrite

__all__ = [super_simulated_mode]
sys_util = SystemTestHelper()
log = logging.getLogger(__name__)

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
DISTSTYLE_QUERY = "SELECT DISTSTYLE, \"table\" FROM SVV_TABLE_INFO "\
                  "WHERE table_id='{}'::regclass::oid"
CREATE_TEMPORARY_TABLE_SQL = (
    "CREATE TEMPORARY TABLE {} as (SELECT * FROM {}.{})")
DATA_VALIDATION_TABLE_SQL = ("SELECT * FROM {}.{}_{}_validation_checksum")


@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_local_gucs(
    gucs=dict(list(burst_user_temp_support_gucs.items()) + [
        ('burst_enable_write_user_ctas', 'true'),
        ('burst_enable_write_user_temp_ctas', 'true')]))
@pytest.mark.custom_burst_gucs(
    gucs=dict(list(burst_user_temp_support_gucs.items()) + [
        ('burst_enable_write_user_ctas', 'true'),
        ('burst_enable_write_user_temp_ctas', 'true')]))
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBurstTempTableValidation(BurstTempWrite):

    def _run_data_validation(self, db_session, cluster, cursor,
                             base_table, base_table_schema,
                             clone_temp_table, clone_temp_table_schema):
        ctas_table_prefix = 'ctas_validation'
        log.info("Running data validation for {}.{} against {}.{}".format(
            clone_temp_table_schema, clone_temp_table,
            base_table_schema, base_table))
        # Create data validation tables for the base table.
        sys_util.data_validation(db_session, base_table, 'create',
                                 schema_name=base_table_schema,
                                 ctas_table_prefix=ctas_table_prefix)
        self._check_last_ctas_bursted(cluster)
        # Create data validation tables for the cloned temp table.
        sys_util.data_validation(db_session, clone_temp_table, 'create',
                                 schema_name=clone_temp_table_schema,
                                 ctas_table_prefix=ctas_table_prefix)
        self._check_last_ctas_bursted(cluster)
        # Run validation across the base table and cloned temp table.
        sys_util.compare_result_sets(
            db_session,
            DATA_VALIDATION_TABLE_SQL.format(base_table_schema,
                                             ctas_table_prefix,
                                             base_table),
            DATA_VALIDATION_TABLE_SQL.format(clone_temp_table_schema,
                                             ctas_table_prefix,
                                             clone_temp_table),
            error_string="validate_data_results")
        self._check_last_query_bursted(cluster, cursor)

    def _run_column_min_max_validation(self, db_session, cluster, cursor,
                                       base_table, base_table_schema,
                                       clone_temp_table,
                                       clone_temp_table_schema):
        # Generate the queries to fetch min/max values for table columns.
        base_table_query = build_min_max_query(db_session, base_table,
                                               end_query_stmt=False)
        clone_table_query = build_min_max_query(db_session, clone_temp_table,
                                                end_query_stmt=False)
        log.info("Base table query: {} clone table query: {}".format(
            base_table_query, clone_table_query))
        # Run validation againt min/max for the base and cloned tables.
        sys_util.compare_result_sets(db_session,
                                     base_table_query,
                                     clone_table_query,
                                     error_string="min_max_validation")
        self._check_last_query_bursted(cluster, cursor)

    def test_burst_user_temp_table_data_validation(self, cluster):
        """
        In this test, we will initially load tables from allshapes dataset.
        We will then create multiple temp tables which are clones of the tables
        from the allshapes dataset. We would then execute queries that access
        the clones temp tables, which essentially compute the crc checksum,
        min/max validation of column metadata on burst cluster and validate the
        results against the original tables.
        """
        conn_params = cluster.get_conn_params(user='master')
        db_session = DbSession(conn_params)
        # Load and fetch the list of tables from the allshapes dataset.
        with db_session.cursor() as cursor:
            base_table_schema = "public"
            cursor.execute("set query_group to burst;")
            # Create temp table clones
            (temp_table_dict, perm_table_list) = \
                self._setup_temp_all_shapes_tables(cluster, cursor,
                                                   max_tables_to_load=1)
            self._start_and_wait_for_refresh(cluster)
            # Compute CRC checksum across column values for original tables
            # as well as the cloned tables on the main and burst clusters.
            # Validate the computed CRC checksum is identical in all cases.
            # In addition, also validate min/max values across table columns.
            for base_table in perm_table_list:
                clone_temp_table = base_table + "_temp"
                # Validate crc checksum across table data.
                self._run_data_validation(db_session, cluster, cursor,
                                          base_table, base_table_schema,
                                          clone_temp_table,
                                          temp_table_dict[clone_temp_table])
                # Validate min/max values across table data columns.
                self._run_column_min_max_validation(db_session, cluster,
                                                    cursor, base_table,
                                                    base_table_schema,
                                                    clone_temp_table,
                                                    temp_table_dict[clone_temp_table])
