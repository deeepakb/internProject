import logging
import pytest
import uuid
import getpass

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.burst.burst_temp_write import burst_user_temp_support_gucs, \
    BurstTempWrite

__all__ = [super_simulated_mode]
log = logging.getLogger(__name__)

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
CREATE_TEMPORARY_TABLE_SQL = (
    "CREATE TEMPORARY TABLE {} as (SELECT * FROM {}.{})")
ANALYZE_ROWS_CHECK = ("select count(*) from pg_class, pg_statistic "
                      "where pg_statistic.starelid = pg_class.oid "
                      "and relname = '{}';")
INSERT_STMT = (
    "INSERT INTO {}.{} (SELECT * from {} ORDER BY 1 LIMIT 10)"
)


@pytest.mark.serial_only
@pytest.mark.super_simulated_mode
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_burst_gucs(
         gucs=burst_user_temp_support_gucs)
@pytest.mark.custom_local_gucs(
         gucs=burst_user_temp_support_gucs)
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstTempAnalyze(BurstTempWrite):

    def _check_temp_tbl_analzye(self, cursor, schema, temp_table):
        cursor.execute("set analyze_threshold_percent to 0;")
        cursor.execute("analyze {}.{}".format(schema, temp_table))
        cursor.execute(ANALYZE_ROWS_CHECK.format(temp_table))
        return cursor.fetch_scalar()

    def test_burst_temp_analyze(self, cluster):
        """
        Test temp table with analyze
        1. Create two cloned temp tables.
        2. Analyze first temp clone after running DML on main.
        3. Analyze second temp clone after running DML on burst.
        4. Compare results from both analyze are same.
        """
        conn_params = cluster.get_conn_params(user='master')
        db_session = DbSession(conn_params)
        with db_session.cursor() as cursor, \
                self.db.cursor() as boot_cursor:
            (temp_table_dict, perm_table_list) = \
                self._setup_temp_all_shapes_tables(cluster, cursor,
                                                   max_tables_to_load=1)
            temp_table_name = list(temp_table_dict.keys())[0]
            temp_table_schema = list(temp_table_dict.values())[0]
            perm_table_name = perm_table_list[0]
            # Create temp table clone to burst DML
            cursor.execute(CREATE_TEMPORARY_TABLE_SQL.format(
                "clone_temp_table", "public", perm_table_name))
            # Run analyze for temp table with DML on main
            cursor.execute("set query_group to noburst;")
            cursor.execute(INSERT_STMT.format(temp_table_schema, temp_table_name,
                                              perm_table_name))
            self._check_last_query_didnt_burst(cluster, cursor)
            result_1 = self._check_temp_tbl_analzye(boot_cursor, temp_table_schema,
                                                    temp_table_name)

            # Run analyze for temp table with DML on burst
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")
            cursor.execute(INSERT_STMT.format(temp_table_schema, "clone_temp_table",
                                              perm_table_name))
            self._check_last_query_bursted(cluster, cursor)
            result_2 = self._check_temp_tbl_analzye(boot_cursor, temp_table_schema,
                                                    "clone_temp_table")

            assert result_1 == result_2
