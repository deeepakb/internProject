# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
import uuid
import getpass

from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_temp_write import burst_user_temp_support_gucs
from raff.mv.mv_utils import get_current_timestamp, ViewUpdateStatus, \
    assert_last_mv_refresh_bursted


log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))

BURST_GUCS = dict(list(burst_user_temp_support_gucs.items()))
COMMON_LOCAL_GUCS = dict(list(BURST_GUCS.items()) +
                         [('ctas_auto_analyze', 'true'),
                          ('mv_enable_refresh_to_burst', 'true')])


class BaseBurstWriteCtasMVRefresh(BurstWriteTest):
    """
    This class contains helpers which are shared by Burst Write MV tests.
    """

    def _setup_local_table(self, cursor):
        """
        Sets up a local base table with two columns.
        """
        # Specify DISTKEY to avoid the automatic "Small table conversion"
        # which marks dependent MVs for recompute.
        CREATE_TABLE_SQL = \
            "CREATE TABLE ctas_test_table (i int, j int)" \
            " DISTSTYLE KEY DISTKEY(i)"
        INSERT_TABLE_SQL = \
            "INSERT INTO ctas_test_table" \
            " VALUES (0, 1), (0, 2), (1, 10), (1, 20)"
        cursor.execute(CREATE_TABLE_SQL)
        cursor.execute(INSERT_TABLE_SQL)

    def _update_local_table(self, cursor):
        """
        Insert tuples into local base table, created via
        - `_setup_local_table`.
        """
        cursor.execute("INSERT INTO ctas_test_table "
                       "SELECT * FROM ctas_test_table LIMIT 1")

    def _create_mv(self, cursor, mv_schema, mv_name, mv_query,
                   auto_refresh=False):
        """
        Create a MV based on the given MV query, the given MV schema, and the
        given MV name.

        Optional Args:
            auto_refresh (bool):
                When True, the MV will be configured as auto-refreshable.
        """
        CREATE_SQL = "CREATE MATERIALIZED VIEW {}.{} AUTO REFRESH {} AS {}"
        cursor.execute(CREATE_SQL.format(mv_schema, mv_name,
                                         "YES" if auto_refresh else "NO",
                                         mv_query))

    def _base_test_burst_mv_refresh_local_base_table(
            self, cluster, db_session, mv_name, mv_projection,
            expected_refresh_status):
        BASE_MV_QUERY = """
            SELECT i, {}
            FROM ctas_test_table
            GROUP BY 1
        """
        mv_schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            self._setup_local_table(cursor)
            self._create_mv(cursor, mv_schema, mv_name,
                            BASE_MV_QUERY.format(mv_projection),
                            auto_refresh=False)
            # Make MVs stale and sync Burst cluster.
            self._update_local_table(cursor)
            self._start_and_wait_for_refresh(cluster)
            # Refresh MV and verify bursting.
            starttime = get_current_timestamp(cursor)
            cursor.execute("set query_group to burst")
            cursor.execute("REFRESH MATERIALIZED VIEW {}.{}"
                           .format(mv_schema, mv_name))
            assert_last_mv_refresh_bursted(
                cluster, self.db.cursor(), mv_schema, mv_name, starttime,
                expected_refresh_status)


LOCAL_GUCS_MV_REFRESH_PERM = dict(
    list(COMMON_LOCAL_GUCS.items()) +
    [('mv_refresh_use_perm_tables', 'true')])


@pytest.mark.custom_burst_gucs(gucs=BURST_GUCS)
@pytest.mark.custom_local_gucs(gucs=LOCAL_GUCS_MV_REFRESH_PERM)
@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.super_simulated_mode
@pytest.mark.skip_load_data  # TPC-DS tables are not required.
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBurstWriteCtasMVRefreshPerm(BaseBurstWriteCtasMVRefresh):
    """
    This test class covers Burst Write CTAS queries from MV Refresh
    where staging tables (necessary for aggregates) are based on perm tables.
    """
    @pytest.mark.parametrize("mv_suffix, mv_projection",
                             [("avg", "AVG(j)"),
                              ("count_star", "COUNT(*)"),
                              ("count_column", "COUNT(j)"),
                              ("min", "MIN(j)"),
                              ("max", "MAX(j)"),
                              ("sum", "SUM(j)")])
    @pytest.mark.session_ctx(user_type='regular')
    def test_burst_write_ctas_mv_refresh_perm(
            self, cluster, db_session, mv_suffix, mv_projection):
        """
        Tests MV Refresh Bursting for CTAS queries, based on perm tables.
        MVs with different aggregate functions are tested which all require
        staging tables (CTAS) for incremental refresh.
        """
        mv_name = "mv_{}_refresh_use_perm_tables".format(mv_suffix)
        self._base_test_burst_mv_refresh_local_base_table(
            cluster, db_session, mv_name, mv_projection,
            ViewUpdateStatus.IVM_SUCCESSFUL)


LOCAL_GUCS_MV_REFRESH_TEMP = dict(
    list(COMMON_LOCAL_GUCS.items()) +
    [('mv_refresh_use_perm_tables', 'false')])


@pytest.mark.custom_burst_gucs(gucs=BURST_GUCS)
@pytest.mark.custom_local_gucs(gucs=LOCAL_GUCS_MV_REFRESH_TEMP)
@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.skip_load_data  # TPC-DS tables are not required.
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBurstWriteCtasMVRefreshTemp(BaseBurstWriteCtasMVRefresh):
    """
    This test class covers Burst Write CTAS queries from MV Refresh
    where staging tables (necessary for aggregates) are based on temp tables.
    """
    @pytest.mark.parametrize("mv_suffix, mv_projection",
                             [("avg", "AVG(j)"),
                              ("count_star", "COUNT(*)"),
                              ("count_column", "COUNT(j)"),
                              ("min", "MIN(j)"),
                              ("max", "MAX(j)"),
                              ("sum", "SUM(j)")])
    @pytest.mark.session_ctx(user_type='regular')
    def test_burst_write_ctas_mv_refresh_temp(
            self, cluster, db_session, mv_suffix, mv_projection):
        """
        Tests MV Refresh Bursting for CTAS queries, based on temp tables.
        MVs with different aggregate functions are tested which all require
        staging tables (CTAS) for incremental refresh.
        """
        mv_name = "mv_{}_refresh_use_temp_tables".format(mv_suffix)
        self._base_test_burst_mv_refresh_local_base_table(
            cluster, db_session, mv_name, mv_projection,
            ViewUpdateStatus.IVM_SUCCESSFUL)
