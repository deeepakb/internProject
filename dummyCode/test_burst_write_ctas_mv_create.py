# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
import uuid
import getpass

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_temp_write import burst_user_temp_support_gucs
from raff.common.base_test import run_priviledged_query, \
    run_priviledged_query_scalar_int
from raff.burst.burst_status import BurstStatus
from raff.data_loaders.common import create_external_schema
from raff.mv.mv_utils import get_current_timestamp, ViewUpdateStatus, \
    assert_last_mv_refresh_bursted

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
LOCAL_GUCS = \
    dict(list(burst_user_temp_support_gucs.items()) + [(
        'ctas_auto_analyze', 'true'), ('mv_enable_refresh_to_burst', 'true')])
BURST_GUCS = dict(list(burst_user_temp_support_gucs.items()))


@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_local_gucs(gucs=LOCAL_GUCS)
@pytest.mark.custom_burst_gucs(gucs=BURST_GUCS)
@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.skip_load_data  # TPC-DS tables are not required.
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteCtasMVCreate(BurstWriteTest):
    def _setup_local_table(self, cursor):
        CREATE_TABLE_SQL = \
            "CREATE TABLE ctas_test_table (i int) diststyle key distkey(i)"
        INSERT_TABLE_SQL = \
            "INSERT INTO ctas_test_table values (1), (7), (10)"
        cursor.execute(CREATE_TABLE_SQL)
        cursor.execute(INSERT_TABLE_SQL)

    def _assert_create_mv_burst_status(self, cluster, create_mv_sql):
        # Determine xid and queries of the the completed Create MV transaction.
        SELECT_MOST_RECENT_XID_SQL = \
            "SELECT xid FROM STL_QUERY WHERE querytxt ILIKE '{}%' "\
            "ORDER BY starttime DESC LIMIT 1"
        create_mv_xid = run_priviledged_query_scalar_int(
            cluster, self.db.cursor(),
            SELECT_MOST_RECENT_XID_SQL.format(create_mv_sql))
        SELECT_QUERIES_BY_XID_SQL = \
            "SELECT query, trim(querytxt) as qtxt," \
            " concurrency_scaling_status " \
            "FROM STL_QUERY WHERE xid={}"
        queries = run_priviledged_query(
            cluster, self.db.cursor(),
            SELECT_QUERIES_BY_XID_SQL.format(create_mv_xid))
        assert len(queries) > 0, \
            "MV Create did not run any queries, given xid='{}'" \
            .format(create_mv_xid)
        # Verify the BurstStatus of each query.
        for i, query_tuple in enumerate(queries):
            query_id = query_tuple[0]
            query_text = query_tuple[1]
            query_burst_status = query_tuple[2]
            log.info("CREATE MV Query {} - BurstStatus {} - '{}'".format(
                query_id, query_burst_status, query_text))
            # Determine expected BurstStatus, and verify.
            expected_burst_status = \
                BurstStatus.burst_write_for_mv_ctas_disabled
            if "padb_fetch_sample" in query_text:
                # TODO(menzler): Remove this block once we have support for
                # for Burst CTAS ANALYZE.
                expected_burst_status = \
                    BurstStatus.in_eligible_query_type
            elif "pg_s3" in query_text:
                # As part of the `CREATE` Spectrum MV transaction, a private
                # metadata table is created in the `pg_s3` schema, using
                # a `CREATE TABLE` DDL query. This elif clause catches the
                # cascading `SELECT` and `INSERT` queries, which target the
                # private metadata table. Those queries don't burst because
                # Burst clusters are not aware of the private metadata table.
                # Even a Burst Refresh in between would not help because it is
                # `CommitBased`, and the `CREATE` Spectrum MV transaction is
                # still in flight.
                expected_burst_status = \
                    BurstStatus.scheduled_on_main
            assert query_burst_status == expected_burst_status, \
                "CREATE MV Query {} - BurstStatus {}, but {} was expected - " \
                "'{}'".format(query_id, query_burst_status,
                              expected_burst_status, query_text)

    def test_create_mv(self, cluster, db_session):
        """
        Tests whether the queries from CREATE MV burst as expected, given a
        local base table. Also, it is verified if MV Refresh queries burst
        after modifying the base table.
        """
        MV_NAME = "mv_TestBurstWriteCtasMVCreate"
        with db_session.cursor() as cursor, self.db.cursor() as bs_cursor:
            schema = db_session.session_ctx.schema
            self._setup_local_table(cursor)
            # 1) Create MV.
            cursor.execute("set query_group to burst")
            create_mv_sql = \
                "CREATE MATERIALIZED VIEW {}.{}" \
                " AUTO REFRESH NO AS SELECT * FROM ctas_test_table" \
                .format(schema, MV_NAME)
            cursor.execute(create_mv_sql)
            # Determine xid and verify if the queries have bursted as expected.
            self._assert_create_mv_burst_status(cluster, create_mv_sql)
            # 2) Make the MV stale and sync Burst cluster.
            cursor.execute("reset query_group")
            cursor.execute("INSERT INTO ctas_test_table values (2), (8), (11)")
            self._start_and_wait_for_refresh(cluster)
            # 3) Refresh MV and verify if it bursted.
            starttime = get_current_timestamp(cursor)
            cursor.execute("set query_group to burst")
            refresh_mv_sql = \
                "REFRESH MATERIALIZED VIEW {}.{}".format(schema, MV_NAME)
            cursor.execute(refresh_mv_sql)
            assert_last_mv_refresh_bursted(
                cluster, bs_cursor, schema, MV_NAME, starttime,
                ViewUpdateStatus.IVM_SUCCESSFUL)

    def test_create_mv_for_spectrum_table(self, cluster, db_session):
        """
        Tests whether the queries from CREATE MV burst as expected, given an
        external Spectrum base table. Also, it is verified if MV Refresh
        queries burst.
        """
        MV_NAME = "mv_spectrum_static_TestBurstWriteCtasMVSpectrumCreate"
        EXT_SCHEMA = "s3"
        EXT_TABLE_NAME = "alltypes_parquet"
        with db_session.cursor() as cursor, self.db.cursor() as bs_cursor:
            schema = db_session.session_ctx.schema
            # Setup external schema for accessing Spectrum tables.
            create_external_schema(db_session.connection_info)
            # 1) Create Spectrum MV.
            cursor.execute("set query_group to burst")
            create_mv_sql = \
                "CREATE MATERIALIZED VIEW {}.{}" \
                " AUTO REFRESH NO AS SELECT * FROM {}.{}" \
                .format(schema, MV_NAME, EXT_SCHEMA, EXT_TABLE_NAME)
            cursor.execute(create_mv_sql)
            # Determine xid and verify if the queries have bursted as expected.
            self._assert_create_mv_burst_status(cluster, create_mv_sql)
            # 2) Sync Burst Cluster, so that Burst knows about new tables.
            self._start_and_wait_for_refresh(cluster)
            # 3) Refresh the Spectrum MV and verify if all queries bursted.
            #    Hint: This test covers a Spectrum base table which remains
            #    static. Nevertheless, MV Refresh will run and issue queries
            #    for refreshing the private metadata table of the Spectrum MV.
            starttime = get_current_timestamp(cursor)
            refresh_mv_sql = \
                "REFRESH MATERIALIZED VIEW {}.{}".format(schema, MV_NAME)
            cursor.execute(refresh_mv_sql)
            assert_last_mv_refresh_bursted(
                cluster, bs_cursor, schema, MV_NAME, starttime,
                ViewUpdateStatus.IVM_SUCCESSFUL)
