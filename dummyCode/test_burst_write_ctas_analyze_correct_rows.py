# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import getpass
import logging
import uuid
import pytest

from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext

__all__ = [super_simulated_mode]
log = logging.getLogger(__name__)

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))


@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBurstWriteCtasAnalyzeCorrectRows(BurstWriteTest):
    def _setup_tables(self, cursor):
        # We make the table we insert to DISTSTYLE all so
        # the INSERT query will not burst.
        cursor.execute("CREATE TABLE t1 (a int) DISTSTYLE ALL")
        cursor.execute("CREATE TABLE base_table (a int)")
        cursor.execute(
            "INSERT INTO base_table VALUES (1), (2), (3), (4), (5), (6)")

    def _get_latest_row_count_analyzed(self, cursor):
        cursor.execute(
            "SELECT rows FROM stl_analyze where is_background = 'f' "
            "ORDER BY starttime LIMIT 1")
        return cursor.fetchall()[0][0]

    def _get_last_ctas_xid(self, cursor):
        cursor.execute(
            "SELECT xid FROM stl_internal_query_details WHERE "
            "query_cmd_type=4 ORDER BY initial_query_time DESC limit 1")
        return cursor.fetch_scalar()

    def _check_skipped_row_count_fetch(self, cursor):
        # Check that ANALYZE skipped the row count fetch after CTAS.
        ctas_xid = self._get_last_ctas_xid(cursor)
        query = "SELECT count(*) FROM stl_query WHERE xid={} and " \
                "userid>1 AND querytxt ilike " \
                "'%padb_fetch_sample: select count(*) from%'"
        cursor.execute(query.format(ctas_xid))
        assert cursor.fetch_scalar() == 0

    def _run_single_ctas_on_burst(self, cursor):
        cursor.execute("DROP TABLE IF EXISTS ctas_2")
        cursor.execute("SET query_group TO burst")
        cursor.execute("CREATE TABLE ctas_2 AS SELECT * FROM base_table")

    def _get_reltuples(self, cursor):
        cursor.execute(
            "SELECT reltuples FROM pg_class where relname like '%ctas_2%'")
        return cursor.fetch_scalar()

    def _check_analyze_did_not_run(self, cursor):
        # Check ANALYZE did not run after latest CTAS.
        ctas_xid = self._get_last_ctas_xid(cursor)
        cursor.execute(
            "SELECT count(*) FROM stl_query WHERE xid={}".format(ctas_xid))
        # Only query in this xid should be the CTAS.
        assert cursor.fetch_scalar() == 1

    def _refresh_and_run_statement(self, cluster, cursor):
        self._start_and_wait_for_refresh(cluster)
        cursor.execute("SET query_group TO burst")
        cursor.execute(
            "INSERT INTO t1 VALUES (1); CREATE TABLE ctas_1 AS SELECT * FROM base_table"
        )

    def test_burst_write_ctas_analyze_correct_rows(self, cluster):
        ctx = SessionContext(schema='public')
        db_session = DbSession(
            cluster.get_conn_params(user='master'), session_ctx=ctx)
        with db_session.cursor() as cursor, \
                self.db.cursor() as boot_cursor:
            self._setup_tables(cursor)
            # Run Burst CTAS in multi-command statement. Verify ANALYZE uses
            # the correct row count and also did not run a row count fetch.
            self._refresh_and_run_statement(cluster, cursor)
            self._check_last_ctas_bursted(cluster)
            expected_row_count = self._get_latest_row_count_analyzed(
                boot_cursor)
            assert expected_row_count == 6
            self._check_skipped_row_count_fetch(boot_cursor)
            # Run Burst CTAS in a lone statement. Verify ANALYZE uses
            # the correct row count and also did not run a row count fetch.
            self._run_single_ctas_on_burst(cursor)
            self._check_last_ctas_bursted(cluster)
            expected_row_count = self._get_latest_row_count_analyzed(
                boot_cursor)
            assert expected_row_count == 6
            self._check_skipped_row_count_fetch(boot_cursor)
            # Run lone CTAS with CTAS ANALYZE disabled. Verify reltupes
            # were updated.
            boot_cursor.execute("SET ctas_auto_analyze TO false")
            boot_cursor.execute("SET SESSION_AUTHORIZATION TO master")
            self._run_single_ctas_on_burst(boot_cursor)
            boot_cursor.execute("RESET SESSION_AUTHORIZATION")
            self._check_last_ctas_bursted(cluster)
            self._check_analyze_did_not_run(boot_cursor)
            assert self._get_reltuples(boot_cursor) == 6
