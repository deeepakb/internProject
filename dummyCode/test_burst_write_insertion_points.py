import time
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode,\
        get_burst_conn_params
from raff.common.db.redshift_db import RedshiftDb
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.storage.storage_test import create_thread
from raff.storage.storage_test import disable_all_autoworkers

__all__ = [disable_all_autoworkers, super_simulated_mode]


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode", "disable_all_autoworkers")
@pytest.mark.custom_burst_gucs(
    gucs={
        'burst_enable_write': 'true',
        'burst_enable_write_copy': 'true',
        'burst_enable_write_insert': 'true',
        'enable_burst_failure_handling': 'true',
        # avoid invariant xcheck because of a known issue RedshiftDP-58107
        'map_xinvariant_to_xcheck': 'false',
    })
class TestBurstWriteInsertionPoints(BurstWriteTest):
    def _write_query(self, cursor, table_name, cluster):
        try:
            cursor.execute("INSERT INTO {} VALUES(42)".format(table_name))
        except Exception:
            pass

    @pytest.mark.skip(reason="rsqa-13330")
    def test_burst_write_unexpected_append_ips_bug(self, cluster):
        # Check for RedshiftDP-58999 bug occurance
        # If the test passed - bug is fixed

        db_session = DbSession(cluster.get_conn_params(user='master'))
        db_session_2 = DbSession(cluster.get_conn_params(user='master'))
        burst_db = RedshiftDb(conn_params=get_burst_conn_params())
        with burst_db.cursor() as burst_cursor:
            burst_cursor.execute(
                "xpx 'event set EtInsPtsReleaseTooSlow'")

        table_name = "PUBLIC.t_TestBurstWriteSSModeBug"
        with db_session_2.cursor() as cursor:
            cursor.execute("BEGIN")

        with db_session.cursor() as cursor:
            cursor.execute("CREATE TABLE IF NOT EXISTS {}(a int)\
                    diststyle even".format(table_name))
            cursor.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN\
                    SCHEMA PUBLIC TO {}".format(db_session_2.session_ctx.username))

        self._start_and_wait_for_refresh(cluster)

        with db_session.cursor() as cursor:
            with create_thread(self._write_query,
                               (cursor, table_name, cluster)) as thread:
                thread.start()
                with self.db.cursor() as bootstrap_cursor:
                    while bootstrap_cursor.execute_scalar("select count(*) from\
                            stv_inflight where text like '%INSERT%'") != 2:
                        time.sleep(1)
                    pid = bootstrap_cursor.execute_scalar("select pid from stv_inflight\
                            where text like '%INSERT%' ORDER BY starttime LIMIT 1")

                    bootstrap_cursor.execute(
                        "select pg_cancel_backend({})".format(str(pid)))

                    # Wait until query is aborted
                    while bootstrap_cursor.execute_scalar("select count(*) from\
                            stl_query where aborted = 1 and pid = {}".format(pid)) != 1:
                        time.sleep(1)

                    aborted_tx = bootstrap_cursor.execute_scalar("select xid from\
                            stl_query where aborted = 1 and pid = {}".format(pid))

                    # Wait until query is undone, so that UNDO does not come up
                    # after the refresh on line 86 which will make query on line 90
                    # unburstable
                    while bootstrap_cursor.execute_scalar("select count(*) from\
                            stl_undone where \
                            xact_id_undone = {}".format(aborted_tx)) != 1:
                        time.sleep(1)

                    self._start_and_wait_for_refresh(cluster)

                    with db_session_2.cursor() as cursor_2:
                        # Crash here if bug is not fixed
                        self._write_query(cursor_2, table_name, cluster)
                        self._check_last_query_bursted(cluster, cursor_2)
