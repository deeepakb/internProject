# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import logging
import time

from raff.common.db.session import DbSession
from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.storage.storage_test import create_thread
from raff.burst.burst_super_simulated_mode_helper import SsmXenGuard
from psycopg2.extensions import QueryCanceledError
from raff.common.db.db_exception import InternalError, OperationalError
from six.moves import range

__all__ = ["super_simulated_mode"]
log = logging.getLogger(__name__)

BASIC_INSERT = ("insert into dp61292_tbl values (1,1);")
SEARCH_PATH = 'SET SEARCH_PATH TO "{}", "$user", public;'


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.no_jdbc
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(
    gucs={
        'burst_enable_write': 'true',
        'enable_burst_failure_handling': 'true',
        'burst_enable_insert_failure_handling': 'true',
        # avoid invariant xcheck because of a known issue RedshiftDP-58107
        'map_xinvariant_to_xcheck': 'false'
    })
@pytest.mark.custom_local_gucs(
    gucs={
        'burst_enable_write': 'true',
        'enable_burst_failure_handling': 'true',
        'burst_enable_insert_failure_handling': 'true',
        'always_burst_eligible_query': 'false',
        # avoid invariant xcheck because of a known issue RedshiftDP-58107
        'map_xinvariant_to_xcheck': 'false'
    })
@pytest.mark.session_ctx(user_type='bootstrap')
@pytest.mark.usefixtures("super_simulated_mode")
class TestBWCancelBeforeSnapin(BurstWriteTest):
    def do_background(self, cursor, schema, cmd):
        cursor.execute(SEARCH_PATH.format(schema))
        try:
            cursor.execute("select pg_backend_pid();")
            self.backend_pid = cursor.fetch_scalar()
            cursor.execute(cmd)
        except QueryCanceledError as e:
            log.info("BG query failed: {}".format(e))
            assert "cancelled" in str(e).splitlines()[0].strip()
        except InternalError as e:
            log.info("BG query failed: {}".format(e))
            assert "cancelled" in str(e).splitlines()[0].strip()
        except OperationalError as e:
            log.info("BG query failed: {}".format(e))
            assert "terminating connection" in \
                    str(e).splitlines()[0].strip()

    def verify_table_content(self, cursor, res):
        cmd = "select count(*), sum(c0), sum(c1) from dp61292_tbl;"
        cursor.execute(cmd)
        assert cursor.fetchall() == res

    def _setup_table(self, cursor):
        tbl_def = "create table public.dp61292_tbl(c0 int, c1 int) diststyle even;"
        cursor.execute(tbl_def)
        cursor.execute("GRANT ALL PRIVILEGES ON {} TO {}".format(
            "public.dp61292_tbl", "public"))

    def test_bw_cancel_before_snapin(self, cluster, db_session):
        """
        1. A burst write query aborted on a empty table before burst
           write snapin step.
        2. The same burst cluster was refreshed after several commit.
        3. Run burst write query on the same undo table and burst cluster.
        4. Burst write query should not assert and validate table content.
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session_master.session_ctx.schema
        db_session_2 = DbSession(cluster.get_conn_params(user='master'))
        burst_xen_guard = SsmXenGuard(guard_name="after_stl_insert_log")
        with db_session_master.cursor() as cursor, \
                db_session_2.cursor() as cursor2, \
                db_session.cursor() as cursor_bootstrap:
            self._setup_table(cursor)
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")
            burst_xen_guard.enable()
            with create_thread(self.do_background,
                               (cursor, schema, BASIC_INSERT)) as thread:
                thread.start()
                log.info("waiting for burst xen guard")
                burst_xen_guard.wait_until_process_blocks(timeout_secs=300)
                cancel_cmd = 'select pg_cancel_backend({});'.format(
                    self.backend_pid)
                log.info(cancel_cmd)
                cursor_bootstrap.execute(cancel_cmd)
                # Wait until no burst is marked on the undo table
                undo_mark_check = ("select count(*) "
                    "from stl_burst_write_query_event "
                    "where tbl='dp61292_tbl'::regclass::oid and undo_sb > 0 "
                    "and event ilike '%Marking as No Burst%';")
                timeout = 100
                while True:
                    cursor_bootstrap.execute(undo_mark_check)
                    res = cursor_bootstrap.fetch_scalar()
                    if res > 0:
                        break
                    timeout = timeout - 1
                    assert timeout > 0, "Time out for waiting undo query"
                    time.sleep(1)
                log.info("start to bump sb version")
                for i in range(3):
                    cursor_bootstrap.execute("xpx 'hello';")
                burst_xen_guard.cleanup()
                self._start_and_wait_for_refresh(cluster)
            # Trigger burst write on dirty table on burst cluster
            cursor2.execute("set query_group to burst;")
            cursor2.execute(BASIC_INSERT)
            self.check_last_query_bursted(cluster, cursor2)

            # validate table content
            self.verify_table_content(cursor, [(1, 1, 1, ),])
            cursor.execute("drop table dp61292_tbl;")
