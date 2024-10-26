# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions
from raff.storage.storage_test import create_thread
from psycopg2.extensions import QueryCanceledError
from raff.burst.burst_temp_write import BurstTempWrite

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.ssm_perm_or_temp_config
@pytest.mark.super_simulated_precommit
@pytest.mark.custom_burst_gucs(
        gucs={'slices_per_node': '3', 'burst_enable_write': 'true'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstCopySSModeInterrupt(BurstTempWrite):
    background_pid = 0

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['even'],
                sortkey=[''],
                guard_pos=[
                    'burst:execute_query:perform_request',
                    'burst:internal_fetch:perform_request',
                    'burst_write:dispatch_headers',
                    'burst_write:finish_dispatch_headers',
                    'burst_write:send_snap_in', 'burst_write:finish_snap_in'
                ]))

    def _do_copy(self, cursor):
        cursor.execute('select pg_backend_pid()')
        self.background_pid = cursor.fetch_scalar()
        log.info("pid: {}".format(self.background_pid))
        try:
            cursor.run_copy(
                'catalog_returns_burst',
                's3://tpc-h/tpc-ds/1/catalog_returns.',
                gzip=True,
                delimiter="|")
            assert False, "expect failed copy"
        except QueryCanceledError as e:
            pass
        log.info("background copy done")

    def test_burst_copy_ss_mode_interrupt(self, cluster, vector, is_temp):
        """
        Test: interrupt burst copy on different position.
        """

        db_session = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            self._setup_table(db_session, schema, 'catalog_returns', 'tpcds',
                              '1', vector.diststyle, vector.sortkey, '_burst',
                              is_temp=is_temp)
            if is_temp:
                schema = self._get_temp_table_schema(cursor, "catalog_returns_burst")
            self._start_and_wait_for_refresh(cluster)

            cursor.execute("select count(*) from catalog_returns_burst;")
            catalog_returns_size = cursor.fetch_scalar()
            self._check_last_query_bursted(cluster, cursor)

            xen_guard = self._create_xen_guard(vector.guard_pos)
            with create_thread(self._do_copy, (cursor, )) as thread, xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks()
                with self.db.cursor() as bootstrap_cursor:
                    cancel_sql = "select pg_cancel_backend({})".format(
                        self.background_pid)
                    log.info("cancel_sql: {}".format(cancel_sql))
                    bootstrap_cursor.execute(cancel_sql)
                    self._has_empty_burst_write_state(cluster, bootstrap_cursor, schema,
                                                      'catalog_returns_burst')
            # The aborted burst write should prevent subsequential query from
            # burst; temp tables don't take part in refresh
            cursor.execute("select count(*) from catalog_returns_burst;")
            assert cursor.fetch_scalar() == catalog_returns_size
            if is_temp:
                self._check_last_query_bursted(cluster, cursor)
            else:
                self._check_last_query_didnt_burst(cluster, cursor)
            cursor.run_copy(
                'catalog_returns_burst',
                's3://tpc-h/tpc-ds/1/catalog_returns.',
                gzip=True,
                delimiter="|")
            if is_temp:
                self._check_last_copy_bursted(cluster, cursor)
            else:
                self._check_last_copy_didnt_burst(cluster, cursor)
            cursor.execute("select count(*) from catalog_returns_burst;")
            assert cursor.fetch_scalar() == catalog_returns_size * 2
            self._validate_table(cluster, schema, 'catalog_returns_burst',
                                 vector.diststyle)
