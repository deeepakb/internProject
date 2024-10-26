# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid

from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions
from base_burst_alter_distkey import (BaseBurstAlterDistkey,
                                      ENABLE_BURST_ATO_NO_PERSIST_SHADOW)
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_super_simulated_mode_helper import SsmXenGuard
from raff.common.db.db_exception import NotSupportedError
from raff.storage.storage_test import create_thread
from psycopg2.extensions import QueryCanceledError
from raff.common.db.db_exception import InternalError, OperationalError

__all__ = ["super_simulated_mode"]
log = logging.getLogger(__name__)

SNAPSHOT_IDENTIFIER = "burst-init-{}".format(str(uuid.uuid4().hex))

SEARCH_PATH = 'SET SEARCH_PATH TO "{}", "$user", public;'


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=ENABLE_BURST_ATO_NO_PERSIST_SHADOW)
@pytest.mark.custom_local_gucs(gucs=ENABLE_BURST_ATO_NO_PERSIST_SHADOW)
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBurstATOSignal(BaseBurstAlterDistkey):
    CHECK_QUERY = "select count(*), sum(c_custkey), sum(c_nationkey) from {}.{}"
    _SCHEMA = ''
    backend_pid = 0

    def verify_table_content(self, cursor, res):
        cmd = self.CHECK_QUERY.format(self._SCHEMA, self._TBL_NAME)
        cursor.execute(cmd)
        assert cursor.fetchall() == res

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                guard_pos=[
                    'burst_alter_controller', 'burst_before_alter_init',
                    'burst_after_alter_copy', 'burst_alter_hdr'
                ],
                cancel_cmd=[
                    'select pg_cancel_backend({});',
                ]))

    def do_background(self, cursor, schema, cmd):
        cursor.execute(SEARCH_PATH.format(schema))
        try:
            cursor.execute("select pg_backend_pid();")
            self.backend_pid = cursor.fetch_scalar()
            cursor.execute(cmd)
        except QueryCanceledError as e:
            assert "cancelled" in str(e).splitlines()[0].strip()
        except InternalError as e:
            assert "cancelled" in str(e).splitlines()[0].strip()
        except OperationalError as e:
            assert "terminating connection" in \
                    str(e).splitlines()[0].strip()

    def insert_verify_bursted_and_get_table_state(self, cursor, cluster):
        self._start_and_wait_for_refresh(cluster)
        self.insert_and_verify_bursted(cluster, cursor)
        cursor.execute(self.CHECK_QUERY.format(self._SCHEMA, self._TBL_NAME))
        res_gold = cursor.fetchall()
        return res_gold

    def test_ato_bursting_signal(self, cluster, vector):
        """
        This test create an ATO target table and burst the first checkpoint
        Then
        1. Pause the second checkpoint in burst cluster
        2. send pg_cancel signal to the process in the burst cluster running alter
        3. Retry the ATO to make sure it finishes on burst or main
        4. Verify table content.
        """
        xen_guard = SsmXenGuard(guard_name=vector.guard_pos)
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session_master.session_ctx.schema
        self._SCHEMA = schema
        self._setup_dist_auto_even_table(db_session_master)
        alter_cmd = "alter table {} alter distkey {}".format(
            self._TBL_NAME, "c_custkey")
        retry_burst = True if 'burst_after_alter_copy' or 'burst_alter_hdr' \
            in vector.guard_pos else False
        with db_session_master.cursor() as cursor:
            # Need to burst a query because cold start only check backup version
            res_gold = self.insert_verify_bursted_and_get_table_state(
                cursor, cluster)
            self._simulate_alter_checkpoint_mode(cluster, cursor)
            # Create a checkpoint alter in burst cluster.
            cursor.execute(alter_cmd)
            self._verify_last_alter_query_bursted(cluster)
            xen_guard.enable()
            with create_thread(self.do_background,
                               (cursor, schema, alter_cmd)) as thread:
                thread.start()
                xen_guard.wait_until_process_blocks(timeout_secs=300)
                self.db.cursor().execute(
                    vector.cancel_cmd.format(self.backend_pid))
            xen_guard.cleanup()
            if retry_burst:
                # some guard we wanna still try to burst following alter
                res_gold = self.insert_verify_bursted_and_get_table_state(
                    cursor, cluster)
            try:
                for i in range(10):
                    cursor.execute("alter table {} alter distkey {}".format(
                        self._TBL_NAME, "c_custkey"))
                    if retry_burst and i < 4:
                        # Verify at least some alter copy queries bursted.
                        self._verify_last_alter_query_bursted(cluster)
                    elif retry_burst is False:
                        self._verify_last_alter_query_not_bursted(cluster)
            except NotSupportedError as error:
                assert 'cannot alter to the same distkey' in error.pgerror
                pass
            self._unset_checkpoint_mode(cluster, cursor)
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute("set search_path to {}".format(schema))
            self.verify_table_content_and_properties(bootstrap_cursor, schema,
                                                     self._TBL_NAME, res_gold,
                                                     'auto(key(c_custkey))')
