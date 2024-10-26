# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.common.db.session import DbSession
from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_super_simulated_mode_helper import (
    super_simulated_mode, super_simulated_load_min_all_shapes)
import raff.burst.remote_exec_helpers as helpers

__all__ = ["super_simulated_mode"]
log = logging.getLogger(__name__)

INSERT_SELECT = ("insert into {} select * from {} limit 1;")
VACUUM_ALL = "VACUUM {};"
DELETE = "delete from {} where oid::bigint % 10 > 0"


class BaseBWDuringUncommittedTxn(BurstWriteTest):
    def _setup_table(self, cluster):
        table_list = super_simulated_load_min_all_shapes(
            cluster, max_tables_to_load=1)
        with self.db.cursor() as bs_cursor:
            for table in table_list:
                bs_cursor.execute(DELETE.format(table))
        return table_list

    def _trigger_abort_txn(self, cursor, query):
        cursor.execute("set query_group to noburst;")
        cursor.execute("BEGIN;")
        cursor.execute(query)
        cursor.execute("ABORT;")

    def _trigger_abort_vacuum(self, query):
        sql_set_vacuum_run_abort = ("xpx 'event set "
                                    "EtAbortVacuumRunnerAfterVacuumRun';")
        sql_unset_vacuum_run_abort = ("xpx 'event unset "
                                      "EtAbortVacuumRunnerAfterVacuumRun';")
        with self.db.cursor() as bs_cursor:
            bs_cursor.execute(sql_set_vacuum_run_abort)
            try:
                bs_cursor.execute(query)
            except Exception as e:
                log.info("{} failed with exception: {}".format(query, e))
                assert "Simulating vacuum failure" in str(e)
            bs_cursor.execute(sql_unset_vacuum_run_abort)

    def _run_burst_query(self, cluster, cursor, should_burst, tbl):
        cursor.execute("set query_group to burst;")
        cursor.execute(INSERT_SELECT.format(tbl, tbl))
        if should_burst:
            self._check_last_query_bursted(cluster, cursor)
        else:
            self._check_last_query_didnt_burst(cluster, cursor)

    def base_burst_write_during_uncommitted_txn(self, cluster):
        """
        Test steps:
        1. Create all shapes table set.
        2. Trigger aborted write txn and run burst write query.
        3. Trigger aborted VACUUM.
        4. Run a burst write query and it should be able to run on burst cluster
           if sync_undo is enabled.
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        with db_session_master.cursor() as cursor:
            cursor.execute("set query_group to burst;")
            # step 1
            table_list = self._setup_table(cluster)
            log.info("Test table list: {}".format(table_list))
            for table in table_list:
                log.info("running test on table: {}".format(table))
                cursor.execute(INSERT_SELECT.format(table, table))
                self._check_last_query_bursted(cluster, cursor)
                # step 2
                self._trigger_abort_txn(cursor,
                                        INSERT_SELECT.format(table, table))
                log.info("running burst query on table: {}".format(table))
                self._run_burst_query(cluster, cursor, True, table)
                # step 3
                self._trigger_abort_vacuum(VACUUM_ALL.format(table))
                # step 4
                log.info("running burst query on table: {}".format(table))
                self._run_burst_query(cluster, cursor, True, table)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.no_jdbc
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(
    gucs=dict(helpers.burst_unified_remote_exec_gucs_burst(), **{
        'udf_start_lxc': 'false'
    }))
@pytest.mark.custom_local_gucs(
    gucs=dict(helpers.burst_unified_remote_exec_gucs_main()))
@pytest.mark.usefixtures("super_simulated_mode")
class TestBWDuringUncommittedTxnSyncUndo(BaseBWDuringUncommittedTxn):
    def test_bw_during_uncommitted_txn_sync_undo(self, cluster):
        self.base_burst_write_during_uncommitted_txn(cluster)
