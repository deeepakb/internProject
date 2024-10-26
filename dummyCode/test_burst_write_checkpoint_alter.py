# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest
import uuid
import logging
from raff.storage.alter_table_suite import AlterDistCheckpointBase
from raff.storage.alter_table_suite import enable_atd_checkpoint_gucs
from raff.common.db.session_context import SessionContext
from raff.common.db.session import DbSession
from raff.storage.storage_test import disable_all_autoworkers
from test_burst_write_mixed_workload import TestBurstWriteMixedWorkloadBase
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode,\
                                                         get_burst_conn_params
from raff.common.db.redshift_db import RedshiftDb
from raff.util.utils import run_bootstrap_sql

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, disable_all_autoworkers]


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '3',
        'burst_enable_write': 'true',
        'vacuum_auto_worker_enable': 'false'
    })
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.usefixtures("enable_atd_checkpoint_gucs")
@pytest.mark.usefixtures("disable_all_autoworkers")
@pytest.mark.super_simulated_no_stable_rc
@pytest.mark.skip(reason="DP-39831")
class TestATCBurstWriteTest(AlterDistCheckpointBase,
                            TestBurstWriteMixedWorkloadBase):
    force_checkpoint_mode_event = "EtAlterSimulateCheckpoint"
    CHECK_QUERY = "select count(*), sum(c0), sum(c1) from dp28532;"

    def test_burst_write_atc(self, cluster):
        '''
        Test ALTER DISTSTYLE in checkpoint mode with interleaved bursted DMLs.
        Verify the table contents reflect the bursted DMLs performed between
        the iterations.
        Setup:
        step 1: setup table
        step 2: backup and refresh, write can burst
        step 3: run iteration 1 of the alter cmd
        step 4: write cannot burst after 1st iteration of alter
        step 5: backup and refresh
        step 6: write can be bursted.
        '''
        test_schema = 'test_atc_burst_write_schema_{}'.format(
            str(uuid.uuid4().hex))
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp28532"
        cluster.set_event(self.force_checkpoint_mode_event)

        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1).cursor() as dml_cursor, \
                   DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2).cursor() as check_cursor:
            check_cursor.execute("set search_path to {}".format(test_schema))
            self._setup_table(dml_cursor, test_schema, base_tbl_name, 17)
            self._start_and_wait_for_refresh(cluster)
            self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
            # Run 1st iteration of checkpoint alter
            check_cursor.execute("alter table {} alter distkey "
                                 "c0".format(base_tbl_name))
            self._check_shadow_table(check_cursor, base_tbl_name, True)
            # Write after 1 iteration of checkpoint alter cannot burst.
            self._insert_tables_cannot_bursted(cluster, dml_cursor,
                                               base_tbl_name)
            self._copy_tables_cannot_bursted(cluster, dml_cursor,
                                             base_tbl_name)
            # Backup and refresh.
            self._start_and_wait_for_refresh(cluster)
            self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
            self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)

            # Run the remaining iterations.
            for i in range(3):
                log.info("iteration: {}".format(i))
                check_cursor.execute("alter table {} alter distkey "
                                     "c0".format(base_tbl_name))
            self.verify_table_content_and_properties(
                check_cursor, test_schema, base_tbl_name,
                [(1310777, 7209119, 7209134)], 'distkey(c0)')

    def test_burst_write_atc_immediate_backup_refresh(self, cluster):
        '''
        Test ALTER DISTSTYLE in checkpoint mode with interleaved bursted DMLs.
        Verify the table contents reflect the bursted DMLs performed between
        the iterations.
        Setup:
        step 1: setup table
        step 2: backup and refresh, write can burst
        step 3: run iteration 1 of the alter cmd
        step 4: immediately backup and refresh
        step 5: write can be bursted.
        '''
        test_schema = 'test_atc_burst_write_schema_{}'.format(
            str(uuid.uuid4().hex))
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp28532"
        cluster.set_event(self.force_checkpoint_mode_event)

        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1).cursor() as dml_cursor, \
                   DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2).cursor() as check_cursor:
            check_cursor.execute("set search_path to {}".format(test_schema))
            self._setup_table(dml_cursor, test_schema, base_tbl_name, 17)
            self._start_and_wait_for_refresh(cluster)
            self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
            # Run 1st iteration of checkpoint alter
            check_cursor.execute("alter table {} alter distkey "
                                 "c0".format(base_tbl_name))
            self._check_shadow_table(check_cursor, base_tbl_name, True)

            # Immediately backup and refresh.
            self._start_and_wait_for_refresh(cluster)
            # Write can be bursted after backup and refresh.
            self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
            self._delete_tables_bursted(cluster, dml_cursor, base_tbl_name)
            self._update_tables_2_bursted(cluster, dml_cursor, base_tbl_name)
            self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
            # Run the remaining iterations.
            for i in range(6):
                log.info("iteration: {}".format(i))
                check_cursor.execute("alter table {} alter distkey "
                                     "c0".format(base_tbl_name))

            self.verify_table_content_and_properties(
                check_cursor, test_schema, base_tbl_name,
                [(1048605, 12058746, 6815842)], 'distkey(c0)')

    def test_burst_write_atc_with_reset_checkpoint(self, cluster):
        '''
        Test ALTER DISTSTYLE in checkpoint mode with interleaved bursted DMLs.
        Verify the table contents reflect the bursted DMLs performed between
        the iterations.
        Setup:
        step 1: setup table
        step 2: backup and refresh, write can burst
        step 3: run iteration 1 of the alter cmd, check shadow table exist
        step 4: issue another alter to reset the checkpoint
        step 5: check shadow table is dropped
        step 6: write cannot burst
        step 7: backup and refresh
        step 8: write can be bursted
        step 9: restart checkpoint alter from the 1st iteration.
        '''
        test_schema = 'test_atc_burst_write_schema_{}'.format(
            str(uuid.uuid4().hex))
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp28532"
        cluster.set_event(self.force_checkpoint_mode_event)

        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1).cursor() as dml_cursor, \
                   DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2).cursor() as check_cursor:
            check_cursor.execute("set search_path to {}".format(test_schema))
            self._setup_table(dml_cursor, test_schema, base_tbl_name, 17)
            self._start_and_wait_for_refresh(cluster)
            self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
            # Run 1st iteration of checkpoint alter
            check_cursor.execute("alter table {} alter distkey "
                                 "c0".format(base_tbl_name))
            self._check_shadow_table(check_cursor, base_tbl_name, True)
            # Issue a random alter to reset checkpoint.
            check_cursor.execute(
                "alter table {} alter sortkey(c1)".format(base_tbl_name))
            self._check_shadow_table(check_cursor, base_tbl_name, False)
            # Read and write at this step cannot burst.
            self._read_cannot_bursted(cluster, dml_cursor, base_tbl_name)
            self._insert_tables_cannot_bursted(cluster, dml_cursor,
                                               base_tbl_name)
            self._copy_tables_cannot_bursted(cluster, dml_cursor,
                                             base_tbl_name)

            # Backup and refresh.
            self._start_and_wait_for_refresh(cluster)
            self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
            self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)

            # Restart from the first iteration for checkpoint alter.
            for i in range(5):
                log.info("iteration: {}".format(i))
                check_cursor.execute("alter table {} alter distkey "
                                     "c0".format(base_tbl_name))
            self.verify_table_content_and_properties(
                check_cursor, test_schema, base_tbl_name,
                [(1310777, 7209119, 7209134)], 'distkey(c0)')
