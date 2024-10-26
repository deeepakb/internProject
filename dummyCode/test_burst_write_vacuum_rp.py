# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode,\
                                                         get_burst_conn_params
from raff.common.db.redshift_db import RedshiftDb
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.common.dimensions import Dimensions
from raff.common.host_type import HostType
from raff.util.utils import run_bootstrap_sql
from raff.burst.burst_test import get_burst_cluster_name
from raff.common.burst_helper import (get_cluster_by_identifier,
                                      get_cluster_by_arn, identifier_from_arn)
from test_burst_write_mixed_workload import TestBurstWriteMixedWorkloadBase


log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(gucs={'burst_enable_write': 'true'})
@pytest.mark.custom_local_gucs(
    gucs={
        'vacuum_range_partitioning_enable': 'true',
        'vacuum_auto_worker_enable': 'false',
        'auto_analyze': 'false',
        'vacuum_part_size_multiplier': '0.01',
        'vacuum_delete_columnar_num_columns': '2',
        'vacuum_auto_sort_enable': 'true'
    })
class TestBurstWriteWithVacuumRP(TestBurstWriteMixedWorkloadBase):
    def _do_vacuum_rp(self, cluster, cursor, tbl):
        # Only the last xpx modify_table_partition couldn't trigger vacuum rp.
        cursor.execute("xpx 'modify_table_partition {} 2 1 3';".format(tbl))
        cursor.execute("xpx 'modify_table_partition {} 2 1 0';".format(tbl))
        cursor.execute("xpx 'modify_table_partition {} 2 1 1';".format(tbl))
        cursor.execute("xpx 'event set EtVacuumForceRangePartitionMode';")
        cursor.execute("vacuum {}".format(tbl))
        # vacuum query does not burst
        self._check_last_query_didnt_burst(cluster, cursor)
        cursor.execute("xpx 'event unset EtVacuumForceRangePartitionMode';")

    def test_burst_write_vacuum_rp_commit(self, cluster):
        """
        Test:
        begin;
        bursted write;
        commit;
        vacuum rp;
        write cannot be bursted;
        refresh;
        write can be bursted;
        """
        test_schema = 'test_vacuum_rp_schema_{}'.format(str(uuid.uuid4().hex))
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp28558"
        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1:
            dml_cursor = session1.cursor()
            with DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2:
                check_cursor = session2.cursor()
                check_cursor.execute(
                    "set search_path to {}".format(test_schema))
                dml_cursor.execute("set query_group to metrics")
                self.execute_test_file(
                    "create_tables_burst_write_vacuum_rp", session=session1)
                self._start_and_wait_for_refresh(cluster)
                dml_cursor.execute('begin')
                # write can burst
                self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
                dml_cursor.execute('commit')
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Burst', 'Owned')])
                self._do_vacuum_rp(cluster, check_cursor, base_tbl_name)
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Burst', 'Owned'),
                                                ('Main', 'Structure Changed')])
                # write cannot burst after vacuum rp
                self._insert_tables_cannot_bursted(cluster, dml_cursor,
                                                   base_tbl_name)
                self._copy_tables_cannot_bursted(cluster, dml_cursor,
                                                 base_tbl_name)
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Main', 'Structure Changed')])
                # refresh
                self._start_and_wait_for_refresh(cluster)
                # write can burst
                self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Burst', 'Owned'),
                                                ('Main', 'Structure Changed')])
                # validate
                self._validate_table(cluster, test_schema, "dp28558",
                                     'diststyle auto')

    def test_burst_write_vacuum_rp_abort(self, cluster):
        """
        Test:
        begin;
        bursted write;
        abort;
        vacuum rp;
        write cannot be bursted;
        refresh;
        write can be bursted;
        """
        test_schema = 'test_vacuum_rp_schema_{}'.format(str(uuid.uuid4().hex))
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp28558"
        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1:
            dml_cursor = session1.cursor()
            with DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2:
                check_cursor = session2.cursor()
                check_cursor.execute(
                    "set search_path to {}".format(test_schema))
                dml_cursor.execute("set query_group to metrics")
                self.execute_test_file(
                    "create_tables_burst_write_vacuum_rp", session=session1)
                self._start_and_wait_for_refresh(cluster)
                dml_cursor.execute('begin')
                # write can burst
                self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Burst', 'Owned')])
                dml_cursor.execute('abort')
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Main', 'Undo')])
                self._do_vacuum_rp(cluster, check_cursor, base_tbl_name)
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Main', 'Structure Changed'),
                                                ('Main', 'Undo')])
                # write cannot burst after vacuum rp
                self._insert_tables_cannot_bursted(cluster, dml_cursor,
                                                   base_tbl_name)
                self._copy_tables_cannot_bursted(cluster, dml_cursor,
                                                 base_tbl_name)
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Main', 'Structure Changed'),
                                                ('Main', 'Undo')])
                # refresh
                self._start_and_wait_for_refresh(cluster)
                # write can burst
                self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Burst', 'Owned'),
                                                ('Main', 'Structure Changed')])
                # validate
                self._validate_table(cluster, test_schema, "dp28558",
                                     'diststyle auto')

    def test_burst_write_vacuum_rp_commit_immediate_refresh(self, cluster):
        """
        Test:
        begin;
        bursted write;
        commit;
        vacuum rp;
        refresh;
        write can be bursted;
        """
        test_schema = 'test_vacuum_rp_schema_{}'.format(str(uuid.uuid4().hex))
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp28558"
        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1:
            dml_cursor = session1.cursor()
            with DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2:
                check_cursor = session2.cursor()
                check_cursor.execute(
                    "set search_path to {}".format(test_schema))
                dml_cursor.execute("set query_group to metrics")
                self.execute_test_file(
                    "create_tables_burst_write_vacuum_rp", session=session1)
                self._start_and_wait_for_refresh(cluster)
                dml_cursor.execute('begin')
                # write can burst
                self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
                dml_cursor.execute('commit')
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Burst', 'Owned')])
                self._do_vacuum_rp(cluster, check_cursor, base_tbl_name)
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Burst', 'Owned'),
                                                ('Main', 'Structure Changed')])
                # refresh
                self._start_and_wait_for_refresh(cluster)
                # write can burst
                self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Burst', 'Owned'),
                                                ('Main', 'Structure Changed')])
                # validate
                self._validate_table(cluster, test_schema, "dp28558",
                                     'diststyle auto')

    def test_burst_write_vacuum_rp_abort_immediate_refresh(self, cluster):
        """
        Test:
        begin;
        bursted write;
        abort;
        vacuum rp;
        refresh;
        write can be bursted;
        """
        test_schema = 'test_vacuum_rp_schema_{}'.format(str(uuid.uuid4().hex))
        session_ctx1 = SessionContext(schema=test_schema)
        session_ctx2 = SessionContext(user_type='bootstrap')
        base_tbl_name = "dp28558"
        with DbSession(
                cluster.get_conn_params(user='master'),
                session_ctx=session_ctx1) as session1:
            dml_cursor = session1.cursor()
            with DbSession(
                    cluster.get_conn_params(),
                    session_ctx=session_ctx2) as session2:
                check_cursor = session2.cursor()
                check_cursor.execute(
                    "set search_path to {}".format(test_schema))
                dml_cursor.execute("set query_group to metrics")
                self.execute_test_file(
                    "create_tables_burst_write_vacuum_rp", session=session1)
                self._start_and_wait_for_refresh(cluster)
                dml_cursor.execute('begin')
                # write can burst
                self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Burst', 'Owned')])
                dml_cursor.execute('abort')
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Main', 'Undo')])
                self._do_vacuum_rp(cluster, check_cursor, base_tbl_name)
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Main', 'Structure Changed'),
                                                ('Main', 'Undo')])
                # refresh
                self._start_and_wait_for_refresh(cluster)
                # write can burst
                self._insert_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._copy_tables_bursted(cluster, dml_cursor, base_tbl_name)
                self._validate_ownership_state(test_schema, base_tbl_name,
                                               [('Burst', 'Owned'),
                                                ('Main', 'Structure Changed')])
                # validate
                self._validate_table(cluster, test_schema, "dp28558",
                                     'diststyle auto')
