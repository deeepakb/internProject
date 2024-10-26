# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
import uuid
import getpass

from raff.burst.burst_status import BurstStatus
from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_burst_gucs(
        gucs={'burst_enable_write': 'true',
              'burst_enable_write_user_ctas': 'false'})
@pytest.mark.custom_local_gucs(
        gucs={'burst_enable_write': 'true',
              'burst_enable_write_user_ctas': 'false'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteCTASDisabled(BurstWriteTest):
    def _setup_base_table(self, cursor, base_table):
        cursor.execute("CREATE TABLE {} (i int) diststyle even;"
                       .format(base_table))
        cursor.execute("INSERT INTO {} values (1), (7), (10);"
                       .format(base_table))

    def _execute_ctas(self, cursor, base_table, ctas_table):
        cursor.execute("set query_group to burst;")
        cursor.execute("""CREATE TABLE {} AS (SELECT * from {})"""
                       .format(ctas_table, base_table))

    def test_burst_write_ctas_disabled(self, cluster):
        db_session = DbSession(cluster.get_conn_params(user='master'))
        base_table = "test_table"
        ctas_table = "ctas_table"
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst")
            self._setup_base_table(cursor, base_table)
            self._start_and_wait_for_refresh(cluster)
            self._execute_ctas(cursor, base_table, ctas_table)
            self._check_last_ctas_didnt_burst_with_code(
                cluster,
                BurstStatus.burst_write_for_user_ctas_disabled)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_burst_gucs(
        gucs={'burst_enable_write': 'true',
              'burst_enable_write_user_ctas': 'true'})
@pytest.mark.custom_local_gucs(
        gucs={'burst_enable_write': 'true',
              'burst_enable_write_user_ctas': 'true'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteCTASReadDirtyTable(BurstWriteTest):
    def _setup_base_table(self, cursor, base_table):
        cursor.execute("CREATE TABLE {} (i int) diststyle even;"
                       .format(base_table))
        cursor.execute("INSERT INTO {} values (1), (7), (10);"
                       .format(base_table))

    def _execute_ctas(self, cursor, base_table, ctas_table):
        cursor.execute("set query_group to burst;")
        cursor.execute("""CREATE TABLE {} AS (SELECT * from {})"""
                       .format(ctas_table, base_table))

    def _check_burst_failure_reasons(self, cursor):
        # Get CTAS qid
        cursor.execute("""select query from \
                        stl_internal_query_details where query_cmd_type = 4 \
                        order by initial_query_time desc limit 1""")
        qid = cursor.fetch_scalar()
        cursor.execute(
            "select concurrency_scaling_status_txt from "
            "svl_query_concurrency_scaling_status "
            "where concurrency_scaling_status = 0 and "
            "query = {}".format(qid))
        status_res = cursor.fetchall()
        # Get prepare errors for CTAS query
        cursor.execute("""select btrim(error) from stl_burst_prepare \
                        where query = {}""".format(qid))
        prepare_result = cursor.fetchall()
        assert "Failed to prepare cluster" in status_res[0][0]
        assert "Clusters have not been refreshed a sb version" \
            in prepare_result[0][0]

    def test_burst_write_ctas_dirty(self, cluster):
        """
        Test that CTAS won't burst when the table to read is dirty and
        check for expected errors in prepare.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        base_table = "test_table"
        ctas_table = "ctas_table"
        with db_session.cursor() as cursor, \
                self.db.cursor() as boot_cursor:
            cursor.execute("set query_group to burst")
            self._setup_base_table(cursor, base_table)
            self._start_and_wait_for_refresh(cluster)
            # make table dirty
            cursor.execute("begin;")
            cursor.execute("insert into {} values (0),(1),(2),(3)"
                           .format(base_table))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("abort;")
            # attempt to read from dirty table in ctas
            self._execute_ctas(cursor, base_table, ctas_table)
            self._check_last_ctas_didnt_burst(cluster)
            self._check_burst_failure_reasons(boot_cursor)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_burst_gucs(
        gucs={'burst_enable_write': 'true',
              'burst_enable_write_user_ctas': 'true',
              'burst_allow_dirty_reads': 'false'})
@pytest.mark.custom_local_gucs(
        gucs={'burst_enable_write': 'true',
              'burst_enable_write_user_ctas': 'true',
              'burst_allow_dirty_reads': 'false'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteCTASReadDirtyTableAllowOff(BurstWriteTest):

    def _setup_base_table(self, cursor, base_table):
        cursor.execute("CREATE TABLE {} (i int) diststyle even;"
                       .format(base_table))
        cursor.execute("INSERT INTO {} values (1), (7), (10);"
                       .format(base_table))

    def _execute_ctas(self, cursor, base_table, ctas_table):
        cursor.execute("set query_group to burst;")
        cursor.execute("""CREATE TABLE {} AS (SELECT * from {})"""
                       .format(ctas_table, base_table))

    def test_burst_write_ctas_dirty_read_off(self, cluster):
        """
        Test that CTAS won't burst when the table to read is dirty and
        dirty reads on burst are disabled we get the expected
        concurrency scaling status.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        base_table = "test_table"
        ctas_table = "ctas_table"
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst")
            self._setup_base_table(cursor, base_table)
            self._start_and_wait_for_refresh(cluster)
            # make table dirty
            cursor.execute("begin;")
            cursor.execute("insert into {} values (0),(1),(2),(3)"
                           .format(base_table))
            self._execute_ctas(cursor, base_table, ctas_table)
            # dirty reads disabled, we expected CTAS to have cs status 11
            self._check_last_ctas_didnt_burst_with_code(cluster,
                                                        BurstStatus.
                                                        dirty_tables_accessed)
            cursor.execute("commit;")
