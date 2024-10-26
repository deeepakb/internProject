# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
import uuid
import getpass

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.storage.alter_table_suite import AlterTableSuite
from raff.storage.storage_test import create_thread
from raff.common.dimensions import Dimensions

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
INSERT_QUERY = (
    "insert into {}.{} values (5,'Customer#000000005',"
    "'hwBtxkoBF qSW4KrIk5U 2B1AU7H',3,'13-750-942-6364',794.47,'HOUSEHOLD',"
    "'blithely final ins');")
SELECT_QUERY = ("SELECT c_custkey from {}.{} where c_custkey = 5;")
DELETE_QUERY = "DELETE FROM {}.{} where c_custkey = 5"
VACUUM = "VACUUM {} {} TO 100 PERCENT;"


@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.serial_only
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_burst_gucs(gucs={
    'burst_enable_write': 'true',
    'enable_burst_s3_commit_based_refresh': 'true'})
@pytest.mark.custom_local_gucs(
    gucs={
        'enable_burst_s3_commit_based_refresh': 'true',
        'enable_burst_s3_commit_based_cold_start': 'false'
    })
class TestBurstCBRVacuum(BurstWriteTest, AlterTableSuite):

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(guard_pos=["vacuum:internal_commit",
                            "vacuum:get_write_lock",
                            "vacuum:incremental_delete::start",
                            "vacuum:begin_incremental_sort"])
        )

    def verify_table_content(self, cursor, res):
        cmd = "select count(*), sum(c_custkey), sum(c_nationkey) \
               from customer_burst;"
        cursor.execute(cmd)
        assert cursor.fetchall() == res

    def _execute_burst_read_query(self, cursor, schema):
        cursor.execute("set selective_dispatch_level = 0;")
        cursor.execute("set enable_result_cache_for_session to off;")
        cursor.execute(SELECT_QUERY.format(schema, "customer_burst"))

    def _execute_burst_write_query(self, cursor, schema):
        cursor.execute("set selective_dispatch_level = 0;")
        cursor.execute("set enable_result_cache_for_session to off;")
        cursor.execute(INSERT_QUERY.format(schema, "customer_burst"))

    def _vacuum_thread(self, cursor):
        cursor.execute("VACUUM customer_burst TO 100 PERCENT;")

    @pytest.mark.parametrize("vacuum_cmd", ['SORT ONLY', 'DELETE ONLY', ''])
    def test_burst_cbr_sort_delete_full(self, cluster, vacuum_cmd):
        """
        This test verifies that refresh can be triggered after sort, delete and
        full vacuum is executed. Also, it verifies that read and write queries
        can be bursted after the refresh and table contents are as expected.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        self.execute_test_file("create_burst_write_tables", session=db_session)
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor, \
                self.db.cursor() as boot_cursor:
            cursor.execute(DELETE_QUERY.format(schema, "customer_burst"))
            self._check_last_query_didnt_burst(cluster, cursor)
            cursor.execute("set query_group to burst;")
            cursor.execute(VACUUM.format(vacuum_cmd, "customer_burst"))
            self._execute_burst_read_query(cursor, schema)
            self._check_last_query_didnt_burst(cluster, cursor)
            self._execute_burst_write_query(cursor, schema)
            self._check_last_query_didnt_burst(cluster, cursor)
            self._start_and_wait_for_refresh(cluster)
            self._execute_burst_read_query(cursor, schema)
            self._check_last_query_bursted(cluster, cursor)
            self._execute_burst_write_query(cursor, schema)
            self._check_last_query_bursted(cluster, cursor)
            boot_cursor.execute("SET search_path TO {};".format(schema))
            self.verify_table_content_and_properties(
                boot_cursor, schema, "customer_burst", [(156, 11392, 1709)],
                'diststyle auto')
            self._validate_ownership_state(
                schema, "customer_burst",
                [('Burst', 'Owned'), ('Main', 'Structure Changed')])

    def test_burst_cbr_during_vacuum(self, cluster, vector):
        db_session = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session.session_ctx.schema
        with db_session.cursor() as cursor, \
                self.db.cursor() as boot_cursor:
            cursor.execute("set query_group to burst;")
            self.execute_test_file("create_burst_write_tables", session=db_session)
            cursor.execute(DELETE_QUERY.format(schema, "customer_burst"))
            xen_guard = self.create_xen_guard(vector.guard_pos)
            with create_thread(self._vacuum_thread, (cursor, )) as thread, \
                    xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks(timeout_secs=120)
                self._start_and_wait_for_refresh(cluster)
            self._execute_burst_read_query(cursor, schema)
            self._check_last_query_bursted(cluster, cursor)
            self._execute_burst_write_query(cursor, schema)
            self._check_last_query_didnt_burst(cluster, cursor)
            self._start_and_wait_for_refresh(cluster)
            self._execute_burst_write_query(cursor, schema)
            self._check_last_query_bursted(cluster, cursor)
            self._execute_burst_read_query(cursor, schema)
            boot_cursor.execute("SET search_path TO {};".format(schema))
            self.verify_table_content_and_properties(
                boot_cursor, schema, "customer_burst", [(156, 11392, 1709)],
                'diststyle auto')
            self._validate_ownership_state(
                schema, "customer_burst",
                [('Burst', 'Owned'), ('Main', 'Structure Changed')])
