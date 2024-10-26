# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
import getpass
import uuid
import time

from raff.common.db.session import DbSession
from raff.storage.storage_test import create_thread
from raff.storage.alter_table_suite import AlterTableSuite
from raff.burst.burst_temp_write import burst_user_temp_support_gucs, \
     BurstTempWrite
from raff.burst.burst_super_simulated_mode_helper import (
    super_simulated_mode, touch_spinfile, rm_spinfile,
    wait_until_burst_process_blocks)

__all__ = [super_simulated_mode]
log = logging.getLogger(__name__)

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
SMALL_TABLE_1 = """create {} table if not exists {} (
            a integer,
            b integer,
            primary key(a)
            ) distkey(a);
            """

INSERT = """INSERT INTO {}
                  SELECT a FROM {}
                  LIMIT 1
                  ;"""

READ = """SELECT a
          FROM {}
          LIMIT 1
          ;"""

BURST_POKE_NODES_SPIN_FILE = "burst_ctas:after_burst_poke_nodes"

NUM_BLOCKS = "select sum(block_count) from stv_tbl_perm where name='{}';"

UPDATE = "update {} set {} = {} where {} in (select {}.a from {});"

DELETE = "delete from {} where {} in (select {}.b from {});"


@pytest.mark.serial_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=burst_user_temp_support_gucs)
@pytest.mark.custom_local_gucs(gucs=burst_user_temp_support_gucs)
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstTempConcurrentRefresh(BurstTempWrite, AlterTableSuite):
    def _setup_table(self, cursor, table_type, is_temp, tbl_name):
        if is_temp:
            cursor.execute(table_type.format('temp', tbl_name))
        else:
            cursor.execute(table_type.format('', tbl_name))

    def _read_query(self, cursor):
        cursor.execute("set query_group to burst")
        cursor.execute(READ.format('t'))

    """
        FT24: Testing burst temp table query with a concurrent refresh
        This test will:
        1. Create a temp table and perm table, inject data into both
        2. Add a spinner at the desired location in code
        3. Burst read from the temp table
        4. Start a burst refresh and wait for it to finish
        5. Remove the spinner and let the burst read query finish
        6. Verify the burst query ran successfully
    """

    def test_temp_concurrent_refresh(self, cluster):
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor:
            # Create tables, inject data
            cursor.execute("set query_group to burst")
            self._setup_table(cursor, SMALL_TABLE_1, False, 'p')
            self._setup_table(cursor, SMALL_TABLE_1, True, 't')

            # Run query to acquire burst cluster
            cursor.execute(READ.format('t'))

            cursor.execute(INSERT.format('p', 'p'))
            cursor.execute(INSERT.format('t', 't'))

            with create_thread(self._read_query, (cursor,)) as read_thread, \
                 create_thread(self._start_and_wait_for_refresh, (cluster,)) as\
                 refresh_thread:

                # Add spinner
                touch_spinfile(BURST_POKE_NODES_SPIN_FILE)
                time.sleep(10)
                read_thread.start()
                wait_until_burst_process_blocks(BURST_POKE_NODES_SPIN_FILE)
                # Start a thread which executes a burst refresh
                refresh_thread.start()

                # Release the spinner, let burst query finish
                rm_spinfile(BURST_POKE_NODES_SPIN_FILE)
                refresh_thread.join()
                read_thread.join()

                # Verify that the burst query ran successfully
                self._check_last_query_bursted(cluster, cursor)
