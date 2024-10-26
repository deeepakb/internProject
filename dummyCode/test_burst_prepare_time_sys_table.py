# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved

import logging
import pytest
import random
import threading
import datetime
from time import sleep

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(gucs={
    'burst_enable_write': 'true',
    'enable_burst_failure_handling': 'true'
})
class TestBurstWritePrepareTimeSysTable(BurstWriteTest):
    def burst_insert_query_and_get_qid(self, cluster, cursor):
        self._start_and_wait_for_refresh(cluster)
        cursor.execute("set query_group to burst;")
        random_int = random.randint(0, 100)
        cursor.execute(
            "INSERT INTO test_table VALUES ({});".format(random_int))
        cursor.execute("SELECT pg_last_query_id();")
        result = cursor.fetchone()
        if result is not None:
            qid = int(result[0])
        else:
            raise Exception("No burst write qid found.")
        return qid

    def burst_unset_force_unsuitable_event(self, cluster):
        sleep(15)
        cluster.unset_event('EtForceUnsuitableBurst')
        return

    def test_burst_prepare_time_sys_table(self, cluster):
        """
        This test first creates a simple table.
        We then burst a simple insert query and check to see if the qid has been
        added to stl_burst_prepare_time with a valid time.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor, \
                self.db.cursor() as boot_cursor:
            # Creating a simple table to run our tests.
            cursor.execute("CREATE TABLE test_table (id int) diststyle even;")
            # Here we are attempting to burst a write query in the transaction.
            start_time = datetime.datetime.now().replace(microsecond=0)
            start_str = start_time.isoformat(' ')
            queryid = self.burst_insert_query_and_get_qid(cluster, cursor)
            # Checking to see if the qid was recorded with a valid prepare time.
            boot_cursor.execute("SELECT * FROM \
                                stl_burst_prepare_time \
                                WHERE qid = {} AND total_time_in_prepare > 0 \
                                AND logtime >= '{}' \
                                AND attempts = 1;".format(queryid, start_str))
            # Assert that the query comes back with at least 1 row.
            assert boot_cursor.rowcount is not None

    def test_burst_prepare_time_sys_table_multiple_attempt(self, cluster):
        """
        This test first creates a simple table.
        We then burst a simple insert query and check to see if the qid has been
        added to stl_burst_prepare_time with more than 1 attempt.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor, \
                self.db.cursor() as boot_cursor:
            # Creating a simple table to run our tests.
            cursor.execute("CREATE TABLE test_table (id int) diststyle even;")
            boot_cursor.execute("set always_burst_eligible_query to true;")
            cluster.set_event('EtForceUnsuitableBurst')
            # Starting a thread which will unset the event after the query attempts a
            # few times.
            global creation_thread
            creation_thread = threading.Thread(
                target=self.burst_unset_force_unsuitable_event, args=(cluster))
            start_time = datetime.datetime.now().replace(microsecond=0)
            start_str = start_time.isoformat(' ')
            creation_thread.start()
            queryid = self.burst_insert_query_and_get_qid(cluster, cursor)
            # Checking to see if the qid was recorded with attempt_count > 1.
            boot_cursor.execute("SELECT * FROM \
                                stl_burst_prepare_time \
                                WHERE qid = {} \
                                AND logtime >= '{}' \
                                AND attempts > 1;".format(queryid, start_str))
            # Assert that the query comes back with at least 1 row.
            assert boot_cursor.rowcount is not None
