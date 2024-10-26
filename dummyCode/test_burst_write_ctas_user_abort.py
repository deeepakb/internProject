# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
import uuid
import getpass

from psycopg2.extensions import QueryCanceledError
from psycopg2 import OperationalError
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode, \
    touch_spinfile, rm_spinfile, wait_until_burst_process_blocks, cleanup_spin_file
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.storage.storage_test import create_thread
from raff.common.dimensions import Dimensions

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
cancel_cmd = "pg_cancel_backend({})"
terminate_cmd = "pg_terminate_backend({})"


@pytest.mark.serial_only
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_burst_gucs(gucs={
    'burst_enable_write': 'true',
    'burst_enable_write_user_ctas': 'true'})
@pytest.mark.custom_local_gucs(gucs={
    'enable_burst_s3_commit_based_refresh': 'true',
    'burst_enable_write_user_ctas': 'true'})
class TestBurstWriteCtasUserAbort(BurstWriteTest):

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions({"cancel_cmd": [
            "pg_cancel_backend({})",
            "pg_terminate_backend({})"
        ]})

    def _get_running_ctas_pid(self, cluster):
        """
        Get pid for CTAS running on Burst.
        """
        with self.db.cursor() as cursor:
            cursor.execute("""select pid from \
                            stv_inflight where userid>1 \
                            and text ilike '%create table ctas1%'
                            and concurrency_scaling_status = 1
                            order by starttime desc limit 1;""")
            pid = cursor.fetch_scalar()
        return pid

    def _execute_ctas(self, cursor):
        """
        Execute long ctas query.
        """
        log.info("Executing CTAS...")
        cursor.execute("set query_group to burst;")
        query = ("create table ctas1 as select * from catalog_sales;")
        cursor.execute(query)
        log.info("Finished executing CTAS.")

    def _execute_insert(self, cursor):
        query = ("insert into ctas1 select * from catalog_sales limit 10 ")
        cursor.execute(query)

    def _cancel_thread(self, cluster, cursor, cancel_cmd):
        """
            Thread to cancel inflight query
        """
        pid = self._get_running_ctas_pid(cluster)
        log.info("Cancelling query with VPID {}".format(pid))
        cursor.execute("select {}".format(cancel_cmd.format(pid)))

    def test_burst_write_ctas_user_abort(self, cluster, cursor, vector):
        """
        test burst write CTAS when user aborts.
        """
        db_session1 = DbSession(cluster.get_conn_params(user='master'))
        db_session2 = DbSession(cluster.get_conn_params(user='master'))
        with db_session1.cursor() as cursor, \
                self.db.cursor() as boot_cursor:
            self._start_and_wait_for_refresh(cluster)
            spin_name = "spin_execute"
            try:
                with create_thread(self._execute_ctas, (cursor, )) \
                        as ctas_thread, \
                        create_thread(self._cancel_thread, (cluster,
                                                            boot_cursor,
                                                            vector.cancel_cmd, )) \
                        as cancel_thread:
                    touch_spinfile(spin_name)
                    ctas_thread.start()
                    wait_until_burst_process_blocks(spin_name)
                    cancel_thread.start()
                    rm_spinfile(spin_name)
            # exception caused by pg_cancel_backend()
            except QueryCanceledError as e:
                assert "cancelled on user's request" in str(e)
            # exception caused by pg_terminate_backend()
            except OperationalError as e:
                assert "terminating connection" in str(e)

        with db_session2.cursor() as cursor:
            # Now execute CTAS without abort
            self._execute_ctas(cursor)
            self._check_last_ctas_bursted(cluster)
            self._execute_insert(cursor)
            self._check_last_query_bursted(cluster, cursor)

        cleanup_spin_file()
