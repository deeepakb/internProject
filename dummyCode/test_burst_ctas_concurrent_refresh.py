# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid
import getpass
import datetime
import time
import threading

from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_super_simulated_mode_helper import\
    get_burst_conn_params, touch_spinfile, rm_spinfile
from raff.common.db.session import DbSession
from raff.storage.storage_test import create_thread
from raff.common.base_test import (run_priviledged_query_scalar_int,
                                   run_priviledged_query)

log = logging.getLogger(__name__)
SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
exception_event = threading.Event()

CTAS_STMT = "CREATE TABLE public.ctas_table AS SELECT * FROM base_table;"


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_burst_gucs(
    gucs={
        'burst_enable_write': 'true',
        'burst_enable_write_user_ctas': 'true',
        'xen_guard_enabled': 'true'
    })
@pytest.mark.custom_local_gucs(
    gucs={
        'burst_enable_write': 'true',
        'burst_enable_write_user_ctas': 'true',
        'xen_guard_enabled': 'true'
    })
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstCTASConcurrentRefresh(BurstWriteTest):
    def _setup_base_table(self, cursor):
        # Change table dist style so that insert query can be bursted and
        # trigger cold start.
        cursor.execute(
            "CREATE TABLE public.base_table(c0 int) DISTSTYLE EVEN;")
        cursor.execute(
            "CREATE TABLE public.base_table2(c0 int) DISTSTYLE EVEN;")
        cursor.execute("INSERT INTO public.base_table VALUES (1);")
        for i in range(20):
            cursor.execute("INSERT INTO base_table select * from base_table;")
        cursor.execute('grant all on public.base_table to public;')

    def _cleanup_tables(self, cursor):
        cursor.execute("DROP TABLE IF EXISTS public.base_table;")
        cursor.execute("DROP TABLE IF EXISTS public.base_table2;")
        cursor.execute("DROP TABLE IF EXISTS public.ctas_table;")

    def _get_ctas_qid(self, cursor):
        query = ("select query from stl_query where querytxt like '%{}%' "
                 "order by query desc limit 1;")
        cursor.execute(query.format(CTAS_STMT))
        qid = cursor.fetch_scalar()
        return qid

    def _check_last_ctas_bursted(self, cluster, cursor):
        qid = self._get_ctas_qid(cursor)
        self.verify_query_bursted(cluster, qid)

    def _check_last_ctas_didnt_burst(self, cluster, cursor):
        qid = self._get_ctas_qid(cursor)
        self.verify_query_didnt_bursted(cluster, qid)

    def _run_ctas(self, cluster, cursor, is_burst):
        try:
            if is_burst:
                cursor.execute("SET query_group TO burst")
            else:
                cursor.execute("SET query_group TO noburst")
            log.info("Running CTAS, is_burst: {}".format(is_burst))
            cursor.execute(CTAS_STMT)
            log.info("Verifying CTAS")
            if is_burst:
                self._check_last_ctas_bursted(cluster, cursor)
            else:
                self._check_last_ctas_didnt_burst(cluster, cursor)
        except Exception as e:
            # Set the exception event
            exception_event.set()
            log.info("_run_ctas Exception occurred: {}".format(str(e)))

    def _refresh_without_commit(self, cluster):
        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')
        with self.db.cursor() as cursor:
            query = ("select distinct sb_version from stv_superblock;")
            sb_version = run_priviledged_query_scalar_int(
                cluster, cursor, query)
            log.info(
                "Latest sb_version for commit refresh: {}".format(sb_version))

            cluster.run_xpx('burst_start_refresh')
            query_start = """select * from stl_burst_manager_refresh
                                where action in ('RefreshStart',
                                                'RefreshInProgress') and
                                    refresh_version >= {} and
                                eventtime >= '{}';""".format(
                sb_version, start_str)
            timeout = time.time() + 3000
            results = []
            while time.time() <= timeout and len(results) == 0:
                self._check_and_start_personalization(cluster)
                cluster.run_xpx('burst_start_refresh')
                time.sleep(10)
                results = run_priviledged_query(cluster, cursor, query_start)
                log.info("start_and_wait_for_refresh first check: {}".format(
                    results))

            if len(results) == 0:
                return

            query_end = """select * from stl_burst_manager_refresh
                            where action = 'RefreshEnd' and
                                    refresh_version >= {} and
                                    eventtime >= '{}';""".format(
                sb_version, start_str)
            timeout = time.time() + 300
            results = []
            while time.time() <= timeout and len(results) == 0:
                time.sleep(10)
                results = run_priviledged_query(cluster, cursor, query_end)
                log.info("start_and_wait_for_refresh second check: {}".format(
                    results))

    def test_burst_ctas_concurrent_refresh(self, cluster):
        """
        This test is to make sure the new tbl_perm created by burst CTAS would
        not be dropped by concurrent burst refresh process on burst CNs.
        Test race condition steps:
        1. Setup base tables.
        2. Trigger CTAS and spin it on main before creating new tbl_perm so
           that the next burst refresh would not have the new CTAS table id in
           changed/skipped tables list.
        3. Trigger burst refresh and spin it on main after generating the
           changed/skipped tables list.
        4. Unspin CTAS on main and spin it on burst after it just creating the
           tbl_perm on burst CNs.
        5. Unspin burst refresh and wait until it finishes.
        6. Unspin CTAS on burst and it should not have any assertion when
           trying to find the new CTAS table.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor:
            self._setup_base_table(cursor)
            self._start_and_wait_for_refresh(cluster)
            # No burst cluster is acquired above.
            # Force to acquire burst cluster and personalize.
            cursor.execute("set query_group to burst;")
            cursor.execute("INSERT INTO base_table2 VALUES (0);")
            # Run query on main and make burst cluster stale so that
            # burst refesh won't hit RefreshClusterAlreadyUpToDate
            # error.
            cursor.execute("set query_group to noburst;")
            cursor.execute("INSERT INTO base_table2 VALUES (0);")
            ctas_xen_guard = self._create_xen_guard(
                "burst_ctas:before_main_xen_define_relation")
            refresh_xen_guard = self._create_xen_guard(
                "burst_refresh:after_add_changed_table_list")
            # Create spin file inside SS mode
            burst_spin = ("burst_ctas:after_burst_poke_nodes")
            params = (cluster, cursor, True)
            with create_thread(self._run_ctas, params) as ctas_thread,\
                 create_thread(self._refresh_without_commit, (cluster,)) as\
                    refresh_thread, ctas_xen_guard, refresh_xen_guard:
                ctas_thread.start()
                log.info("2. Waiting for ctas_xen_guard on main")
                ctas_xen_guard.wait_until_process_blocks(timeout_secs=180)
                log.info("3.1 ctas_xen_guard spinning and refresh")
                refresh_thread.start()
                log.info("3.2 Waiting for refresh_xen_guard on main")
                refresh_xen_guard.wait_until_process_blocks(timeout_secs=180)
                log.info(
                    "4.1 refresh_xen_guard spinning and unblock ctas on main")
                touch_spinfile(burst_spin)
                ctas_xen_guard.disable()
                log.info("4.2 Wait for CTAS spin on burst SS mode")
                time.sleep(40)
                log.info("5. Trigger refresh to delete CTAS tbl_perm on burst")
                refresh_xen_guard.disable()
                refresh_thread.join()
                log.info("6. Unspin CTAS on burst and hit null ptr tbl_perm")
                rm_spinfile(burst_spin)
                ctas_thread.join()
            if exception_event.is_set():
                raise Exception(
                    "An exception occurred in the burst CTAS thread")
            for i in range(5):
                cursor.execute(
                    "insert into ctas_table select * from ctas_table;")
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("select sum(c0) from ctas_table;")
            res = cursor.fetchall()
            log.info("ctas_table sum: {}".format(res))
            assert res == [
                (33554432, ),
            ]
            self._cleanup_tables(cursor)

    def test_burst_ctas_concurrent_refresh_ownership(self, cluster):
        """
        This test is to make sure when the burst cluster owns the new CTAS
        table after burst CTAS and there is concurrent burst refresh,
        the following DMLs on the new CTAS table can still run on burst cluster
        without any error.
        Testing steps:
        1. Setup base tables.
        2. Trigger CTAS and spin it on main before creating new tbl_perm so
           that the next burst refresh would not have the new CTAS table id in
           changed/skipped tables list.
        3. Trigger burst refresh and spin it on main after generating the
           changed/skipped tables list.
        4. Unspin CTAS on main and let it finish.
        5. Unspin burst refresh and wait until it finishes.
        6. Burst DMLs on new CTAS table and it should not have any error.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor:
            self._setup_base_table(cursor)
            self._start_and_wait_for_refresh(cluster)
            # No burst cluster is acquired above.
            # Force to acquire burst cluster and personalize.
            cursor.execute("set query_group to burst;")
            cursor.execute("INSERT INTO base_table2 VALUES (0);")
            # Run query on main and make burst cluster stale so that
            # burst refesh won't hit RefreshClusterAlreadyUpToDate
            # error.
            cursor.execute("set query_group to noburst;")
            cursor.execute("INSERT INTO base_table2 VALUES (0);")
            ctas_xen_guard = self._create_xen_guard(
                "burst_ctas:before_main_xen_define_relation")
            refresh_xen_guard = self._create_xen_guard(
                "burst_refresh:after_add_changed_table_list")
            params = (cluster, cursor, True)
            with create_thread(self._run_ctas, params) as ctas_thread,\
                 create_thread(self._refresh_without_commit, (cluster,)) as\
                    refresh_thread, ctas_xen_guard, refresh_xen_guard:
                ctas_thread.start()
                log.info(
                    "2. Waiting for burst_ctas:before_main_xen_define_relation"
                )
                ctas_xen_guard.wait_until_process_blocks(timeout_secs=180)
                log.info(
                    "3.1 burst_ctas:before_main_xen_define_relation spinning"
                    " and triggering refresh")
                refresh_thread.start()
                log.info("3.2 Waiting for refresh_xen_guard on main")
                refresh_xen_guard.wait_until_process_blocks(timeout_secs=180)
                log.info(
                    "4 refresh_xen_guard spinning and unblock ctas on main")
                ctas_xen_guard.disable()
                time.sleep(40)
                log.info("5. Trigger refresh to delete CTAS tbl_perm on burst")
                refresh_xen_guard.disable()
                ctas_thread.join()
                refresh_thread.join()
            if exception_event.is_set():
                raise Exception(
                    "An exception occurred in the burst CTAS thread")
            log.info("6. burst insert on new CTAS table will hit nullptr")
            for i in range(5):
                cursor.execute(
                    "insert into ctas_table select * from ctas_table;")
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("select sum(c0) from ctas_table;")
            res = cursor.fetchall()
            log.info("ctas_table sum: {}".format(res))
            assert res == [
                (33554432, ),
            ]
            self._cleanup_tables(cursor)
