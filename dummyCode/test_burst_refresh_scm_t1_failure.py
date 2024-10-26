# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid

from raff.common.db.session import DbSession
from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_super_simulated_mode_helper import (
    super_simulated_mode, cold_start_ss_mode, bootup_burst)
from raff.superblock.helper import get_tree_based_dual_path_superblock_gucs
from raff.common.db.redshift_db import RedshiftDb
from raff.burst.burst_super_simulated_mode_helper import get_burst_conn_params

__all__ = ["super_simulated_mode"]
log = logging.getLogger(__name__)

SNAPSHOT_IDENTIFIER = "burst-init-{}".format(str(uuid.uuid4().hex))
TBL_DEF = ("create table public.dp48783_tbl_{}"
           "(c0 int, c1 int) distkey(c0);")
INSERT_VALUES = ("insert into public.dp48783_tbl_{} values "
                 "(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7);")
INSERT_SELECT = ("insert into public.dp48783_tbl_{} "
                 "select * from public.dp48783_tbl_{};")
DELETE_VALUES = ("delete from public.dp48783_tbl_{} where c0 = 5;")

ENABLE_CBR_BURST_SCM_GUCS = {
    'burst_max_idle_time_seconds': 60 * 60,
    'burst_commit_refresh_check_frequency_seconds': 60 * 60,
    'burst_enable_write': 'true',
    'vacuum_auto_worker_enable': 'false',
    'enable_burst_s3_commit_based_refresh': 'true',
    'burst_refresh_prefer_epsilon_metadata': 'true',
    'burst_refresh_prefer_epsilon_superblock': 'true',
}
ENABLE_CBR_BURST_SCM_GUCS.update(get_tree_based_dual_path_superblock_gucs())
ENABLE_BBR_BURST_SCM_GUCS = {
    'burst_max_idle_time_seconds': 60 * 60,
    'burst_refresh_start_seconds': 60 * 60,
    'burst_enable_write': 'true',
    'vacuum_auto_worker_enable': 'false',
    'enable_burst_s3_commit_based_cold_start': 'false',
    'enable_burst_s3_commit_based_refresh': 'false',
    'burst_refresh_prefer_epsilon_metadata': 'true',
    'burst_refresh_prefer_epsilon_superblock': 'true',
}
ENABLE_BBR_BURST_SCM_GUCS.update(get_tree_based_dual_path_superblock_gucs())


class BaseBurstRefreshSCMT1Failure(BurstWriteTest):
    def _get_tbl_id(self, cursor, tbl_name):
        cursor.execute("SELECT '{}'::regclass::oid".format(tbl_name))
        tbl_id = cursor.fetch_scalar()
        log.info("tbl name: {}, tbl id:{}".format(tbl_name, tbl_id))
        return tbl_id

    def _setup_tables(self, cursor, num_tbl):
        for i in range(num_tbl):
            cursor.execute("begin")
            cursor.execute(TBL_DEF.format(i))
            cursor.execute(INSERT_VALUES.format(i))
            for j in range(10):
                cursor.execute(INSERT_SELECT.format(i, i))
            cursor.execute(DELETE_VALUES.format(i))
            cursor.execute("commit")
            self._get_tbl_id(cursor, "dp48783_tbl_{}".format(i))
        cursor.execute('grant all on all tables in schema public to public')

    def _validate_table_content(self, cluster, cursor, tbl_name):
        check_query = "select sum(c0), sum(c1) from {};".format(tbl_name)
        cursor.execute("set query_group to noburst;")
        cursor.execute(check_query)
        main_res = cursor.fetchall()
        self._check_last_query_didnt_burst(cluster, cursor)
        cursor.execute("set query_group to burst;")
        cursor.execute(check_query)
        burst_res = cursor.fetchall()
        self._check_last_query_bursted(cluster, cursor)
        assert main_res == burst_res, "{} result not match".format(tbl_name)

    def _validate_all_tables_content(self, cluster, cursor, tbl_num):
        for i in range(tbl_num):
            tbl_name = 'dp48783_tbl_{}'.format(i)
            self._validate_table_content(cluster, cursor, tbl_name)

    def _get_latest_skipped_tbl_list(self):
        sql = ("SELECT btrim(tables) from stl_burst_refresh_tables_list where "
               "list_type = 0 and "
               "sb_version = (select max(sb_version) "
               "from stl_burst_refresh_tables_list);")
        with RedshiftDb(
                conn_params=get_burst_conn_params()).cursor() as burst_cursor:
            burst_cursor.execute(sql)
            res = burst_cursor.fetchall()
            return {
                int(num) for sublist in res for num in sublist[0].split(',')
                if num
            }

    def _validate_fallback_option_set(self):
        sql = ("select distinct btrim(error) from stl_burst_manager_refresh "
               "where error ilike '%Set fallback from dual path Epsilon "
               "superblock failure%'")
        with self.db.cursor() as cursor:
            cursor.execute(sql)
            res = cursor.fetchall()
            log.info("_validate_fallback_option_set: {}".format(res))
            assert len(res) > 0, "fallback option not set"

    def _set_event_on_busrt_cluster(self, event):
        with RedshiftDb(
                conn_params=get_burst_conn_params()).cursor() as burst_cursor:
            burst_cursor.execute("xpx 'event set {}';".format(event))

    def _unset_event_on_busrt_cluster(self, event):
        with RedshiftDb(
                conn_params=get_burst_conn_params()).cursor() as burst_cursor:
            burst_cursor.execute("xpx 'event set {}';".format(event))

    def base_burst_refresh_scm_t1_failure_fallback(self, cluster, is_cbr):
        """
        Test: Validate if burst refresh can fallback to traditional sb when it
        hit hit error on SCM T1 tree based SB during grafting tables.
        1. Create tables with content.
        2. Set simulated failure event on burst cluster and trigger burst
           refresh.
        3. Rebuild SS mode and trigger burst refresh which should succeed.
        4. Validate tables content.
        """
        cold_start_ss_mode(cluster)
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        failed_event = 'EtSimEpsilonSBGraftFailure'
        with db_session_master.cursor() as cursor:
            cursor.execute("set query_group to noburst;")
            num_tbls = 3
            self._setup_tables(cursor, num_tbls)
            # Set event on burst cluster to simulate graft failure
            self._set_event_on_busrt_cluster(failed_event)
            trigger_error = False
            try:
                self._start_and_wait_for_refresh(cluster, refresh_wait_time=60)
            except Exception as e:
                log.info("Expected refresh error: {}".format(e))
                assert "Simulate a grafting refresh error" in str(e)
                trigger_error = True
            assert trigger_error
            self._validate_fallback_option_set()
            # Restart SS mode should not hit simulated failure
            if is_cbr:
                bootup_burst(ENABLE_CBR_BURST_SCM_GUCS)
            else:
                bootup_burst(ENABLE_BBR_BURST_SCM_GUCS)
            self._set_event_on_busrt_cluster(failed_event)
            cold_start_ss_mode(cluster)
            log.info("SS mode restart done")
            self._start_and_wait_for_refresh(cluster)
            # validate tbls after refresh
            self._validate_all_tables_content(cluster, cursor, num_tbls)
            for i in range(num_tbls):
                cursor.execute("set query_group to burst;")
                cursor.execute(INSERT_VALUES.format(i))
                self._check_last_query_bursted(cluster, cursor)
            self._check_last_query_bursted(cluster, cursor)
            self._validate_all_tables_content(cluster, cursor, num_tbls)
            self._unset_event_on_busrt_cluster(failed_event)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=ENABLE_CBR_BURST_SCM_GUCS)
@pytest.mark.custom_local_gucs(gucs=ENABLE_CBR_BURST_SCM_GUCS)
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBurstRefreshSCMT1FailureCBR(BaseBurstRefreshSCMT1Failure):
    def test_burst_refresh_scm_t1_failure_fallback_cbr(self, cluster):
        self.base_burst_refresh_scm_t1_failure_fallback(cluster, True)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=ENABLE_BBR_BURST_SCM_GUCS)
@pytest.mark.custom_local_gucs(gucs=ENABLE_BBR_BURST_SCM_GUCS)
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBurstRefreshSCMT1FailureBBR(BaseBurstRefreshSCMT1Failure):
    def test_burst_refresh_scm_t1_failure_fallback_bbr(self, cluster):
        self.base_burst_refresh_scm_t1_failure_fallback(cluster, False)
