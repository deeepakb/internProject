# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid

from raff.common.db.session import DbSession
from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.redshift_db import RedshiftDb
from raff.burst.burst_super_simulated_mode_helper import get_burst_conn_params

__all__ = ["super_simulated_mode"]
log = logging.getLogger(__name__)

SNAPSHOT_IDENTIFIER = "burst-init-{}".format(str(uuid.uuid4().hex))
TBL_DEF = ("create table public.dp48783_tbl_{}"
           "(c0 int, c1 int) distkey(c0);")
INSERT_VALUES = ("insert into public.dp48783_tbl_{} values "
                 "(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7);")
ENABLE_CBR_BURST_GUCS = {
    'burst_enable_write': 'true',
    'vacuum_auto_worker_enable': 'false',
    'enable_burst_s3_commit_based_refresh': 'true',
}


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=ENABLE_CBR_BURST_GUCS)
@pytest.mark.custom_local_gucs(gucs=ENABLE_CBR_BURST_GUCS)
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestSTLBurstRefreshTablesList(BurstWriteTest):
    def _get_tbl_id(self, cursor, tbl_name):
        cursor.execute("SELECT '{}'::regclass::oid".format(tbl_name))
        tbl_id = cursor.fetch_scalar()
        log.info("tbl name: {}, tbl id:{}".format(tbl_name, tbl_id))
        return tbl_id

    def _setup_burst_table(self, cluster, cursor):
        cursor.execute(TBL_DEF.format('burst'))
        self._start_and_wait_for_refresh(cluster)
        cursor.execute("set query_group to burst;")
        cursor.execute(INSERT_VALUES.format('burst'))
        self._check_last_query_bursted(cluster, cursor)

    def _setup_tables(self, cursor, num_tbl):
        tbl_ids = set()
        for i in range(num_tbl):
            cursor.execute("begin")
            cursor.execute(TBL_DEF.format(i))
            cursor.execute(INSERT_VALUES.format(i))
            cursor.execute("commit")
            tbl_ids.add(self._get_tbl_id(cursor, "dp48783_tbl_{}".format(i)))
        return tbl_ids

    def _get_latest_skipped_tbl_list(self):
        sql = ("SELECT btrim(tables) from stl_burst_refresh_tables_list where "
               "list_type = 0 and "
               "sb_version = (select max(sb_version) "
               "from stl_burst_refresh_tables_list);")
        with RedshiftDb(
                conn_params=get_burst_conn_params()).cursor() as burst_cursor:
            burst_cursor.execute(sql)
            res = burst_cursor.fetchall()
            assert len(res) > 1, "Not enough skipped tables"
            return {
                int(num) for sublist in res for num in sublist[0].split(',')
                if num
            }

    def _get_latest_changed_tbl_list(self):
        sql = ("SELECT btrim(tables) from stl_burst_refresh_tables_list where "
               "list_type = 1 and "
               "sb_version = (select max(sb_version) "
               "from stl_burst_refresh_tables_list);")
        with RedshiftDb(
                conn_params=get_burst_conn_params()).cursor() as burst_cursor:
            burst_cursor.execute(sql)
            res = burst_cursor.fetchall()
            assert len(res) > 1, "Not enough changed tables"
            return {
                int(num) for sublist in res for num in sublist[0].split(',')
                if num
            }

    def _validate_tables_list(self, test_table_list, burst_table_list):
        try:
            assert test_table_list.issubset(burst_table_list)
        except Exception as e:
            log.error(str(e))
            log.info("Test table list has more tables: {}".format(
                test_table_list - burst_table_list))
            with self.db.cursor() as cursor:
                for tbl in test_table_list - burst_table_list:
                    cursor.execute(
                        ("select * from stl_burst_write_query_event "
                         "where tbl = {} order by eventtime").format(tbl))
                    log.info(cursor.fetchall())
            log.info("Burst table list has more tables: {}".format(
                burst_table_list - test_table_list))
            raise e

    def test_stl_burst_refresh_tables_list(self, cluster):
        """
        Test: Validate if stl_burst_refresh_tables_list can split the table
        list into multiple entries when table ids is more than 1024 lines.
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        with db_session_master.cursor() as cursor:
            self._setup_burst_table(cluster, cursor)
            cursor.execute("set query_group to noburst;")
            num_tbls = 172
            new_tbl_ids = self._setup_tables(cursor, num_tbls)
            # Check changed table list for the 1st refresh
            self._start_and_wait_for_refresh(cluster)
            changed_list = self._get_latest_changed_tbl_list()
            self._validate_tables_list(new_tbl_ids, changed_list)

            # Check skipped table list for the 2nd refresh
            cursor.execute(TBL_DEF.format('new'))
            self._start_and_wait_for_refresh(cluster)
            skipped_list = self._get_latest_skipped_tbl_list()
            self._validate_tables_list(new_tbl_ids, skipped_list)
