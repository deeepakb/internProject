# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid

from raff.common.db.session import DbSession
from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_test import setup_teardown_burst, customise_burst_cluster

__all__ = [
    "super_simulated_mode", "setup_teardown_burst", "customise_burst_cluster"
]
log = logging.getLogger(__name__)

SNAPSHOT_IDENTIFIER = "burst-init-{}".format(str(uuid.uuid4().hex))
TBL_DEF = ("create table public.dp48783_tbl_{}"
           "(c0 int, c1 int) distkey(c0);")
INSERT_VALUES = ("insert into public.dp48783_tbl_{} values "
                 "(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7);")
INSERT_SELECT = ("insert into public.dp48783_tbl_{} "
                 "select * from public.dp48783_tbl_{};")
DELETE_VALUES = ("delete from public.dp48783_tbl_{} where c0 = 5;")
CHECK_QUERY = "select sum(c0), sum(c1) from public.dp48783_tbl_{};"
ALTER_ADD_COLUMN = (
    "alter table public.dp48783_tbl_{} add column uncommitted_col "
    "varchar(max) default 'uncommitted_values';")
ENABLE_CBR_BURST_GUCS = {
    'burst_max_idle_time_seconds': 60 * 60,
    'burst_commit_refresh_check_frequency_seconds': 60 * 60,
    'burst_enable_write': 'true',
    'vacuum_auto_worker_enable': 'false',
    'enable_burst_s3_commit_based_refresh': 'true',
}


class BaseBurstRefreshDuringTxn(BurstWriteTest):
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

    def _print_last_query_status(self, msg, cursor, should_burst=False):
        cursor.execute("select pg_last_query_id();")
        qid = cursor.fetch_scalar()
        cursor.execute(
            "select concurrency_scaling_status from stl_query where query = {};".
            format(qid))
        cs_status = cursor.fetch_scalar()
        log.info("{} Last qid = {}, burst status={}".format(
            msg, qid, cs_status))
        if should_burst:
            assert cs_status == 1
        else:
            assert cs_status != 1

    def base_test_burst_refresh_during_txn(self, cluster):
        """
        Test: Validate if queries qualification and refresh changed only tables
        can work correctly when refresh happens during txn.
        1. insert into a table on main in a unclosed txn
        2. create a new table on main in another unclosed txn
        3. trigger background commit and burst refresh
        4. try to burst write on these 2 tables and should fail
        5. commit the txns and refresh
        6. burst write on all tables and validate tables content
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        with db_session_master.cursor() as cursor:
            cursor.execute("set query_group to noburst;")
            num_tbls = 2
            self._setup_tables(cursor, num_tbls)
            self._start_and_wait_for_refresh(cluster)
            # validate table content after tables setup
            self._validate_all_tables_content(cluster, cursor, num_tbls)

            tbl_name_raw = 'dp48783_tbl_{}'

            # case : insert on burst
            cursor.execute("set query_group to burst;")
            cursor.execute(INSERT_VALUES.format(0))
            self._check_last_query_bursted(cluster, cursor)

            # case: insert into table with background commit
            log.info("case: insert into table with background commit")
            db_session_bg_commit = DbSession(
                cluster.get_conn_params(user='master'))
            cursor_bg_commit = db_session_bg_commit.cursor()
            cursor_bg_commit.execute("set query_group to noburst;")
            cursor_bg_commit.execute("BEGIN;")
            cursor_bg_commit.execute(INSERT_VALUES.format(1))
            self._check_last_query_didnt_burst(cluster, cursor_bg_commit)

            # case: create table without commit
            log.info("case: create table without commit")
            db_session_uncommitted_new_table = DbSession(
                cluster.get_conn_params(user='master'))
            cursor_uncommitted_new_table = db_session_uncommitted_new_table.cursor(
            )
            cursor_uncommitted_new_table.execute("set query_group to burst;")
            cursor_uncommitted_new_table.execute("BEGIN;")
            cursor_uncommitted_new_table.execute(TBL_DEF.format('new'))
            self._get_tbl_id(cursor_uncommitted_new_table,
                             tbl_name_raw.format('new'))
            cursor_uncommitted_new_table.execute(INSERT_VALUES.format('new'))

            # Trigger bg commit
            cursor.execute(INSERT_VALUES.format(0))
            cluster.run_xpx("hello")

            self._start_and_wait_for_refresh(cluster)
            log.info("refresh on changed tables done")
            # Try to burst all queries that should not run on burst cluster
            cursor_bg_commit.execute("set query_group to burst;")
            cursor_bg_commit.execute(CHECK_QUERY.format(1))
            self._print_last_query_status("bg commit select", cursor_bg_commit)
            cursor_bg_commit.execute(INSERT_VALUES.format(1))
            self._print_last_query_status("bg commit insert", cursor_bg_commit)
            # After commit, still not able to burst without refresh
            cursor_bg_commit.execute("commit")
            cursor_bg_commit.execute("set query_group to burst;")
            cursor_bg_commit.execute(CHECK_QUERY.format(1))
            self._print_last_query_status("bg commit select", cursor_bg_commit)
            cursor_bg_commit.execute(INSERT_VALUES.format(1))
            self._print_last_query_status("bg commit insert", cursor_bg_commit)
            cursor_uncommitted_new_table.execute("set query_group to burst;")
            cursor_uncommitted_new_table.execute(INSERT_VALUES.format('new'))
            self._print_last_query_status("bg create table",
                                          cursor_uncommitted_new_table)

            # Burst write on all tables and validate contents
            cursor_uncommitted_new_table.execute("commit")
            self._start_and_wait_for_refresh(cluster)
            self._validate_all_tables_content(cluster, cursor, num_tbls)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=ENABLE_CBR_BURST_GUCS)
@pytest.mark.custom_local_gucs(gucs=ENABLE_CBR_BURST_GUCS)
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBurstRefreshDuringTxn(BaseBurstRefreshDuringTxn):
    def test_burst_refresh_during_txn(self, cluster):
        self.base_test_burst_refresh_during_txn(cluster)


@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=ENABLE_CBR_BURST_GUCS)
@pytest.mark.usefixtures("customise_burst_cluster")
@pytest.mark.customise_burst_cluster_args(ENABLE_CBR_BURST_GUCS)
@pytest.mark.serial_only
@pytest.mark.cluster_only
class TestBurstRefreshDuringTxnCluster(BaseBurstRefreshDuringTxn):
    def test_burst_refresh_during_txn(self, cluster):
        self.base_test_burst_refresh_during_txn(cluster)
