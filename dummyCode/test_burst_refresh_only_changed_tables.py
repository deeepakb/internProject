# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid

from raff.common.db.session import DbSession
from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.superblock.helper import get_tree_based_dual_path_superblock_gucs
from raff.common.db.redshift_db import RedshiftDb
from raff.burst.burst_super_simulated_mode_helper import get_burst_conn_params

__all__ = ["super_simulated_mode"]
log = logging.getLogger(__name__)

SNAPSHOT_IDENTIFIER = "burst-init-{}".format(str(uuid.uuid4().hex))
TBL_DEF = ("create table public.dp48783_tbl_{}"
           "(c0 int, c1 int) distkey(c0) sortkey(c0);")
DROP_TBL = "drop table if exists public.dp48783_tbl_{};"
INSERT_VALUES = ("insert into public.dp48783_tbl_{} values "
                 "(4,4),(5,5),(6,6),(1,1),(2,2),(3,3),(7,7);")
INSERT_SELECT = ("insert into public.dp48783_tbl_{} "
                 "select * from public.dp48783_tbl_{};")
DELETE_VALUES = ("delete from public.dp48783_tbl_{} where c0 = 5;")
DELETE_VALUES_2 = ("delete from public.dp48783_tbl_{} where c0 = 1;")
ALTER_ADD_COLUMN = (
    "alter table public.dp48783_tbl_{} add column uncommitted_col "
    "varchar(max) default 'uncommitted_values';")

ENABLE_CBR_BURST_SCM_GUCS = {
    'burst_max_idle_time_seconds': 60 * 60,
    'burst_commit_refresh_check_frequency_seconds': 60 * 60,
    'burst_enable_write': 'true',
    'enable_burst_refresh_changed_tables': 'true',
    'vacuum_auto_worker_enable': 'false',
    'enable_burst_s3_commit_based_refresh': 'true',
    'burst_refresh_prefer_epsilon_metadata': 'true',
    'burst_refresh_prefer_epsilon_superblock': 'true',
    'data_sharing_refresh_specific_epsilon_metadata_table': 'false'
}
ENABLE_CBR_BURST_SCM_GUCS.update(get_tree_based_dual_path_superblock_gucs())
ENABLE_BBR_BURST_SCM_GUCS = {
    'burst_max_idle_time_seconds': 60 * 60,
    'burst_refresh_start_seconds': 60 * 60,
    'burst_enable_write': 'true',
    'enable_burst_refresh_changed_tables': 'true',
    'vacuum_auto_worker_enable': 'false',
    'enable_burst_s3_commit_based_refresh': 'false',
    'enable_burst_s3_commit_based_cold_start': 'false',
    'burst_refresh_prefer_epsilon_metadata': 'true',
    'burst_refresh_prefer_epsilon_superblock': 'true',
    'data_sharing_refresh_specific_epsilon_metadata_table': 'false'
}
ENABLE_BBR_BURST_SCM_GUCS.update(get_tree_based_dual_path_superblock_gucs())


class BaseBurstRefreshChangedTables(BurstWriteTest):
    def _get_tbl_id(self, cursor, tbl_name):
        cursor.execute("SELECT '{}'::regclass::oid".format(tbl_name))
        tbl_id = cursor.fetch_scalar()
        log.info("tbl name: {}, tbl id:{}".format(tbl_name, tbl_id))
        return tbl_id

    def _setup_tables(self, cursor, num_tbl):
        for i in range(num_tbl):
            cursor.execute("begin")
            cursor.execute(DROP_TBL.format(i))
            cursor.execute(TBL_DEF.format(i))
            cursor.execute(INSERT_VALUES.format(i))
            for j in range(11):
                cursor.execute(INSERT_SELECT.format(i, i))
            cursor.execute(DELETE_VALUES.format(i))
            cursor.execute("commit")
            self._get_tbl_id(cursor, "dp48783_tbl_{}".format(i))
        cursor.execute('grant all on all tables in schema public to public')

    def _cleanup_tables(self, num_tbl):
        with self.db.cursor() as cursor:
            for i in range(num_tbl):
                cursor.execute(DROP_TBL.format(i))
            cursor.execute(DROP_TBL.format('new'))
            cursor.execute(DROP_TBL.format('new_2'))
            cursor.execute(
                "drop table if exists pg_internal.redshift_auto_health_check_1")

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

    def _get_latest_changed_tbl_list(self):
        sql = ("SELECT btrim(tables) from stl_burst_refresh_tables_list where "
               "list_type = 1 and "
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

    def _get_last_refresh_start_time(self):
        sql = ("select eventtime from stl_burst_manager_refresh "
               "where action ilike '%RefreshStart%' and "
               "type ilike '%RefreshOnDemand%' order by 1 desc limit 1;")
        with self.db.cursor() as cursor:
            cursor.execute(sql)
            last_refresh_time = cursor.fetchall()
        return last_refresh_time[0][0]

    def _get_refreshed_tbl_list(self):
        last_refresh_time = self._get_last_refresh_start_time()
        sql = ("SELECT distinct "
               "SUBSTRING(message::text, charindex('localized table'"
               "::CHARACTER VARYING::text, message::text) + 16, charindex(' "
               "with'::CHARACTER VARYING::text, message::text) - "
               "charindex('localized table'::CHARACTER VARYING::text, "
               "message::text) - 16)::int AS tbl_id "
               "FROM stl_burst_refresh "
               "WHERE message ilike '%localized table%' "
               "and record_time > '{}'"
               "order by 1;")
        with RedshiftDb(
                conn_params=get_burst_conn_params()).cursor() as burst_cursor:
            burst_cursor.execute(sql.format(last_refresh_time))
            res = burst_cursor.fetchall()
            refreshed_tabled_list = {t[0] for t in res}
            log.info("refreshed table list: {}".format(
                sorted(refreshed_tabled_list)))
            return refreshed_tabled_list

    def _get_num_tbls_in_metadata(self):
        last_refresh_time = self._get_last_refresh_start_time()
        sql = ("SELECT distinct "
               "SUBSTRING(message::text, charindex('[Worker]'"
               "::CHARACTER VARYING::text, message::text) + 9, charindex("
               "' tables to be'::CHARACTER VARYING::text, message::text) - "
               "charindex('[Worker]'::CHARACTER VARYING::text, "
               "message::text) - 9)::int AS num_tbl "
               "FROM stl_burst_refresh "
               "WHERE message ilike '%tables to be refreshed%' "
               "and record_time > '{}'"
               "order by 1 desc;")
        with RedshiftDb(
                conn_params=get_burst_conn_params()).cursor() as burst_cursor:
            burst_cursor.execute(sql.format(last_refresh_time))
            res = burst_cursor.fetchall()
            return res[0][0]

    def _validate_tables_list(self, test_table_list, burst_table_list):
        try:
            assert test_table_list.issubset(burst_table_list)
        except Exception as e:
            log.error(str(e))
            log.info("Test table list has more tables: {}".format(
                sorted(test_table_list - burst_table_list)))
            with self.db.cursor() as cursor:
                for tbl in sorted(test_table_list - burst_table_list):
                    cursor.execute(
                        ("select eventtime,xid,query,tbl,btrim(event),"
                         "btrim(owned_cluster), num_dirty_cluster,"
                         "structure_change_sb, undo_sb "
                         "from stl_burst_write_query_event "
                         "where tbl = {} order by eventtime desc").format(tbl))
                    log.info("{} : {}".format(tbl, cursor.fetchall()))
            log.info("Burst table list has more tables: {}".format(
                burst_table_list - test_table_list))
            raise e

    def _query_stv_blocklist(self, table):
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(
                'select tbl,col,slice,blocknum,num_values,modified,'
                'hdr_modified,unsorted,tombstone,btrim(id)'
                'from stv_blocklist where tbl = {} '
                'order by 1,2,3,4'.format(table))
            res = bootstrap_cursor.result_fetchall()
            return res

    def base_burst_refresh_vacuum_owned_table(self, cluster):
        """
        This test case covers a corner case that VACUUM happens on a burst
        cluster owned table and trigger a refresh.
        1. Creates testing tables.
        2. VACUUM a table and refresh burst cluster to VACUUM committed version
        3. The vacuumed table must be refreshed during the refresh
        4. Run DML on another table so that sb version increases
        5. Trigger refresh and the vacuumed table is not refreshed
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        with db_session_master.cursor() as cursor:
            cursor.execute("set query_group to noburst;")
            num_tbls = 3
            # step 1
            self._setup_tables(cursor, num_tbls)
            self._start_and_wait_for_refresh(cluster)
            # validate table content after tables setup
            self._validate_all_tables_content(cluster, cursor, num_tbls)
            changed_tbls = set()
            tbl_name_raw = 'dp48783_tbl_{}'
            tbl_idx = 0

            tbl_idx = tbl_idx + 1
            tbl_name = tbl_name_raw.format(tbl_idx)
            tbl_id = self._get_tbl_id(cursor, tbl_name)
            cursor.execute("set query_group to burst;")
            cursor.execute(INSERT_VALUES.format(tbl_idx))
            self.check_last_query_bursted(cluster, cursor)
            blk1 = self._query_stv_blocklist(tbl_id)
            # step 2: VACUUM a table and refresh burst cluster to VACUUM
            # committed version
            cursor.execute("set query_group to noburst;")
            cursor.execute("VACUUM {} to 100 percent;".format(tbl_name))
            cursor.execute("select pg_last_query_id();")
            qid = cursor.fetch_scalar()
            with self.db.cursor() as bs_cursor:
                bs_cursor.execute(
                    "SELECT max(sb_version) "
                    "FROM stl_commit_internal_stats sc, stl_query sq "
                    "WHERE sq.query = {} and sq.xid = sc.xid;".format(qid))
                vacuum_sb_version = bs_cursor.fetchall()
            log.info("VACUUM sb version: {}".format(vacuum_sb_version))
            changed_tbls.add(tbl_id)
            blk2 = self._query_stv_blocklist(tbl_id)
            assert blk1 != blk2
            # Step 3: The vacuumed table is refreshed during the refresh
            self._start_and_wait_for_refresh(
                cluster, no_commit=True, with_new_backup=False)
            with self.db.cursor() as bs_cursor:
                bs_cursor.execute("SELECT max(refresh_version) "
                                  "from stl_burst_manager_refresh;")
                refreshed_sb_version = bs_cursor.fetchall()
            log.info("refreshed sb version: {}".format(refreshed_sb_version))
            assert vacuum_sb_version == refreshed_sb_version

            log.info("refresh on changed tables done")
            # check stl_burst_refresh_tables_list
            burst_changed_tbls = self._get_latest_changed_tbl_list()
            log.info("test changed list: {}".format(sorted(changed_tbls)))
            log.info("1st burst changed list: {}".format(
                sorted(burst_changed_tbls)))
            self._validate_tables_list(changed_tbls, burst_changed_tbls)
            # step 4: Run DML on another table so that sb version increases
            cursor.execute("VACUUM {} to 100 percent;".format(tbl_name))
            cursor.execute("set query_group to noburst;")
            cursor.execute(INSERT_VALUES.format(0))
            # step 5: vacuumed table should be refreshed this time
            self._start_and_wait_for_refresh(
                cluster, no_commit=True, with_new_backup=False)
            burst_changed_tbls = self._get_latest_changed_tbl_list()
            log.info("test changed list: {}".format(sorted(changed_tbls)))
            log.info("2nd burst changed list: {}".format(
                sorted(burst_changed_tbls)))
            assert not changed_tbls.issubset(burst_changed_tbls)
            cursor.execute("set query_group to burst;")
            cursor.execute(INSERT_VALUES.format(tbl_idx))
            self.check_last_query_bursted(cluster, cursor)

    def base_burst_refresh_only_changed_tables(self, cluster, is_cbr):
        """
        Test: Validate if burst refresh can refresh correct lists of tables
        after enabling refresh only changed tables improvement.
        1. Create tables.
        2. Cold start burst cluster and validate all table contents.
        3. Insert data on a table on main cluster.
           (This caused one changed table)
        4. Insert data on another table on burst cluster.
           (This caused one burst cluster owned table)
        5. Abort inserting data on another table on main cluster.
           (This caused one undo table)
        6. Create a table without commit but trigger background commit.
        7. VACUUM a table. (This caused one structure change table)
        8. In the same txn, insert data into a table on burst cluster at first,
           then insert data into a table on main cluster.
           (This caused one dirty table on the burst cluster)
        9. Alter add/drop column on a table.
        10. Insert data on a table without commit but trigger background commit.
        11. Insert data into main cluster specific table, verify not refreshed
        12. Trigger burst refresh and verify all tables content.
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        with db_session_master.cursor() as cursor:
            cursor.execute("set query_group to noburst;")
            num_tbls = 12
            self._setup_tables(cursor, num_tbls)
            with self.db.cursor() as bootstrap_cursor:
                bootstrap_cursor.execute(
                    "create table pg_internal.redshift_auto_health_check_1 "
                    "(x int);")
            self._start_and_wait_for_refresh(cluster)
            # validate table content after tables setup
            self._validate_all_tables_content(cluster, cursor, num_tbls)
            skipped_tbls = set()
            changed_tbls = set()
            no_refresh_expected_tbls = set()
            changed_tbls_after_commit = set()
            tbl_name_raw = 'dp48783_tbl_{}'
            tbl_idx = -1
            # case 0: insert on main
            tbl_idx = tbl_idx + 1
            cursor.execute("set query_group to noburst;")
            cursor.execute(INSERT_VALUES.format(tbl_idx))
            self._check_last_query_didnt_burst(cluster, cursor)
            tbl_name = tbl_name_raw.format(tbl_idx)
            changed_tbls.add(self._get_tbl_id(cursor, tbl_name))

            # case 11: Insert into main cluster specific table, verify not
            # refreshed
            with self.db.cursor() as bootstrap_cursor:
                # Health check tables are hardcoded with Oid 100004.
                bootstrap_cursor.execute(
                    "select relname from pg_class where oid = 100004 limit 1")
                rel_name = bootstrap_cursor.fetch_scalar()
                bootstrap_cursor.execute(
                    "insert into pg_internal.{} values (42)".format(rel_name))
                no_refresh_expected_tbls.add(100004)
            # Do not insert this table into the expected changed tables list as
            # this table should not be refreshed

            # case 1: insert on burst
            tbl_idx = tbl_idx + 1
            cursor.execute("set query_group to burst;")
            cursor.execute(INSERT_VALUES.format(tbl_idx))
            self._check_last_query_bursted(cluster, cursor)
            tbl_name = tbl_name_raw.format(tbl_idx)
            skipped_tbls.add(self._get_tbl_id(cursor, tbl_name))

            # case 2: undo table
            tbl_idx = tbl_idx + 1
            cursor.execute("set query_group to noburst;")
            cursor.execute("BEGIN;")
            cursor.execute(INSERT_VALUES.format(tbl_idx))
            self._check_last_query_didnt_burst(cluster, cursor)
            cursor.execute("ABORT;")
            tbl_name = tbl_name_raw.format(tbl_idx)
            changed_tbls.add(self._get_tbl_id(cursor, tbl_name))

            # case 3: insert into table with background commit
            tbl_idx = tbl_idx + 1
            db_session_bg_commit = DbSession(
                cluster.get_conn_params(user='master'))
            cursor_bg_commit = db_session_bg_commit.cursor()
            cursor_bg_commit.execute("set query_group to noburst;")
            cursor_bg_commit.execute("BEGIN;")
            cursor_bg_commit.execute(INSERT_VALUES.format(tbl_idx))
            self._check_last_query_didnt_burst(cluster, cursor_bg_commit)
            # Trigger bg commit
            cursor.execute(INSERT_VALUES.format('0'))
            cluster.run_xpx("hello")
            tbl_name = tbl_name_raw.format(tbl_idx)
            changed_tbls.add(self._get_tbl_id(cursor, tbl_name))

            # case 4: VACUUM
            tbl_idx = tbl_idx + 1
            tbl_name = tbl_name_raw.format(tbl_idx)
            cursor.execute("set query_group to noburst;")
            cursor.execute("VACUUM FULL {} to 100 percent;".format(tbl_name))
            self._check_last_query_didnt_burst(cluster, cursor)
            changed_tbls.add(self._get_tbl_id(cursor, tbl_name))

            # case 5: dirty table
            tbl_idx = tbl_idx + 1
            cursor.execute("BEGIN;")
            cursor.execute("set query_group to burst;")
            cursor.execute(INSERT_VALUES.format(tbl_idx))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("set query_group to noburst;")
            cursor.execute(INSERT_VALUES.format(tbl_idx))
            self._check_last_query_didnt_burst(cluster, cursor)
            cursor.execute("COMMIT;")
            tbl_name = tbl_name_raw.format(tbl_idx)
            changed_tbls.add(self._get_tbl_id(cursor, tbl_name))

            # case 6: Alter ADD/DROP Column
            tbl_idx = tbl_idx + 1
            tbl_name = tbl_name_raw.format(tbl_idx)
            cursor.execute("set query_group to noburst;")
            cursor.execute(
                "alter table {} add column c2 varchar(20) DEFAULT 'burst_test';".
                format(tbl_name))
            cursor.execute(
                "alter table {} add column c3 varchar(20) DEFAULT 'burst_test';".
                format(tbl_name))
            cursor.execute("alter table {} drop column c2;".format(tbl_name))
            changed_tbls.add(self._get_tbl_id(cursor, tbl_name))

            # case 7: Alter dist even
            tbl_idx = tbl_idx + 1
            tbl_name = tbl_name_raw.format(tbl_idx)
            cursor.execute("set query_group to noburst;")
            cursor.execute(
                "alter table {} alter diststyle even;".format(tbl_name))
            changed_tbls.add(self._get_tbl_id(cursor, tbl_name))

            # case 8: DELETE FROM TBL
            tbl_idx = tbl_idx + 1
            tbl_name = tbl_name_raw.format(tbl_idx)
            cursor.execute("set query_group to noburst;")
            cursor.execute(DELETE_VALUES_2.format(tbl_idx))
            changed_tbls.add(self._get_tbl_id(cursor, tbl_name))

            # case 9: DELETE Nothing
            tbl_idx = tbl_idx + 1
            tbl_name = tbl_name_raw.format(tbl_idx)
            cursor.execute("set query_group to noburst;")
            cursor.execute(DELETE_VALUES.format(tbl_idx))
            skipped_tbls.add(self._get_tbl_id(cursor, tbl_name))

            # case: create table without commit
            # The uncommitted new created table would be persisted in T1 but
            # not persisted in T2.
            db_session_uncommitted_new_table = DbSession(
                cluster.get_conn_params(user='master'))
            cursor_uncommitted_new_table = \
                db_session_uncommitted_new_table.cursor()
            cursor_uncommitted_new_table.execute("set query_group to noburst;")
            cursor_uncommitted_new_table.execute("BEGIN;")
            cursor_uncommitted_new_table.execute(TBL_DEF.format('new'))
            cursor_uncommitted_new_table.execute(INSERT_VALUES.format('new'))
            # Trigger bg commit
            cursor.execute(INSERT_VALUES.format('0'))
            cluster.run_xpx("hello")
            tbl_name = tbl_name_raw.format('new')
            changed_tbls.add(
                self._get_tbl_id(cursor_uncommitted_new_table, tbl_name))
            changed_tbls_after_commit.add(
                self._get_tbl_id(cursor_uncommitted_new_table, tbl_name))

            # case: create table with commit
            cursor.execute("set query_group to noburst;")
            cursor.execute(TBL_DEF.format('new_2'))
            tbl_name = tbl_name_raw.format('new_2')
            changed_tbls.add(self._get_tbl_id(cursor, tbl_name))

            # For tables that are not changed, they should be in skipped list
            log.info("Add unchanged tables to skip list")
            for i in range(tbl_idx + 1, num_tbls):
                tbl_name = tbl_name_raw.format(i)
                skipped_tbls.add(self._get_tbl_id(cursor, tbl_name))

            # Refresh and validate skipped/changed tables list
            self._start_and_wait_for_refresh(cluster)
            log.info("refresh on changed tables done")
            # check stl_burst_refresh_tables_list
            burst_skipped_tbls = self._get_latest_skipped_tbl_list()
            burst_changed_tbls = self._get_latest_changed_tbl_list()
            log.info("test skipped list: {}".format(sorted(skipped_tbls)))
            log.info("test changed list: {}".format(sorted(changed_tbls)))
            log.info("burst skipped list: {}".format(
                sorted(burst_skipped_tbls)))
            log.info("burst changed list: {}".format(
                sorted(burst_changed_tbls)))
            self._validate_tables_list(skipped_tbls, burst_skipped_tbls)
            self._validate_tables_list(changed_tbls, burst_changed_tbls)
            assert(not no_refresh_expected_tbls.issubset(burst_changed_tbls))
            # Check actual refreshed tables list
            refreshed_tabled_list = self._get_refreshed_tbl_list()
            assert skipped_tbls.isdisjoint(refreshed_tabled_list)
            assert (changed_tbls -
                    changed_tbls_after_commit).issubset(refreshed_tabled_list)
            # Check number of tbl built in metadata
            num_tbl_metadata = self._get_num_tbls_in_metadata()
            log.info("num of tables in metadata: {}".format(num_tbl_metadata))
            if is_cbr:
                assert num_tbl_metadata == len(refreshed_tabled_list)
            # validate tbls after refresh
            self._validate_all_tables_content(cluster, cursor, num_tbls)
            self._validate_table_content(cluster, cursor,
                                         tbl_name_raw.format('new_2'))

            # Burst write on all tables and validate contents
            cursor_bg_commit.execute("commit")
            cursor_uncommitted_new_table.execute("commit")
            log.info("Refresh after committing opened txns")
            self._start_and_wait_for_refresh(cluster)
            refreshed_tabled_list = self._get_refreshed_tbl_list()
            burst_skipped_tbls = self._get_latest_skipped_tbl_list()
            burst_changed_tbls = self._get_latest_changed_tbl_list()
            log.info("burst skipped list: {}".format(
                sorted(burst_skipped_tbls)))
            log.info("burst changed list: {}".format(
                sorted(burst_changed_tbls)))
            self._validate_tables_list(
                changed_tbls - changed_tbls_after_commit, burst_skipped_tbls)
            # Refresh would not contain the table with txn insert because
            # the insert content was persisted during background commit.
            self._validate_tables_list(changed_tbls_after_commit,
                                       burst_changed_tbls)
            self._validate_tables_list(changed_tbls_after_commit,
                                       refreshed_tabled_list)
            for i in range(num_tbls):
                cursor.execute("set query_group to burst;")
                cursor.execute(INSERT_VALUES.format(i))
                self._check_last_query_bursted(cluster, cursor)
            cursor_uncommitted_new_table.execute("set query_group to burst;")
            cursor_uncommitted_new_table.execute(INSERT_VALUES.format('new'))
            self._check_last_query_bursted(cluster,
                                           cursor_uncommitted_new_table)
            cursor.execute(INSERT_VALUES.format('new_2'))
            self._check_last_query_bursted(cluster, cursor)
            self._validate_all_tables_content(cluster, cursor, num_tbls)
            self._validate_table_content(cluster, cursor_uncommitted_new_table,
                                         tbl_name_raw.format('new'))
            self._validate_table_content(cluster, cursor,
                                         tbl_name_raw.format('new_2'))
        self._cleanup_tables(num_tbls)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=ENABLE_CBR_BURST_SCM_GUCS)
@pytest.mark.custom_local_gucs(gucs=ENABLE_CBR_BURST_SCM_GUCS)
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBurstRefreshChangedTablesCBR(BaseBurstRefreshChangedTables):
    def test_burst_refresh_only_changed_tables_cbr(self, cluster):
        self.base_burst_refresh_only_changed_tables(cluster, True)

    def test_burst_refresh_vacuum_owned_table_cbr(self, cluster):
        self.base_burst_refresh_vacuum_owned_table(cluster)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=ENABLE_BBR_BURST_SCM_GUCS)
@pytest.mark.custom_local_gucs(gucs=ENABLE_BBR_BURST_SCM_GUCS)
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBurstRefreshChangedTablesBBR(BaseBurstRefreshChangedTables):
    def test_burst_refresh_only_changed_tables_bbr(self, cluster):
        self.base_burst_refresh_only_changed_tables(cluster, False)
