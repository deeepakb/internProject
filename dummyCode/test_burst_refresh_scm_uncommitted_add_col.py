# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid

from raff.common.db.session import DbSession
from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.superblock.helper import get_tree_based_dual_path_superblock_gucs

__all__ = ["super_simulated_mode"]
log = logging.getLogger(__name__)

SNAPSHOT_IDENTIFIER = "burst-init-{}".format(str(uuid.uuid4().hex))
INSERT_SELECT = ("insert into public.dp20365_tbl_{} "
                 "select * from public.dp20365_tbl_{};")
ALTER_ADD_COLUMN = (
    "alter table public.dp20365_tbl_{} add column uncommitted_col "
    "varchar(max) default 'uncommitted_values';")

ENABLE_CBR_BURST_SCM_GUCS = {
    'burst_enable_write': 'true',
    'vacuum_auto_worker_enable': 'false',
    'enable_burst_s3_commit_based_refresh': 'true',
    'burst_refresh_prefer_epsilon_metadata': 'true',
    'burst_refresh_prefer_epsilon_superblock': 'true',
}
ENABLE_CBR_BURST_SCM_GUCS.update(get_tree_based_dual_path_superblock_gucs())


class BaseBurstRefreshUncommittedAddCol(BurstWriteTest):
    def _setup_table(self, cursor):
        tbl_def = ("create table public.dp20365_tbl_{}"
                   "(c0 int, c1 int) distkey(c0);")
        basic_insert = ("insert into public.dp20365_tbl_{} values "
                        "(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7);")
        for i in range(2):
            cursor.execute("begin")
            cursor.execute(tbl_def.format(i))
            cursor.execute(basic_insert.format(i))
            for j in range(10):
                cursor.execute(INSERT_SELECT.format(i, i))
            cursor.execute("commit")
        cursor.execute('grant all on all tables in schema public to public')


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.no_jdbc
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=ENABLE_CBR_BURST_SCM_GUCS)
@pytest.mark.custom_local_gucs(gucs=ENABLE_CBR_BURST_SCM_GUCS)
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestCBRSCMUncommittedAddCol(BaseBurstRefreshUncommittedAddCol):
    def test_cbr_scm_uncommitted_add_col(self, cluster, db_session):
        """
        Test: burst commit based refresh that contains uncommitted added column
        Burst refresh should pass all SCM dual path validation.
        1. Create table and stop alter table add column before commit.
        2. Trigger a background commit.
        3. Refresh the burst cluster with the sb containing uncommitted col.
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        with db_session_master.cursor() as cursor, \
                db_session.cursor() as cursor_bootstrap:
            cursor.execute("set query_group to noburst;")
            self._setup_table(cursor)
            log.info("1st refresh starts")
            self._start_and_wait_for_refresh(cluster)
            log.info("1st refresh done and burst write on both tables")
            # Run burst write queries to make sure burst cluster is available
            cursor.execute("set query_group to burst;")
            cursor.execute(INSERT_SELECT.format(0, 0))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(INSERT_SELECT.format(1, 1))
            self._check_last_query_bursted(cluster, cursor)
            # Run a Alter Add Column without commit
            cursor.execute("set query_group to noburst;")
            cursor.execute(INSERT_SELECT.format(0, 0))
            cursor.execute("BEGIN;")
            cursor.execute(ALTER_ADD_COLUMN.format(0))
            # Trigger background commit
            cursor_bootstrap.execute(INSERT_SELECT.format(1, 1))
            # run burst refresh on a sb version that contains uncommitted col
            log.info("2nd refresh starts")
            self._start_and_wait_for_refresh(cluster, with_new_backup=False)
            cursor.execute("COMMIT;")
            log.info("3rd refresh starts")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")
            cursor.execute(INSERT_SELECT.format(1, 1))
            self._check_last_query_bursted(cluster, cursor)
