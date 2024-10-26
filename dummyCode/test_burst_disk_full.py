# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode,\
                                                         get_burst_conn_params
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.common.dimensions import Dimensions
from raff.common.db.redshift_db import RedshiftDb
from test_burst_write_equivalence import TestBurstWriteBlockEquivalenceBase

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

START_REREP = "xpx 'start_block_rerep';"
DELETE_CMD = "delete from {}"
DROP_TABLE_CMD = "drop table if exists {};"
GET_TABLE_ID_CMD = "select oid from pg_class where relname = '{}';"
CHECK_BLOCK_LOCAL_REPS = ("select count(*) > 0 from stv_block_reps "
                          " where tbl = {} and local = 1 "
                          " and s3_state = '{}';")
COUNT_TABLE_BLOCKS = ("select count(*) from stv_blocklist where tbl = {}"
                      " and tombstone = 0")
COUNT_TABLE_LOCAL_REPS = ("select count(*) from stv_block_reps where tbl = {}"
                          " and local = 1 "
                          " and s3_state in ('kS3Backup', 'kS3PermMirror');")
QUERY_CMD = "SELECT * FROM {};"

DELETE_ROWS = "delete_rows"
UPDATE_ROWS = "update_rows"
BACKUP_BLOCK = "kS3Backup"
TEMP_BLOCK = "kS3TempMirror"
LARGE = "large"
SMALL = "small"


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(
    gucs={
        'slices_per_node': '3',
        'burst_enable_write': 'true',
        'enable_reserve_disk_address': 'false',
        'burst_enable_write_copy': 'true',
        'burst_enable_write_insert': 'true',
        'burst_enable_write_delete': 'true',
        'burst_enable_write_update': 'true',
        'vacuum_auto_worker_enable': 'false'
    })
class TestBurstDiskFull(TestBurstWriteBlockEquivalenceBase):

    def _run_cmd(self, cursor, cmd):
        log.info(cmd)
        cursor.execute(cmd)

    def _get_table_id(self, cursor, table_name):
        """
            Returns table id from pg_class given table name
        """
        query = GET_TABLE_ID_CMD.format(table_name)
        self._run_cmd(cursor, query)
        return cursor.fetchall()[0][0]

    def _validate_local_reps_on_burst_cluster(self, cursor, tbl_id,
                                              block_state, is_block_local):
        """
            Validates that the given table's blocks on burst cluster with
            write-enabled are in s3 only when disk is full. Otherwise, the
            blocks are expected to be on s3 & burst cluster as well.
        """
        expect = [(is_block_local,)]
        errmsg = ("Expected {} local reps for {} blocks on burst"
                  " for tbl = {}").format("" if is_block_local else "NO",
                                          block_state, tbl_id)

        query = CHECK_BLOCK_LOCAL_REPS.format(tbl_id, block_state)
        self._run_cmd(cursor, query)
        assert expect == cursor.fetchall(), errmsg

    def _validate_blocks_on_main_cluster(self, cursor, tbl_id):
        """
            Validates that all of given table's blocks on main cluster are
            present in s3 in backup/perm state and are local as well.
        """
        query = COUNT_TABLE_BLOCKS.format(tbl_id)
        self._run_cmd(cursor, query)
        block_count = set(cursor.fetchall())

        """
            Blocks coming to main cluster from burst cluster are not local in
            main cluster. They are in kS3PermMirror in s3-only from the main
            cluster's perspective. Hence, start rerep on main to bring those
            blocks on main cluster disks.
        """
        self._run_cmd(cursor, START_REREP)

        query = COUNT_TABLE_LOCAL_REPS.format(tbl_id)
        self._run_cmd(cursor, query)
        local_rep_count = set(cursor.fetchall())

        assert block_count == local_rep_count, \
               ("[FAIL]: Expected all table blocks to be local and in backup"
                " or in perm state in main cluster"
                " for tbl = {}").format(tbl_id)

    def _validate_data_equivalence(self, cluster, cursor, base_tbl_name):
        """
            Validates the following 3 tables have same data.
            - no_burst_table on main
            - burst_table on main
            - burst_table on burst
        """
        # compare no_burst_table-on-main and burst_table-on-main
        self._validate_content_equivalence(cursor, base_tbl_name)

        # compare no_burst_table-on-main and burst_table-on-burst
        # Query when set not bursted.
        cursor.execute("set query_group to metrics;")
        cursor.execute(QUERY_CMD.format(base_tbl_name + "_not_burst"))
        no_burst_rows = tuple(sorted(cursor.fetchall()))
        self._check_last_query_didnt_burst(cluster, cursor)
        # Query when set bursted.
        cursor.execute("set query_group to burst;")
        cursor.execute(QUERY_CMD.format(base_tbl_name + "_burst"))
        burst_rows = tuple(sorted(cursor.fetchall()))
        self._check_last_query_bursted(cluster, cursor)
        assert no_burst_rows == burst_rows, ("FAIL: Expected same content on"
                                             " main & burst clusters")

    def _delete_tables(self, cluster, cursor, base_tbl_name):
        # Delete when set not bursted.
        cursor.execute("set query_group to metrics;")
        cursor.execute(DELETE_CMD.format(base_tbl_name + "_not_burst"))
        self._check_last_query_didnt_burst(cluster, cursor)
        # Delete when set bursted.
        cursor.execute("set query_group to burst;")
        cursor.execute(DELETE_CMD.format(base_tbl_name + "_burst"))
        self._check_last_query_bursted(cluster, cursor)

    def _drop_tables(self, cursor, tbl_name):
        self._run_cmd(cursor, "set query_group to metrics;")
        self._run_cmd(cursor, DROP_TABLE_CMD.format(tbl_name))

    def _do_nothing(self, cluster, cursor, base_tbl_name):
        return

    def _get_table_op(self, vector):
        TABLE_OPS = {
            DELETE_ROWS: self._delete_tables,
            UPDATE_ROWS: self._update_tables_1
        }
        return TABLE_OPS.get(vector.operation, self._do_nothing)

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=['diststyle even', 'distkey(c0)'],
                sortkey=['', 'sortkey(c0)'],
                size=[SMALL, LARGE],
                diskfull=[False, True],
                operation=[None, DELETE_ROWS, UPDATE_ROWS]))

    def test_burst_disk_full(self, cluster, vector):
        """
            This test simulates OOD on burst cluster by setting an event
            and forcing fdisk to skip adding primary reps. The test verifies
            that fdisk continues to write blocks to s3 when that happens and
            that there are no primary reps on burst cluster but just s3-reps.
        """

        log.info("="*60)
        test_schema = 'test_schema'
        base_tbl_name = "dp28449"
        burst_table = base_tbl_name + "_burst"
        no_burst_table = base_tbl_name + "_not_burst"
        MAX_INSERTS = 5
        dml_session = DbSession(
                cluster.get_conn_params(user=SessionContext.MASTER),
                session_ctx=SessionContext(schema=test_schema))
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        with self.db.cursor() as sys_cursor, \
                dml_session.cursor() as dml_cursor, \
                burst_session.cursor() as burst_cursor:
            self._run_cmd(sys_cursor, "xpx 'auto_worker disable both';")
            self._run_cmd(sys_cursor, "set search_path to {}".format(
                test_schema))

            log.info("Setting up tables")
            self._setup_tables(dml_session, base_tbl_name, vector)
            burst_tbl_id, no_burst_tbl_id = tuple(
                [self._get_table_id(sys_cursor, t) for t in
                 [burst_table, no_burst_table]])
            log.info("{} tbl_id = {}".format(burst_table, burst_tbl_id))
            log.info("{} tbl_id = {}".format(no_burst_table, no_burst_tbl_id))

            log.info("Start backup and wait for burst refresh")
            self._start_and_wait_for_refresh(cluster)

            if vector.diskfull:
                CMD = "xpx 'event set EtSkipPrimaryRepForBurstWrite';"
                self._run_cmd(burst_cursor, CMD)

            log.info("Validating content is same on main and burst clusters")
            self._validate_data_equivalence(cluster, dml_cursor, base_tbl_name)
            log.info("Validate {} blocks on burst cluster".format(
                     BACKUP_BLOCK))
            self._validate_local_reps_on_burst_cluster(burst_cursor,
                                                       burst_tbl_id,
                                                       BACKUP_BLOCK,
                                                       vector.size == LARGE and
                                                       not vector.diskfull)

            log.info("Inserting rows on burst cluster via main cluster")
            for i in range(MAX_INSERTS):
                self._insert_tables(cluster, dml_cursor, base_tbl_name)
            log.info("Validate {} blocks on burst cluster".format(TEMP_BLOCK))
            self._validate_local_reps_on_burst_cluster(burst_cursor,
                                                       burst_tbl_id,
                                                       TEMP_BLOCK,
                                                       not vector.diskfull)

            log.info("Doing table operation : {}".format(vector.operation))
            self._get_table_op(vector)(cluster, dml_cursor, base_tbl_name)

            log.info("Validating content is same on main and burst clusters")
            self._validate_data_equivalence(cluster, dml_cursor, base_tbl_name)

            log.info("Validate {} blocks on burst cluster".format(TEMP_BLOCK))
            self._validate_local_reps_on_burst_cluster(burst_cursor,
                                                       burst_tbl_id,
                                                       TEMP_BLOCK,
                                                       not vector.diskfull)

            log.info("Validating table blocks on main cluster")
            tbl_ids = [burst_tbl_id, no_burst_tbl_id]
            for t in tbl_ids:
                self._validate_blocks_on_main_cluster(sys_cursor, t)
            log.info("Validate burst cluster is still up")
            burst_cursor.execute("xpx 'hello';")
            self._insert_tables(cluster, dml_cursor, base_tbl_name)
            self._validate_data_equivalence(cluster, dml_cursor, base_tbl_name)

            log.info("Drop tables on burst and main clusters")
            cursors_and_tables = \
                [(dml_cursor, burst_table), (dml_cursor, no_burst_table)]
            for t in cursors_and_tables:
                self._drop_tables(t[0], t[1])

            CMD = "xpx 'event unset EtSkipPrimaryRepForBurstWrite';"
            self._run_cmd(burst_cursor, CMD)

            log.info("PASSED")
