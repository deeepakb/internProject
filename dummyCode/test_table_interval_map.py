# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest
import logging
import uuid

from py_lib.common.xen_guard_control import XenGuardControl
from raff.common.cluster.cluster_session import ClusterSession
from raff.common.result import SelectResult
from raff.common.simulated_helper import xen_spin, xen_wait_for_spin
from raff.burst.burst_write import BurstWriteTest
from raff.common.simulated_helper import (
    create_localhost_snapshot,
    get_backup_sb_version,
    expect_backup_fail_with_error_code
)

log = logging.getLogger(__name__)

SB_VERSION_LOOKUP = "select distinct sb_version from stv_superblock;"
PG_CLASS_OID_LOOKUP = ("select oid from pg_class where relname = '{}' "
                       "and relnamespace = {}")
PG_NAMESPACE_OID_LOOKUP = "select oid from pg_namespace where nspname = '{}';"
TABLE_INTERVAL_QUERY = ("select modified_version, backup_version "
                        "from stv_table_interval_map "
                        "where table_id = {} order by modified_version;")
ERROR_CODE_METADATA_READ_FAIL = 1098
XEN_GUARD_NAME = ("storage:storage_backup:upload_superblock_and_new_blocks")
TIMEOUT_SECS = 300


@pytest.mark.localhost_only
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestTableIntervalMap(BurstWriteTest):
    """
    Test suite that ensures the modified/clean intervals are correctly
    populated in Xen->table_interval_map.
    """
    def _get_current_sb_version(self):
        """
        Get cluster(simluated mode) superblock version.

        Returns:
            current super block version integer
        """
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(SB_VERSION_LOOKUP)
            result = bootstrap_cursor.fetch_scalar()
        return result

    def _get_table_oid(self, db_session, table):
        """
        Get Table Object Id.

        Args:
            table (string): name of the table

        Returns:
            Integer of the table oid
        """
        schema = db_session.session_ctx.schema
        with self.db.cursor() as bootstrap_cursor:
            nsp_oid_query = PG_NAMESPACE_OID_LOOKUP.format(schema)
            bootstrap_cursor.execute(nsp_oid_query)
            nsp_oid = bootstrap_cursor.fetch_scalar()
            query = PG_CLASS_OID_LOOKUP.format(table, nsp_oid)
            bootstrap_cursor.execute(query)
            result = bootstrap_cursor.fetch_scalar()
        return result

    def _validate_table_interval(self, table_oid, expected_result):
        """
        Validate the entries of a specific table in stl_table_interval_map
        is equal to the expected result.

        Args:
            table_oid (int): Table oid
            expected_result (SelectResult): Expected query result
        """
        query = TABLE_INTERVAL_QUERY.format(table_oid)
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(query)
            result = bootstrap_cursor.result_fetchall()
        log.info("Output of query is {}".format(result))
        assert (result == expected_result)

    def _get_mem_usage_of_itv_map(self):
        """
        Returns the shared memory occcupied by 'MtTableIntervalMap' on LN.
        """
        query = ("select (allocated_size - freed_size) from stv_shmmem_stats"
                 " where type = 'MtTableIntervalMap' and node = 1000;")
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(query)
            result = bootstrap_cursor.fetch_scalar()
        return result

    def _get_ddl_sb_version(self):
        xid_query = ("select xid from stl_ddltext where lower(text) like"
                     "'%create table%' order by starttime desc limit 1;")
        sb_ver_query = ("select distinct sb_version from "
                        "stl_commit_internal_stats where xid = {}")
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(xid_query)
            xid = bootstrap_cursor.fetch_scalar()
            bootstrap_cursor.execute(sb_ver_query.format(xid))
            sb_ver = bootstrap_cursor.fetch_scalar()
        return sb_ver

    def _get_insert_sb_version(self):
        xid_query = ("select xid from stl_query where lower(querytxt) like"
                     "'%insert into%' order by starttime desc limit 1;")
        sb_ver_query = ("select distinct sb_version from "
                        "stl_commit_internal_stats where xid = {}")
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(xid_query)
            xid = bootstrap_cursor.fetch_scalar()
            bootstrap_cursor.execute(sb_ver_query.format(xid))
            sb_ver = bootstrap_cursor.fetch_scalar()
        return sb_ver

    def _create_table_and_validate_itv(self, cursor, db_session):
        cursor.execute("create table t1 (x int);")
        t1_oid = self._get_table_oid(db_session, "t1")
        sb_version_1 = self._get_ddl_sb_version()
        cursor.execute("insert into t1 values (0);")
        self._validate_table_interval(t1_oid,
                                      SelectResult(
                                          rows=[(
                                              sb_version_1,
                                              -1,
                                          )],
                                          column_types=['BIGINT', 'BIGINT']))
        return t1_oid, sb_version_1

    def test_tbl_itv_map_basic_operations(self, cursor, db_session):
        """
        Test the correctness of stv_table_interval_map
        upon basic SQL operations.
        """
        t1_oid, sb_version_1 = self._create_table_and_validate_itv(
            cursor, db_session)

        # get superblock version, ensure the closed intervals after backed
        # up are having the exact same version
        # take backup and ensure no open intervals are present
        backup_id = 'test_backup_{}'.format(str(uuid.uuid4()))
        create_localhost_snapshot(backup_id, wait=True)
        backedup_sb_version = get_backup_sb_version(backup_id)
        self._validate_table_interval(
            t1_oid,
            SelectResult(rows=[(sb_version_1, backedup_sb_version)],
                         column_types=['BIGINT', 'BIGINT']))
        cursor.execute("drop table if exists t1;")

    def test_tbl_itv_map_slow_backup(self, cursor, cluster, db_session):
        """
        Test the correctness of stv_table_interval_map
        when DML happens during a backup.
        """
        t1_oid, sb_version_1 = self._create_table_and_validate_itv(
            cursor, db_session)

        spin_file = "spin_backup_read_and_upload_blocks"
        with cluster.event('EtBackupPauseAtStage2'), xen_spin(spin_file):
            backup_id = 'test_backup_{}'.format(str(uuid.uuid4()))
            log.info("Backup id {}".format(backup_id))
            verify_backup_success = create_localhost_snapshot(
                backup_id, dmls_during_backup=True, wait=False)
            xen_wait_for_spin(spin_file)
            # DML, DDL during a slow backup
            cursor.execute("insert into t1 values (1);")
            sb_version_2 = self._get_insert_sb_version()
            cursor.execute("create table t2 (x int);")
            t2_oid = self._get_table_oid(db_session, "t2")
            sb_version_3 = self._get_ddl_sb_version()
            self._validate_table_interval(
                t1_oid,
                SelectResult(rows=[(sb_version_1, -1,), (sb_version_2, -1)],
                             column_types=['BIGINT', 'BIGINT']))
            self._validate_table_interval(
                t2_oid,
                SelectResult(rows=[(sb_version_3, -1,)],
                             column_types=['BIGINT', 'BIGINT']))

        verify_backup_success()

        backup_sb_version_1 = get_backup_sb_version(backup_id)
        self._validate_table_interval(
            t1_oid,
            SelectResult(
                rows=[(sb_version_1, backup_sb_version_1), (sb_version_2, -1)],
                column_types=['BIGINT', 'BIGINT']))

        backup_id = 'test_backup_{}'.format(str(uuid.uuid4()))
        create_localhost_snapshot(backup_id, wait=True)
        backup_sb_version_2 = get_backup_sb_version(backup_id)
        self._validate_table_interval(
            t1_oid,
            SelectResult(rows=[(sb_version_1, backup_sb_version_1),
                               (sb_version_2, backup_sb_version_2)],
                         column_types=['BIGINT', 'BIGINT']))
        self._validate_table_interval(
            t2_oid,
            SelectResult(rows=[(sb_version_3, backup_sb_version_2)],
                         column_types=['BIGINT', 'BIGINT']))
        cursor.execute("drop table if exists t1;")
        cursor.execute("drop table if exists t2;")

    def test_tbl_itv_map_cancel_backup(self, cluster, cluster_session,
                                       db_session):
        """
        Test the correctness of stv_table_interval_map
        if an ongoing backup is cancelled.
        """

        # Initialize the xen guard to pause backup in stage2
        xen_guard = XenGuardControl(
            host=None,
            username=None,
            guardname=XEN_GUARD_NAME,
            debug=True,
            remote=False)
        with cluster_session(
                gucs={'xen_guard_enabled': 'true'},
                clean_db_before=True,
                clean_db_after=False), db_session.cursor() as cursor:
            t1_oid, sb_version_1 = self._create_table_and_validate_itv(
                cursor, db_session)
            xen_guard.enable()
            cluster.set_event('EtBackupPauseAtStage2')
            backup_id = 'test_backup_{}'.format(str(uuid.uuid4()))
            create_localhost_snapshot(
                backup_id, dmls_during_backup=True, wait=False)
            xen_guard.wait_until_process_blocks(timeout_secs=TIMEOUT_SECS)
            log.info("xen_guard successfully blocked")
            cursor.execute("insert into t1 values (1);")
            sb_version_2 = self._get_insert_sb_version()

            # cancel backup
            cluster.set_event(
                'EtBlockValidationFail,node=-1,primary=1,disk_num=0,S3mirror=1'
            )
            xen_guard.disable()

            # verify backup cancelled
            expect_backup_fail_with_error_code(backup_id,
                                               ERROR_CODE_METADATA_READ_FAIL)

            self._validate_table_interval(
                t1_oid,
                SelectResult(rows=[(sb_version_1, -1), (sb_version_2, -1)],
                             column_types=['BIGINT', 'BIGINT']))
            cluster.unset_event('EtBlockValidationFail')
            cluster.unset_event('EtBackupPauseAtStage2')
            backup_id = 'test_backup_{}'.format(str(uuid.uuid4()))
            create_localhost_snapshot(backup_id, wait=True)
            backup_sb_version = get_backup_sb_version(backup_id)
            # only the latest interval will be closed
            self._validate_table_interval(
                t1_oid,
                SelectResult(rows=[(sb_version_1, -1),
                                   (sb_version_2, backup_sb_version)],
                             column_types=['BIGINT', 'BIGINT']))
            xen_guard.cleanup()
            cursor.execute("drop table if exists t1;")

    def test_tbl_itv_map_multiple_cancel_backup(self, cluster, cluster_session,
                                                db_session):
        """
        Test the correctness of stv_table_interval_map
        if multiple ongoing backups are cancelled.
        """
        xen_guard = XenGuardControl(
            host=None,
            username=None,
            guardname=XEN_GUARD_NAME,
            debug=True,
            remote=False)
        with cluster_session(
                gucs={'xen_guard_enabled': 'true'},
                clean_db_before=True,
                clean_db_after=False), db_session.cursor() as cursor:
            t1_oid, sb_version_1 = self._create_table_and_validate_itv(
                cursor, db_session)
            xen_guard.enable()
            cluster.set_event('EtBackupPauseAtStage2')
            backup_id_1 = 'test_backup_{}'.format(str(uuid.uuid4()))
            create_localhost_snapshot(backup_id_1, wait=False)
            xen_guard.wait_until_process_blocks(timeout_secs=TIMEOUT_SECS)
            cursor.execute("insert into t1 values (1);")
            sb_version_2 = self._get_insert_sb_version()
            cluster.set_event(
                'EtBlockValidationFail,node=-1,primary=1,disk_num=0,S3mirror=1'
            )
            self._validate_table_interval(
                t1_oid,
                SelectResult(
                    rows=[(sb_version_1, -1), (sb_version_2, -1)],
                    column_types=['BIGINT', 'BIGINT']))
            xen_guard.disable()

            expect_backup_fail_with_error_code(backup_id_1,
                                               ERROR_CODE_METADATA_READ_FAIL)

            cluster.unset_event('EtBlockValidationFail')
            backup_id_2 = 'test_backup_{}'.format(str(uuid.uuid4()))
            xen_guard.enable()
            create_localhost_snapshot(backup_id_2, wait=False)
            xen_guard.wait_until_process_blocks(timeout_secs=300)
            cursor.execute("insert into t1 values (2);")
            sb_version_3 = self._get_insert_sb_version()
            cluster.set_event(
                'EtBlockValidationFail,node=-1,primary=1,disk_num=0,S3mirror=1'
            )
            xen_guard.disable()

            expect_backup_fail_with_error_code(backup_id_2,
                                               ERROR_CODE_METADATA_READ_FAIL)

            cluster.unset_event('EtBlockValidationFail')
            cluster.unset_event('EtBackupPauseAtStage2')
            self._validate_table_interval(
                t1_oid,
                SelectResult(
                    rows=[(sb_version_1, -1), (sb_version_2, -1),
                          (sb_version_3, -1)],
                    column_types=['BIGINT', 'BIGINT']))

            backup_id = 'test_backup_{}'.format(str(uuid.uuid4()))
            create_localhost_snapshot(backup_id, wait=True)
            backup_sb_version = get_backup_sb_version(backup_id)
            self._validate_table_interval(
                t1_oid,
                SelectResult(
                    rows=[(sb_version_1, -1), (sb_version_2, -1),
                          (sb_version_3, backup_sb_version)],
                    column_types=['BIGINT', 'BIGINT']))
            xen_guard.cleanup()
            cursor.execute("drop table if exists t1;")

    def test_tbl_itv_map_restart_padb(self, cursor, cluster, db_session):
        '''
        Test the correctness of stv_table_interval_map
        after padb is restarted.
        '''
        cursor.execute('create table dist_even (x int) diststyle even')
        sb_version_1 = self._get_ddl_sb_version()
        cursor.execute('create table dist_all (x int) diststyle all')
        sb_version_2 = self._get_ddl_sb_version()
        dist_even_oid = self._get_table_oid(db_session, 'dist_even')
        dist_all_oid = self._get_table_oid(db_session, 'dist_all')
        cursor.execute('insert into dist_even values (0)')
        cursor.execute('insert into dist_all values (0)')

        self._validate_table_interval(
            dist_even_oid,
            SelectResult(rows=[(sb_version_1, -1)],
                         column_types=['BIGINT', 'BIGINT']))
        self._validate_table_interval(
            dist_all_oid,
            SelectResult(rows=[(sb_version_2, -1)],
                         column_types=['BIGINT', 'BIGINT']))

        sb_version_3 = self._get_insert_sb_version()
        cluster.reboot_cluster()

        # After restart, unbacked up table should have one entry
        # in table_interval_map
        self._validate_table_interval(
            dist_even_oid,
            SelectResult(rows=[(sb_version_3, -1)],
                         column_types=['BIGINT', 'BIGINT']))
        self._validate_table_interval(
            dist_all_oid,
            SelectResult(rows=[(sb_version_3, -1)],
                         column_types=['BIGINT', 'BIGINT']))

    def test_tbl_itv_map_pruning(self, cluster, cluster_session, db_session):
        """
        Test the pruning of map correctly, after hitting the memory limit.
        """
        # Cluster restart prunes the memory of the map automaticlaly.
        # Since the cluster_session context needs a reboot to set the guc, a
        # reboot here ensures that threshold_to_prune is set to the right
        # value.
        cluster.reboot_cluster()
        current_memory_occupied = self._get_mem_usage_of_itv_map()
        threshold_to_prune = current_memory_occupied + 200
        with cluster_session(
                gucs={
                    "table_interval_map_memory_limit": threshold_to_prune
                }):
            with db_session.cursor() as cursor:
                cursor.execute("create table table_even(x int) diststyle even")
                num_attempts = 0
                while self._get_mem_usage_of_itv_map() < threshold_to_prune:
                    if num_attempts >= 3:
                        pytest.fail("Number of attempts exceeded the expected"
                                    " limit. Failing the test.")
                    create_localhost_snapshot(str(uuid.uuid4()), wait=True)
                    cursor.execute("insert into table_even values (0)")
                    sb_version = self._get_current_sb_version()
                    num_attempts += 1
                assert num_attempts == 3
                # Following backup should prune the Map
                assert threshold_to_prune < self._get_mem_usage_of_itv_map()
                create_localhost_snapshot(str(uuid.uuid4()), wait=True)
                assert threshold_to_prune > self._get_mem_usage_of_itv_map()
                # Each backup increments superblock version by 2
                # (does 2 commits). Since pruning is part of the backup
                # commit, and takes last successful backup into
                # consideration two intervals get left behind for this table.
                self._validate_table_interval(
                    self._get_table_oid(db_session, "table_even"),
                    SelectResult(
                        rows=[(sb_version - 3, sb_version - 2),
                              (sb_version, sb_version + 1)],
                        column_types=['BIGINT', 'BIGINT']))
                cursor.execute("drop table if exists table_even;")
