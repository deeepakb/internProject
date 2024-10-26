# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid
import getpass
import random

from raff.burst.burst_write import BurstWriteTest
from raff.storage.alter_table_suite import AlterTableSuite
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions
from raff.storage.storage_test import disable_autoworkers
from raff.data_loaders.common import get_min_all_shapes_table_list


log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
DISTSTYLE_QUERY = "SELECT DISTSTYLE, \"table\" FROM SVV_TABLE_INFO "\
                  "WHERE table_id='{}'::regclass::oid"


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_burst_gucs(
        gucs={'burst_enable_write': 'true',
              'burst_enable_write_user_ctas': 'true'})
@pytest.mark.custom_local_gucs(
        gucs={'ctas_auto_analyze': 'false',
              'burst_enable_write': 'true',
              'burst_enable_write_user_ctas': 'true'})
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.super_simulated_load_min_all_shapes
class TestBurstWriteCTASAlterDiststyle(BurstWriteTest, AlterTableSuite):

    table_list = get_min_all_shapes_table_list()
    table = random.choice(table_list)

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(alter_cmd=[
                'DISTSTYLE all', 'DISTSTYLE auto', 'DISTSTYLE even'
            ]))

    def _check_diststyle(self, cursor, alter_diststyle):
        """
        Check if current diststyle for a table is the same as
        the one to be altered to. If this is the case return
        False so we don't alter the table. Otherwise return
        True.
        """
        cursor.execute(DISTSTYLE_QUERY.format(self.table))
        tbl_diststyle = cursor.fetchall()[0][0]
        log.info("Table diststyle: {}".format(tbl_diststyle))
        if tbl_diststyle.lower() in alter_diststyle:
            return False
        return True

    def _setup_table(self, cursor, base_table, diststyle="DISTSTYLE EVEN"):
        cursor.execute("CREATE TABLE IF NOT EXISTS {} (i int) {}"
                       .format(base_table, diststyle))
        cursor.execute("INSERT INTO {} VALUES (1), (2), (3)"
                       .format(base_table))

    def _execute_ctas(self, cursor, ctas_table, base_table, alter_cmd=''):
        cursor.execute("SET query_group to burst")
        cursor.execute("CREATE TABLE {} {} AS (SELECT * FROM {})"
                       .format(ctas_table, alter_cmd, base_table))

    def _execute_alter(self, cursor, table, cmd, no_burst=True):
        if no_burst:
            cursor.execute("SET query_group TO noburst")
        else:
            cursor.execute("SET query_group TO burst")
        cursor.execute("ALTER TABLE {} ALTER {}"
                       .format(table, cmd))

    def verify_table_content(self, cursor, res):
        cmd = "select count(*) from {};".format('ctas_' + self.table)
        cursor.execute(cmd)
        assert cursor.fetchall() == res

    def verify_table_content_and_properties_eff_dist(self,
                                                     cursor,
                                                     schema,
                                                     table,
                                                     res,
                                                     expect_distkey,
                                                     in_txn=False,
                                                     effective_diststyle=None):
        with disable_autoworkers(cursor):
            # validate table content
            self.verify_table_content(cursor, res)
            # validte uniqueness of rowid
            self.validate_row_id(cursor, table)
            # validate correctness of distribution key
            if 'distkey' in expect_distkey or 'auto(key' in expect_distkey:
                cursor.execute("xpx 'check_distribution {}';".format(table))
                if 'auto' in expect_distkey:
                    self.validate_distkey(cursor, table, expect_distkey)
                    expect_distkey = 'diststyle auto'
            # validate sortedness of table
            cursor.execute("xpx 'sortedness_checker {} 0 1';".format(table))
            # validate diststyle
            self.validate_diststyle(cursor, schema, table, expect_distkey)
            # validate effective diststyle
            self.verify_effective_diststyle(cursor,
                                            schema,
                                            table,
                                            expected=effective_diststyle)
            # validate deletexid
            self.validate_deletexid(cursor, schema, table)
            # validate empty blocks
            self.validate_no_empty_blocks(cursor, schema, table)
            # validate encoding types
            self.validate_encoding_type(cursor, schema, table, in_txn)

    def test_burst_write_ctas_with_diff_diststyles(
            self, cluster, cursor, vector):
        """
        Setup base table with different distribution styles and
        execute BW CTAS. Verify the resulting table has the correct
        distribution style and table contents.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        base_table = self.table
        ctas_table = "ctas_" + base_table
        schema = "public"
        with db_session.cursor() as cursor, \
                self.db.cursor() as boot_cursor:
            cursor.execute("set search_path to public;")
            # If table chosen has the distribution style we are testing
            should_alter = self._check_diststyle(boot_cursor, vector.alter_cmd)
            if should_alter:
                # alter base table to diststyle we are testing
                self._execute_alter(cursor, base_table, vector.alter_cmd)
            self._start_and_wait_for_refresh(cluster)
            self._execute_ctas(cursor, ctas_table, base_table)
            self._check_last_ctas_bursted(cluster)
            self.verify_table_content_and_properties_eff_dist(
                boot_cursor, schema, ctas_table,
                [(5000,)], 'DISTSTYLE auto',
                effective_diststyle=11)  # Diststyle AUTO(EVEN)
            # Clean up table for next run.
            cursor.execute("DROP TABLE IF EXISTS {}".format(ctas_table))

    @pytest.mark.parametrize("cmd", ['DISTSTYLE auto',
                                     'DISTSTYLE even',
                                     'DISTSTYLE all'])
    def test_burst_write_ctas_force_diststyle(
            self, cluster, cursor, vector, cmd):
        """
        Setup base table and execute BW CTAS with forcing
        to different diststyles when the reference table
        can be any diststyle.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        base_table = self.table
        ctas_table = "ctas_" + base_table
        with db_session.cursor() as cursor, \
                self.db.cursor() as boot_cursor:
            schema = db_session.session_ctx.schema
            boot_cursor.execute("SET search_path TO {}".format(schema))
            self._start_and_wait_for_refresh(cluster)
            self._execute_ctas(cursor, ctas_table,
                               base_table, alter_cmd=vector.alter_cmd)
            if vector.alter_cmd == 'DISTSTYLE all':
                self._check_last_query_didnt_burst(cluster, cursor)
            else:
                self._check_last_ctas_bursted(cluster)
            self.verify_table_content_and_properties(
                boot_cursor, schema, ctas_table,
                [(5000,)],
                # diststyle should match what was specified
                # in the query.
                vector.alter_cmd)

    @pytest.mark.parametrize("cmd", ['DISTSTYLE auto',
                                     'DISTSTYLE even',
                                     'DISTSTYLE all'])
    def test_burst_write_ctas_diststyle_out_txn(
                self, cluster, cursor, vector, cmd):
        """
        Testing altering a table created through BW CTAS
        can successfully be altered to any other distribution style
        """
        if vector.alter_cmd == cmd:
            pytest.skip("Can not alter table to same distribution style")

        db_session = DbSession(cluster.get_conn_params(user='master'))
        base_table = self.table
        ctas_table = "ctas_" + base_table
        with db_session.cursor() as cursor, \
                self.db.cursor() as boot_cursor:
            schema = db_session.session_ctx.schema
            boot_cursor.execute("SET search_path TO {}".format(schema))
            self._start_and_wait_for_refresh(cluster)
            self._execute_ctas(cursor, ctas_table, base_table, alter_cmd=cmd)
            if cmd == 'DISTSTYLE all':
                self._check_last_ctas_didnt_burst(cluster)
            else:
                self._check_last_ctas_bursted(cluster)
            self._execute_alter(cursor, ctas_table,
                                vector.alter_cmd, no_burst=False)
            # if vector.alter_cmd == DISTSTYLE auto, it will be AUTO(all)
            # thuse we don't expect alter to burst
            if vector.alter_cmd in ['DISTSTYLE all', 'DISTSTYLE auto']:
                self._check_last_query_didnt_burst(cluster, cursor)
                self.verify_table_content_and_properties(
                    boot_cursor, schema, ctas_table,
                    [(5000,)], vector.alter_cmd)
            elif cmd == 'DISTSTYLE all':
                self._check_last_query_didnt_burst(cluster, cursor)
                self.verify_table_content_and_properties(
                    boot_cursor, schema, ctas_table,
                    [(5000,)], vector.alter_cmd)
            else:
                self._check_last_query_bursted(cluster, cursor)
                self.verify_table_content_and_properties(
                    boot_cursor, schema, ctas_table,
                    [(5000,)], vector.alter_cmd)

    @pytest.mark.parametrize("cmd", ['DISTSTYLE auto',
                                     'DISTSTYLE even',
                                     'DISTSTYLE all'])
    def test_burst_write_ctas_diststyle_in_txn(
                self, cluster, cursor, vector, cmd):
        """
        Testing altering a table created through BW CTAS in txn
        can successfully be altered to any other distribution style
        """
        if vector.alter_cmd == cmd:
            pytest.skip("Can not alter table to same distribution style")

        db_session = DbSession(cluster.get_conn_params(user='master'))
        base_table = self.table
        ctas_table = "ctas_" + base_table
        with db_session.cursor() as cursor, \
                self.db.cursor() as boot_cursor:
            schema = db_session.session_ctx.schema
            boot_cursor.execute("SET search_path TO {}".format(schema))
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("begin")
            self._execute_ctas(cursor, ctas_table, base_table, alter_cmd=cmd)
            if cmd == 'DISTSTYLE all':
                self._check_last_ctas_didnt_burst(cluster)
            else:
                self._check_last_ctas_bursted(cluster)
            self._execute_alter(cursor, ctas_table,
                                vector.alter_cmd, no_burst=False)
            cursor.execute("commit")
            # if vector.alter_cmd == DISTSTYLE auto, it will be AUTO(all)
            # thuse we don't expect alter to burst
            if vector.alter_cmd in ['DISTSTYLE all', 'DISTSTYLE auto']:
                self._check_last_query_didnt_burst(cluster, cursor)
                self.verify_table_content_and_properties(
                    boot_cursor, schema, ctas_table,
                    [(5000,)], vector.alter_cmd)
            elif cmd == 'DISTSTYLE all':
                self._check_last_query_didnt_burst(cluster, cursor)
                self.verify_table_content_and_properties(
                    boot_cursor, schema, ctas_table,
                    [(5000,)], vector.alter_cmd)
            else:
                self._check_last_query_bursted(cluster, cursor)
                self.verify_table_content_and_properties(
                    boot_cursor, schema, ctas_table,
                    [(5000,)], vector.alter_cmd)
