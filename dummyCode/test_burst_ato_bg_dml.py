# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid

from raff.common.db.session import DbSession
from base_burst_alter_distkey import (
    BaseBurstAlterDistkey, ENABLE_BURST_ATO_NO_PERSIST_SHADOW,
    BurstWriteQueryType, WRITE_QUERY_SET, SMALL_SELECT)
from raff.storage.storage_test import create_thread
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.db_exception import NotSupportedError

__all__ = ["super_simulated_mode"]
log = logging.getLogger(__name__)

SNAPSHOT_IDENTIFIER = "burst-init-{}".format(str(uuid.uuid4().hex))
SEARCH_PATH = 'SET SEARCH_PATH TO "{}", "$user", public;'
GRANT_SCHEMA = "GRANT ALL PRIVILEGES ON SCHEMA {} TO {}"
GRANT_SCHEMA_TABLE = "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {} TO {}"


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=ENABLE_BURST_ATO_NO_PERSIST_SHADOW)
@pytest.mark.custom_local_gucs(gucs=ENABLE_BURST_ATO_NO_PERSIST_SHADOW)
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBurstAlterDistkeyBgDML(BaseBurstAlterDistkey):
    CHECK_QUERY = "select count(*), sum(c_custkey), sum(c_nationkey) from {}.{}"
    _SCHEMA = ''
    _TBL_GOLD = "customer_gold"
    backend_pid = 0
    _ALTER_DONE = False

    def _get_write_type(self, num):
        res = num % 3
        if res == 0:
            return BurstWriteQueryType.INSERT
        elif res == 1:
            return BurstWriteQueryType.DELETE
        elif res == 2:
            return BurstWriteQueryType.UPDATE
        else:
            return BurstWriteQueryType.SELECT

    def _perform_writes_on_tables(self, db_session, cluster, should_burst,
                                  write_type):
        # 1 perform select first
        full_tbl_path = "{}.{}".format(self._SCHEMA, self._TBL_NAME)
        full_gold = "{}.{}".format('public', self._TBL_GOLD)
        col = "c_custkey"
        with self.db.cursor() as bootstrap_cursor:
            bootstrap_cursor.execute(SMALL_SELECT.format(col, full_tbl_path))
            key = bootstrap_cursor.fetchone()[0]
        with db_session.cursor() as cursor:
            if write_type is BurstWriteQueryType.INSERT:
                cursor.execute(
                    WRITE_QUERY_SET.get(write_type).format(
                        full_tbl_path, full_tbl_path, col, key))
                if should_burst:
                    self._check_last_query_bursted(cluster, cursor)
                else:
                    self._check_last_query_didnt_burst(cluster, cursor)
                self.db.cursor().execute(
                    WRITE_QUERY_SET.get(write_type).format(
                        full_gold, full_gold, col, key))
            elif write_type is BurstWriteQueryType.DELETE:
                cursor.execute(
                    WRITE_QUERY_SET.get(write_type).format(
                        full_tbl_path, col, key))
                if should_burst:
                    self._check_last_query_bursted(cluster, cursor)
                else:
                    self._check_last_query_didnt_burst(cluster, cursor)
                self.db.cursor().execute(
                    WRITE_QUERY_SET.get(write_type).format(
                        full_gold, col, key))
            elif write_type is BurstWriteQueryType.UPDATE:
                cursor.execute(
                    WRITE_QUERY_SET.get(write_type).format(
                        full_tbl_path, col, '123', col, key))
                if should_burst:
                    self._check_last_query_bursted(cluster, cursor)
                else:
                    self._check_last_query_didnt_burst(cluster, cursor)
                self.db.cursor().execute(
                    WRITE_QUERY_SET.get(write_type).format(
                        full_gold, col, '123', col, key))

    def do_background(self, db_session_master, schema, cluster, num):
        with db_session_master.cursor() as cursor:
            cursor.execute(SEARCH_PATH.format(schema))
            try:
                cmd = "alter table {} alter distkey {}".format(
                    self._TBL_NAME, "c_custkey")
                cursor.execute(cmd)
                if num < 4:
                    self._verify_last_alter_query_bursted(cluster)
            except NotSupportedError as error:
                assert 'cannot alter to the same distkey' in error.pgerror
                self._ALTER_DONE = True
                pass

    def verify_table_content(self, cursor, res):
        cmd = self.CHECK_QUERY.format(self._SCHEMA, self._TBL_NAME)
        cursor.execute(cmd)
        assert cursor.fetchall() == res

    def test_burst_ato_bg_dml(self, cluster):
        """
        In this test, we have 2 table, customer_even and customer gold.
        customer_gold is experiencing same DML as a gold result.
        1. create table customer even and customer gold.
        2. Run concurrent DML on both while customer even has burst alter in background
        3. make sure dml and ato are all bursted.
        4. compare table content and validate
        """
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        db_session_user = DbSession(cluster.get_conn_params(user='master'))
        schema = db_session_master.session_ctx.schema
        self._SCHEMA = schema
        with db_session_user.cursor() as cursor:
            cursor.execute("set query_group to burst;")
        self._setup_dist_auto_even_table(db_session_master)
        with db_session_master.cursor() as cursor:
            cursor.execute(
                GRANT_SCHEMA.format(self._SCHEMA,
                                    db_session_user.session_ctx.username))
            cursor.execute(
                GRANT_SCHEMA_TABLE.format(
                    self._SCHEMA, db_session_user.session_ctx.username))
        xen_guard = self.create_xen_guard("alterdiststyle:finish_redist_iter")
        with self.db.cursor() as cursor:
            cursor.execute("CREATE TABLE {} As SELECT * from {}.{}".format(
                self._TBL_GOLD, schema, self._TBL_NAME))
        with db_session_master.cursor() as cursor:
            # Need to burst a query because cold start only check backup version
            self._start_and_wait_for_refresh(cluster)
            self._perform_writes_on_tables(db_session_master, cluster, True,
                                           BurstWriteQueryType.INSERT)
            self._simulate_alter_checkpoint_mode(cluster, cursor)
        for i in range(10):
            with create_thread(
                    self.do_background, (db_session_master, schema, cluster, i)) \
                        as thread:
                if self._ALTER_DONE is False:
                    xen_guard.enable()
                thread.start()
                try:
                    xen_guard.wait_until_process_blocks(timeout_secs=60)
                except Exception:
                    assert self._ALTER_DONE is True
                    xen_guard.disable()
                xen_guard.disable()
                self._perform_writes_on_tables(db_session_user, cluster,
                                               self._ALTER_DONE == False,
                                               self._get_write_type(i))
        with self.db.cursor() as cursor:
            cursor.execute(self.CHECK_QUERY.format('public', self._TBL_GOLD))
            res_gold = cursor.fetchall()
            cursor.execute("set search_path to {}".format(schema))
            self.verify_table_content_and_properties(cursor, schema,
                                                     self._TBL_NAME, res_gold,
                                                     'auto(key(c_custkey))')
