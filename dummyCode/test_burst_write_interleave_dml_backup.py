# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.common.dimensions import Dimensions
from raff.storage.storage_test import create_thread

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

create_stmt = "create table burst3483(c0 bigint, c1 varchar(120), c2 bigint){}"
insert_values = ("insert into {}.burst3483 values"
                 "(1,'1',1),(2,'2',2),"
                 "(3,'3',3),(4,'4',4),"
                 "(5,'5',5),(6,'6',6),"
                 "(7,'7',7),(8,'8',8),"
                 "(9,'9',9),(10,'10',10);")
read_stmt = "select c0, c1, c2 from {}.burst3483 order by 1,2;"
insert_select_stmt = "insert into {}.burst3483 select * from {}.burst3483;"
delete_stmt = "delete from {}.burst3483 where c0 in (1,2);"
vacuum_stmt = "vacuum delete only {}.burst3483 to 100 percent;"
RESULT_ONE = [(1, '1', 1), (2, '2', 2), (3, '3', 3), (4, '4', 4), (5, '5', 5),
              (6, '6', 6), (7, '7', 7), (8, '8', 8), (9, '9', 9),
              (10, '10', 10)]
dml_sb_version_stmt = ("select dml_sb_version from stv_burst_tbl_sb_version "
                       "where id = '{}.burst3483'::regclass::oid;")
cluster_sb_version_stmt = ("select max(backup_version) from "
                           "stv_burst_manager_cluster_info;")


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.custom_burst_gucs(
        gucs={'enable_data_sharing_producer': 'false',
              'burst_enable_write': 'true'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstInterleaveWriteBackup(BurstWriteTest):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(
            dict(
                diststyle=[
                    'distkey(c0)', 'diststyle even'
                ],
                init_state=['non-pristine', 'pristine']))

    def _get_dml_sb_version(self, schema):
        with self.db.cursor() as cursor:
            cursor.execute(dml_sb_version_stmt.format(schema))
            return cursor.fetch_scalar()

    def _setup_tables(self, cursor_one, schema, diststyle, init_state):
        with self.db.cursor() as cursor:
            cursor.execute("xpx 'auto_worker disable all';")
        cursor_one.execute(create_stmt.format(diststyle))
        if init_state == 'non-pristine':
            # Issue two insert to bootstrap table content.
            cursor_one.execute(insert_values.format(schema))
            # Check normal dml increase dml end version inc.
            pre_sb_version = self._get_dml_sb_version(schema)
            cursor_one.execute(insert_values.format(schema))
            post_sb_version = self._get_dml_sb_version(schema)
            assert pre_sb_version < post_sb_version

    def _validate_table_content(self, cluster, vector, schema, cursor, step,
                                is_burst):
        if vector.init_state == 'non-pristine':
            step = step + 2
        expected_res = sorted(RESULT_ONE * step)
        cursor.execute(read_stmt.format(schema))
        result = cursor.fetchall()
        assert result == expected_res
        if is_burst:
            self._check_last_query_bursted(cluster, cursor)
        else:
            self._check_last_query_didnt_burst(cluster, cursor)

    def _get_burst_sb_version(self):
        with self.db.cursor() as cursor:
            cursor.execute(cluster_sb_version_stmt)
            return cursor.fetch_scalar()

    def _bg_vacuum(self, cursor, schema):
        try:
            cursor.execute("set query_group to metrics;")
            cursor.execute(vacuum_stmt.format(schema))
        except Exception as e:
            pytest.fail("Failed due to: {}".format(e))

    def test_burst_write_vacuum_backup(self, cluster):
        """
        Test:
             session-1           session-2                      session-3
             begin;
             select burst3483;
                                 vac(pause interncal commit)    backup + refresh
             insert to burst3483;
             select burst3483;
             commit;
             select burst3483;
             insert to burst3483;
             select burst3483;

        """
        user = 'burst3483_user'
        schema = 'test_burst3483_vac_schema'
        dml_session_one = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=user, schema=schema))
        dml_session_two = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=user, schema=schema))
        with dml_session_one.cursor() as cursor_one, \
                dml_session_two.cursor() as cursor_two:
            self._setup_tables(cursor_one, schema, "diststyle even",
                               "non-pristine")
            cursor_one.execute("begin;")
            for i in range(22):
                cursor_one.execute(insert_select_stmt.format(schema, schema))
            cursor_one.execute(delete_stmt.format(schema))
            cursor_one.execute("commit;")
            self._start_and_wait_for_refresh(cluster)
            # Issues read on session-one to take visibility snapshot on table
            # burst3483.
            cursor_one.execute("begin;")
            cursor_one.execute("set query_group to burst;")
            cursor_one.execute("select sum(c0), count(*), sum(c2) "
                               "from test_burst3483_vac_schema.burst3483;")
            assert cursor_one.fetchall() == [(436207616, 67108864, 436207616)]
            self._check_last_query_bursted(cluster, cursor_one)
            # Launches vacuum and pauses it on internal commit.
            with create_thread(self._bg_vacuum, (cursor_two, schema)) as thread, \
                    self._create_xen_guard("vacuum:internal_commit") as xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks()
                # Vacuum is paused on internal commit.
                self._start_and_wait_for_refresh(cluster)
                cursor_one.execute(insert_values.format(schema))
                self._check_last_query_didnt_burst(cluster, cursor_one)
                cursor_one.execute("select sum(c0), count(*), sum(c2) "
                                   "from test_burst3483_vac_schema.burst3483;")
                assert cursor_one.fetchall() == [(436207671, 67108874,
                                                  436207671)]
                self._check_last_query_didnt_burst(cluster, cursor_one)
                cursor_one.execute("commit")
                xen_guard.disable()
            # Post commit validation
            cursor_one.execute("select sum(c0), count(*), sum(c2) "
                               "from test_burst3483_vac_schema.burst3483;")
            assert cursor_one.fetchall() == [(436207671, 67108874, 436207671)]
            self._check_last_query_didnt_burst(cluster, cursor_one)
            self._start_and_wait_for_refresh(cluster)
            # One more dml to validate table health.
            cursor_one.execute(insert_values.format(schema))
            cursor_one.execute("select sum(c0), count(*), sum(c2) "
                               "from test_burst3483_vac_schema.burst3483;")
            assert cursor_one.fetchall() == [(436207726, 67108884, 436207726)]
            self._check_last_query_bursted(cluster, cursor_one)
            cursor_one.execute(
                "drop table test_burst3483_vac_schema.burst3483;")

    def test_burst_write_interleave_backup_one(self, cluster, vector):
        """
        Test:
             session-1               session-2                session-3
             begin;
             select burst3483;
                                     insert to burst3483
                                                              backup + refresh
             insert to burst3483
             select burst3483;
             commit;
             select burst3483;
        """
        user = 'burst3483_user'
        schema = 'test_burst3483_schema'
        dml_session_one = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=user, schema=schema))
        dml_session_two = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=user, schema=schema))
        with dml_session_one.cursor() as cursor_one, \
                dml_session_two.cursor() as cursor_two:
            self._setup_tables(cursor_one, schema, vector.diststyle,
                               vector.init_state)
            self._start_and_wait_for_refresh(cluster)
            # Issues read on session-one to take visibility snapshot on table
            # burst3483.
            cursor_one.execute("begin;")
            cursor_one.execute("set query_group to burst;")
            self._validate_table_content(cluster, vector, schema, cursor_one,
                                         0, True)

            cursor_two.execute("set query_group to metrics;")
            # Issues write query on target table on session-2
            cursor_two.execute(insert_values.format(schema))
            dml_sb_version = self._get_dml_sb_version(schema)
            burst_sb_version = self._get_burst_sb_version()
            assert dml_sb_version > burst_sb_version

            # Issues backup and refresh on session-3
            self._start_and_wait_for_refresh(cluster)
            burst_sb_version = self._get_burst_sb_version()
            assert dml_sb_version < burst_sb_version

            # Issues write query on session-1, and makes sure it is bursted and
            # its content is correct.
            cursor_one.execute(insert_values.format(schema))
            self._check_last_query_bursted(cluster, cursor_one)
            self._validate_table_content(cluster, vector, schema, cursor_one,
                                         1, True)
            cursor_one.execute("commit")
            dml_sb_version = self._get_dml_sb_version(schema)
            burst_sb_version = self._get_burst_sb_version()
            assert dml_sb_version > burst_sb_version
            # Commits transaction on session-1, and makes sure it is not
            # bursted and its content is correct.
            self._validate_table_content(cluster, vector, schema, cursor_one,
                                         2, True)
            # One more dml to validate table health.
            cursor_one.execute(insert_values.format(schema))
            self._validate_table_content(cluster, vector, schema, cursor_one,
                                         3, True)
            self._check_last_query_bursted(cluster, cursor_one)
            cursor_one.execute("drop table test_burst3483_schema.burst3483;")

    def test_burst_write_interleave_open_dml_backup(self, cluster, vector):
        """
        Test:
             session-1               session-2                session-3
             begin;
             select burst3483;
                                     begin;
                                     insert to burst3483
                                                              backup + refresh
                                     commit;
             insert to burst3483
             select burst3483;
             commit;
             select burst3483;
        """
        user = 'burst3483_user'
        schema = 'test_burst3483_schema_open_txn'
        dml_session_one = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=user, schema=schema))
        dml_session_two = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=user, schema=schema))
        with dml_session_one.cursor() as cursor_one, \
                dml_session_two.cursor() as cursor_two:
            self._setup_tables(cursor_one, schema, vector.diststyle,
                               vector.init_state)
            self._start_and_wait_for_refresh(cluster)
            # Issues read on session-one to take visibility snapshot on table
            # burst3483.
            cursor_one.execute("begin;")
            cursor_one.execute("set query_group to burst;")
            self._validate_table_content(cluster, vector, schema, cursor_one,
                                         0, True)

            # Opens transaction on session-2
            cursor_two.execute("set search_path to '{}'".format(schema))
            cursor_two.execute("begin;")
            cursor_two.execute("set query_group to metrics;")
            # Issues write query on target table on session-2
            cursor_two.execute(insert_values.format(schema))
            # Issues backup and refresh on session-3
            self._start_and_wait_for_refresh(cluster)
            # Close transaction on session-2
            cursor_two.execute("commit;")
            post_sb_version = self._get_dml_sb_version(schema)
            burst_sb_version = self._get_burst_sb_version()
            assert post_sb_version > burst_sb_version

            # Issues write query on session-1, and makes sure it is not
            # bursted and its content is correct.
            cursor_one.execute(insert_values.format(schema))
            self._check_last_query_didnt_burst(cluster, cursor_one)
            self._validate_table_content(cluster, vector, schema, cursor_one,
                                         1, False)
            cursor_one.execute("commit")
            dml_sb_version = self._get_dml_sb_version(schema)
            burst_sb_version = self._get_burst_sb_version()
            assert dml_sb_version > burst_sb_version
            # Commits transaction on session-1, and makes sure it is not
            # bursted and its content is correct.
            self._validate_table_content(cluster, vector, schema, cursor_one,
                                         2, False)
            # One more dml to validate table health.
            self._start_and_wait_for_refresh(cluster)
            cursor_one.execute(insert_values.format(schema))
            self._validate_table_content(cluster, vector, schema, cursor_one,
                                         3, True)
            cursor_one.execute(
                "drop table test_burst3483_schema_open_txn.burst3483;")
