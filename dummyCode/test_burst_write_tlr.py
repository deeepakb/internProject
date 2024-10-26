# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved
from __future__ import division
import pytest
import logging
from datetime import datetime

from raff.common.cluster.cluster_helper import RedshiftClusterHelper
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.common.dimensions import Dimensions
from raff.common.host_type import HostType
from raff.storage.alter_table_suite import AlterTableSuite
from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_write import burst_write_basic_gucs
from raff.burst.burst_test import setup_teardown_burst
from raff.util.utils import run_bootstrap_sql

log = logging.getLogger(__name__)
__all__ = [setup_teardown_burst]


SOURCE_DATABASE_NAME = "dev"
SOURCE_SCHEMA_NAME = "public"
RESULT = [(1, '1'), (2, '2'), (3, '3'), (4, '4'), (5, '5'), (6, '6'), (7, '7'),
          (8, '8'), (9, '9'), (10, '10'), (11, '11'), (12, '12'), (13, '13'),
          (14, '14'), (15, '15'), (16, '16'), (17, '17'), (18, '18'),
          (19, '19'), (20, '20')]

RESULT_TWO = [(2, '2z'), (2, '2z'), (4, '4z'), (4, '4z'), (6, '6z'), (6, '6z'),
              (8, '8z'), (8, '8z'), (10, '10z'), (10, '10z'), (12, '12z'),
              (12, '12z'), (14, '14z'), (14, '14z'), (16, '16z'), (16, '16z'),
              (18, '18z'), (18, '18z'), (20, '20z'), (20, '20z')]

RESULT_THREE = [(2, '2zz'), (2, '2zz'), (2, '2zz'), (2, '2zz'),
                (4, '4zz'), (4, '4zz'), (4, '4zz'), (4, '4zz'),
                (6, '6zz'), (6, '6zz'), (6, '6zz'), (6, '6zz'),
                (8, '8zz'), (8, '8zz'), (8, '8zz'), (8, '8zz'),
                (10, '10zz'), (10, '10zz'), (10, '10zz'), (10, '10zz'),
                (12, '12zz'), (12, '12zz'), (12, '12zz'), (12, '12zz'),
                (14, '14zz'), (14, '14zz'), (14, '14zz'), (14, '14zz'),
                (16, '16zz'), (16, '16zz'), (16, '16zz'), (16, '16zz'),
                (18, '18zz'), (18, '18zz'), (18, '18zz'), (18, '18zz'),
                (20, '20zz'), (20, '20zz'), (20, '20zz'), (20, '20zz')]
RESULT_LARGE = [(220200960, )]

RESULT_LARGE_TWO = [(230686720, )]

RESULT_LARGE_THREE = [(461373440, )]


@pytest.mark.serial_only
@pytest.mark.custom_burst_gucs(gucs=burst_write_basic_gucs)
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.cluster_only
class TestBurstWriteTLR(BurstWriteTest, AlterTableSuite):
    def setup_test(self, cluster, db_session, sortkeys, snapshot_1,
                   snapshot_2):
        with db_session.cursor() as cursor:
            cursor.execute("begin;")
            for i in range(2 * len(sortkeys)):
                drop_cmd = "DROP TABLE IF EXISTS public.tlr_t{}".format(i)
                run_bootstrap_sql(cluster, drop_cmd)
            cursor.execute("commit;")

            cursor.execute("begin;")
            for i in range(len(sortkeys)):
                sortkey = sortkeys[i]
                cursor.execute("create table public.tlr_t{}"
                               "(c0 int encode lzo, c1 varchar(16)) "
                               " diststyle even {};".format(i * 2, sortkey))
                cursor.execute("create table public.tlr_t{}"
                               "(c0 int encode lzo, c1 varchar(16)) "
                               " distkey(c1) {};".format(i * 2 + 1, sortkey))
            ins_cmd = ("insert into public.tlr_t{}(c0,c1)values(1,'1'),(2,'2'),"
                       "(3,'3'),(4,'4'),(5,'5'),(6,'6'),(7,'7'),(8,'8'),"
                       "(9,'9'),(10,'10'),(11,'11'),(12,'12'),(13,'13'),"
                       "(14,'14'),(15,'15'),(16,'16'),(17,'17'),(18,'18'),"
                       "(19,'19'),(20,'20');")
            cursor.execute("commit;")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")
            cursor.execute("begin;")
            for i in range(2 * len(sortkeys)):
                cursor.execute(ins_cmd.format(i))
                self._check_last_query_bursted(cluster, cursor)

            ins_sel_cmd = ("insert into public.tlr_t{}(c0,c1) select c0,c1 "
                           "from public.tlr_t{};")
            for i in range(len(sortkeys)):
                cursor.execute(ins_sel_cmd.format(i, i))
                self._check_last_query_bursted(cluster, cursor)
            for i in range(len(sortkeys), 2 * len(sortkeys)):
                for times in range(20):
                    cursor.execute(ins_sel_cmd.format(i, i))
            cursor.execute("commit;")

            cluster.backup_cluster(snapshot_1, wait=True)

            update_cmd = "update public.tlr_t{} set c1=c1+'z';"
            delete_cmd = "delete from public.tlr_t{} where c0 % 2 = 1;"
            cursor.execute("begin;")
            for i in range(2 * len(sortkeys)):
                cursor.execute(update_cmd.format(i))
                cursor.execute(delete_cmd.format(i))
                cursor.execute(ins_sel_cmd.format(i, i))
                self._check_last_query_bursted(cluster, cursor)
            cursor.execute("commit;")
            cluster.backup_cluster(snapshot_2, wait=True)

    def teardown_test(self, cluster, snapshot_1, snapshot_2, tbl_num):
        with self.db.cursor() as cursor:
            for i in range(tbl_num):
                cursor.execute("drop table if exists public.tlr_t{}".format(i))
                cursor.execute(
                    "drop table if exists public.tlr_t{}_dst_1".format(i))
                cursor.execute(
                    "drop table if exists public.tlr_t{}_dst_2".format(i))

        if cluster.host_type == HostType.CLUSTER:
            RedshiftClusterHelper.delete_snapshot(snapshot_1)
            RedshiftClusterHelper.delete_snapshot(snapshot_2)

    def _restore_all_tables(self, cluster, snapshot, dst_num, tbl_num):
        for i in range(tbl_num):
            starttime = datetime.now()
            drop_cmd = "DROP TABLE IF EXISTS tlr_t{}_dst_{}".format(i, dst_num)
            run_bootstrap_sql(cluster, drop_cmd)
            cluster.restore_table(
                snapshot,
                SOURCE_DATABASE_NAME,
                'tlr_t{}'.format(i),
                'tlr_t{}_dst_{}'.format(i, dst_num),
                wait_table_hydrated=True)
            endtime = datetime.now()
            log.info("TLR {} secounds on tbl: {}".format(
                (endtime - starttime).seconds,
                'tlr_t{}_dst_{}'.format(i, dst_num)))

    def _validate_table_content(self, cursor, dst_num, tbl_num, result,
                                result_large):
        select_cmd = 'select c0, c1 from public.tlr_t{}_dst_{} order by 1, 2;'
        select_sum_cmd = 'select sum(c0) from public.tlr_t{}_dst_{};'
        res = sorted(2 * result)
        for i in range(tbl_num // 2):
            cursor.execute(select_cmd.format(i, dst_num))
            assert cursor.fetchall() == res
        for i in range(tbl_num // 2, tbl_num):
            cursor.execute(select_sum_cmd.format(i, dst_num))
            assert cursor.fetchall() == result_large

    def _run_dml_on_tables(self, cluster, cursor, dst_num, tbl_num):
        update_cmd = "update public.tlr_t{}_dst_{} set c1=c1+'z';"
        delete_cmd = "delete from public.tlr_t{}_dst_{} where c0 % 2 = 1;"
        ins_sel_cmd = ("insert into public.tlr_t{}_dst_{}(c0,c1) select c0,c1 "
                        "from public.tlr_t{}_dst_{};")
        self._start_and_wait_for_refresh(cluster)
        cursor.execute("begin;")
        cursor.execute("set query_group to burst;")
        for i in range(tbl_num):
            cursor.execute(update_cmd.format(i, dst_num))
            cursor.execute(delete_cmd.format(i, dst_num))
            cursor.execute(ins_sel_cmd.format(i, dst_num, i, dst_num))
            self._check_last_query_bursted(cluster, cursor)
        cursor.execute("commit;")

    def _validate_id_sortedness(self, cluster, dst_num, tbl_num):
        for i in range(tbl_num):
            xpx_cmd = ("xpx 'sortedness_checker "
                       "tlr_t{}_dst_{} 0 1';".format(i, dst_num))
            run_bootstrap_sql(cluster, xpx_cmd)

    def test_burst_write_tlr_one(self, cluster):
        """
        Test burst write with manually backup and table level restore.
        There are 2 rounds of backup and one round of restore from 1st round.
        1st backup: Burst insert and backup.
        2nd backup: Busrt update, delete after 1st backup and create 2nd backup.
        Restore tables from the 1st and validate content.
        """
        cluster.run_xpx('auto_worker disable both')
        sortkeys = ['', 'sortkey(c1, c0)']
        timestamp = datetime.now().strftime('%m-%d-%Y-%H-%M-%S')
        snapshot_1 = "burst-write-tlr-snap-1-" + timestamp
        snapshot_2 = "burst-write-tlr-snap-2-" + timestamp
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        with db_session_master.cursor() as cursor:
            self.setup_test(cluster, db_session_master, sortkeys, snapshot_1,
                            snapshot_2)
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")
            tbl_num = 2 * len(sortkeys)
            dst_num = 1
            self._restore_all_tables(cluster, snapshot_1, dst_num, tbl_num)
            with db_session_master.cursor() as cursor:
                self._validate_table_content(cursor, dst_num, tbl_num, RESULT,
                                             RESULT_LARGE)
                self._run_dml_on_tables(cluster, cursor, dst_num, tbl_num)
                self._validate_table_content(cursor, dst_num, tbl_num,
                                             RESULT_TWO, RESULT_LARGE_TWO)
                self._validate_id_sortedness(cluster, dst_num, tbl_num)
            self.teardown_test(cluster, snapshot_1, snapshot_2, tbl_num)

    def test_burst_write_tlr_two(self, cluster):
        """
        Test burst write with manually backup and table level restore.
        There are 2 rounds of backup and one round of restore from 2nd backup.
        1st backup: Burst insert and backup.
        2nd backup: Busrt update, delete after 1st backup and create 2nd backup.
        Restore tables from the 2nd backup and validate content.
        """
        cluster.run_xpx('auto_worker disable both')
        sortkeys = ['', 'sortkey(c1, c0)']
        timestamp = datetime.now().strftime('%m-%d-%Y-%H-%M-%S')
        snapshot_1 = "burst-write-tlr-snap-1-" + timestamp
        snapshot_2 = "burst-write-tlr-snap-2-" + timestamp
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        with db_session_master.cursor() as cursor:
            self.setup_test(cluster, db_session_master, sortkeys, snapshot_1,
                            snapshot_2)
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to burst;")
            tbl_num = 2 * len(sortkeys)
            dst_num = 2
            self._restore_all_tables(cluster, snapshot_2, dst_num, tbl_num)
            with self.db.cursor() as cursor:
                self._validate_table_content(cursor, dst_num, tbl_num,
                                             RESULT_TWO, RESULT_LARGE_TWO)
                self._run_dml_on_tables(cluster, cursor, dst_num, tbl_num)
                self._validate_table_content(cursor, dst_num, tbl_num,
                                             RESULT_THREE, RESULT_LARGE_THREE)
                self._validate_id_sortedness(cluster, dst_num, tbl_num)
            self.teardown_test(cluster, snapshot_1, snapshot_2, tbl_num)
