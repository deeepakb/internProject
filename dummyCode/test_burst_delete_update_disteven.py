# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import logging

from raff.common.db.session import DbSession
from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import burst_write_basic_gucs
from raff.burst.burst_test import setup_teardown_burst
from test_burst_delete_update_distkey import load_tables_in_ss_mode

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst]

DROP_TABLE = "DROP TABLE IF EXISTS public.{}"
GRANT_STMT = "GRANT ALL ON public.{} TO PUBLIC;"


class BurstDeleteUpdateDistevenBase(BurstWriteTest):
    def _setup_table(self, cluster, cursor, tbl_name):
        self._run_bootstrap_sql(cluster, DROP_TABLE.format(tbl_name))
        crate_cmd = "CREATE TABLE public.{}(i integer) diststyle even;"
        cursor.execute(crate_cmd.format(tbl_name))
        cursor.execute(GRANT_STMT.format(tbl_name))
        cursor.execute("INSERT INTO {}"
                       " SELECT strtol(crc32(l_comment), 16) / 4294698"
                       " FROM tpch1_lineitem_redshift;".format(tbl_name))
        cursor.execute("INSERT INTO {}"
                       " SELECT strtol(crc32(o_comment), 16) / 4294698"
                       " FROM tpch1_orders_redshift;".format(tbl_name))
        cursor.execute("INSERT INTO {}"
                       " SELECT strtol(crc32(ps_comment), 16) / 4294698"
                       " FROM tpch1_partsupp_redshift;".format(tbl_name))
        for i in range(9):
            self._run_bootstrap_sql(cluster,
                                    DROP_TABLE.format(tbl_name + str(i)))

    def _run_txn_yaml_files(self, cluster, db_session, start, end):
        for i in range(start, end):
            self._start_and_wait_for_refresh(cluster)
            exec_file = "burst_delete_update_disteven_{}".format(i)
            self.execute_test_file(exec_file, session=db_session)

    def base_burst_delete_update_disteven_one(self, cluster):
        tbl_name = "t"
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        with db_session_master.cursor() as cursor:
            self._setup_table(cluster, cursor, tbl_name)
            self._start_and_wait_for_refresh(cluster)
            self._run_txn_yaml_files(cluster, db_session_master, 0, 6)

    def base_burst_delete_update_disteven_two(self, cluster):
        tbl_name = "t"
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        with db_session_master.cursor() as cursor:
            self._setup_table(cluster, cursor, tbl_name)
            for i in range(6):
                exec_file = "burst_delete_update_disteven_{}".format(i)
                self.execute_test_file(exec_file, session=db_session_master)
            self._start_and_wait_for_refresh(cluster)
            self._run_txn_yaml_files(cluster, db_session_master, 6, 10)


@pytest.mark.serial_only
@pytest.mark.custom_burst_gucs(gucs=burst_write_basic_gucs)
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.cluster_only
@pytest.mark.load_data
class TestBurstDeleteUpdatedistevenCluster(BurstDeleteUpdateDistevenBase):
    def test_burst_delete_update_disteven_cluster_one(self, cluster):
        self.base_burst_delete_update_disteven_one(cluster)

    def test_burst_delete_update_disteven_cluster_two(self, cluster):
        self.base_burst_delete_update_disteven_two(cluster)

@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs={'burst_enable_write': 'true'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstDeleteUpdatedistevenSS(BurstDeleteUpdateDistevenBase):
    def test_burst_delete_update_disteven_ss_one(self, cluster):
        load_tables_in_ss_mode(self, cluster)
        self.base_burst_delete_update_disteven_one(cluster)

    def test_burst_delete_update_disteven_ss_two(self, cluster):
        load_tables_in_ss_mode(self, cluster)
        self.base_burst_delete_update_disteven_two(cluster)
