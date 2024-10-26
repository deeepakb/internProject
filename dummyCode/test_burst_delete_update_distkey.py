# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
import logging

from raff.common.db.session import DbSession
from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import burst_write_basic_gucs
from raff.burst.burst_test import setup_teardown_burst

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst]

DROP_TABLE = "DROP TABLE IF EXISTS public.{}"
GRANT_STMT = "GRANT ALL ON public.{} TO PUBLIC;"


def setup_tpch1_lineitem_redshift(base_class, cluster):
    tbl_name = 'tpch1_lineitem_redshift'
    crate_cmd = """CREATE TABLE public.{} (
                l_orderkey int8 NOT NULL encode mostly32,
                l_partkey int4 NOT NULL,
                l_suppkey int4 NOT NULL,
                l_linenumber int4 NOT NULL encode delta,
                l_quantity numeric(12,2) NOT NULL encode bytedict,
                l_extendedprice numeric(12,2) NOT NULL encode mostly32,
                l_discount numeric(12,2) NOT NULL encode delta,
                l_tax numeric(12,2) NOT NULL encode delta,
                l_returnflag char(1) NOT NULL,
                l_linestatus char(1) NOT NULL encode runlength,
                l_shipdate timestamp NOT NULL encode delta,
                l_commitdate timestamp NOT NULL encode delta,
                l_receiptdate timestamp NOT NULL encode delta,
                l_shipinstruct char(25) NOT NULL encode bytedict,
                l_shipmode char(10) NOT NULL encode bytedict,
                l_comment varchar(44) NOT NULL encode text255,
                PRIMARY Key(L_ORDERKEY, L_LINENUMBER)
                ) distkey(l_orderkey) sortkey(l_shipdate,l_orderkey)"""
    copy_cmd = """
               COPY tpch1_lineitem_redshift FROM 's3://tpc-h/1/lineitem.tbl'
               IAM_ROLE 'arn:aws:iam::467896856988:role/Redshift-S3-Write'
               GZIP DELIMITER '|';
               """
    base_class._run_bootstrap_sql(cluster, DROP_TABLE.format(tbl_name))
    with base_class.db.cursor() as cursor:
        cursor.execute(crate_cmd.format(tbl_name))
        cursor.execute(GRANT_STMT.format(tbl_name))
        cursor.execute(copy_cmd)

def setup_tpch1_orders_redshift(base_class, cluster):
    tbl_name = 'tpch1_orders_redshift'
    crate_cmd = """CREATE TABLE public.{} (
        o_orderkey int8 NOT NULL encode mostly32,
        o_custkey int8 NOT NULL encode mostly32,
        o_orderstatus char(1) NOT NULL encode lzo,
        o_totalprice numeric(12,2) NOT NULL encode mostly32,
        o_orderdate timestamp NOT NULL encode lzo,
        o_orderpriority char(15) NOT NULL encode bytedict,
        o_clerk char(15) NOT NULL encode lzo,
        o_shippriority int4 NOT NULL encode lzo,
        o_comment varchar(79) NOT NULL encode text255,
        PRIMARY Key(O_ORDERKEY)
        ) distkey(o_orderkey) sortkey(o_orderdate, o_orderkey)"""
    copy_cmd = """
               COPY tpch1_orders_redshift FROM 's3://tpc-h/1/orders.tbl'
               IAM_ROLE 'arn:aws:iam::467896856988:role/Redshift-S3-Write'
               GZIP DELIMITER '|';
               """
    base_class._run_bootstrap_sql(cluster, DROP_TABLE.format(tbl_name))
    with base_class.db.cursor() as cursor:
        cursor.execute(crate_cmd.format(tbl_name))
        cursor.execute(GRANT_STMT.format(tbl_name))
        cursor.execute(copy_cmd)

def setup_tpch1_partsupp_redshift(base_class, cluster):
    tbl_name = 'tpch1_partsupp_redshift'
    crate_cmd = """CREATE TABLE public.{} (
                    ps_partkey int8 not null,
                    ps_suppkey int4 not null,
                    ps_availqty int4 not null,
                    ps_supplycost numeric(12,2) not null,
                    ps_comment varchar(199) not null,
                    Primary Key(PS_PARTKEY, PS_SUPPKEY)
                    ) distkey(ps_partkey) sortkey(ps_partkey);
                    """
    copy_cmd = """
               COPY tpch1_partsupp_redshift FROM 's3://tpc-h/1/partsupp.tbl'
               IAM_ROLE 'arn:aws:iam::467896856988:role/Redshift-S3-Write'
               GZIP DELIMITER '|';
               """
    base_class._run_bootstrap_sql(cluster, DROP_TABLE.format(tbl_name))
    with base_class.db.cursor() as cursor:
        cursor.execute(crate_cmd.format(tbl_name))
        cursor.execute(GRANT_STMT.format(tbl_name))
        cursor.execute(copy_cmd)

def load_tables_in_ss_mode(base_class, cluster):
    setup_tpch1_lineitem_redshift(base_class, cluster)
    setup_tpch1_orders_redshift(base_class, cluster)
    setup_tpch1_partsupp_redshift(base_class, cluster)

class BurstDeleteUpdateDistkeyBase(BurstWriteTest):
    def _setup_table(self, cluster, cursor, tbl_name):
        self._run_bootstrap_sql(cluster, DROP_TABLE.format(tbl_name))
        crate_cmd = "CREATE TABLE public.{}(i integer distkey);"
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
            exec_file = "burst_delete_update_distkey_{}".format(i)
            self.execute_test_file(exec_file, session=db_session)

    def base_burst_delete_update_distkey_one(self, cluster):
        tbl_name = "t"
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        with db_session_master.cursor() as cursor:
            self._setup_table(cluster, cursor, tbl_name)
            self._start_and_wait_for_refresh(cluster)
            self._run_txn_yaml_files(cluster, db_session_master, 0, 6)

    def base_burst_delete_update_distkey_two(self, cluster):
        tbl_name = "t"
        db_session_master = DbSession(cluster.get_conn_params(user='master'))
        with db_session_master.cursor() as cursor:
            self._setup_table(cluster, cursor, tbl_name)
            for i in range(6):
                exec_file = "burst_delete_update_distkey_{}".format(i)
                self.execute_test_file(exec_file, session=db_session_master)
            self._start_and_wait_for_refresh(cluster)
            self._run_txn_yaml_files(cluster, db_session_master, 6, 11)


@pytest.mark.serial_only
@pytest.mark.custom_burst_gucs(gucs=burst_write_basic_gucs)
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.cluster_only
@pytest.mark.load_data
class TestBurstDeleteUpdateDistkeyCluster(BurstDeleteUpdateDistkeyBase):
    def test_burst_delete_update_distkey_cluster_one(self, cluster):
        self.base_burst_delete_update_distkey_one(cluster)

    def test_burst_delete_update_distkey_cluster_two(self, cluster):
        self.base_burst_delete_update_distkey_two(cluster)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs={'burst_enable_write': 'true'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstDeleteUpdateDistkeySS(BurstDeleteUpdateDistkeyBase):
    def test_burst_delete_update_distkey_ss_one(self, cluster):
        load_tables_in_ss_mode(self, cluster)
        self.base_burst_delete_update_distkey_one(cluster)

    def test_burst_delete_update_distkey_ss_two(self, cluster):
        load_tables_in_ss_mode(self, cluster)
        self.base_burst_delete_update_distkey_two(cluster)

