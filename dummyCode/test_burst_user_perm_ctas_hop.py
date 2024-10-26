# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode, \
    prepare_burst
from raff.burst.burst_write import BurstWriteTest, burst_write_basic_gucs
from raff.common.dimensions import Dimensions
from raff.burst.burst_status import BurstStatus

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

UNKNOWNOID_CTAS_HOP = ("""
create table burst_ctas_hop as (
SELECT DISTINCT Avg(2)                          _n0,
                Max(Cast(_t0_._s0 AS CHAR(10))) _n1
FROM   (SELECT c_custkey _s0
        FROM   customer) _t0_
WHERE  NOT EXISTS (SELECT n_name _n0
                   FROM   nation)
GROUP  BY _t0_._s0
ORDER  BY 1 DESC, 2
LIMIT  1
);
""")
TPCH_SCHEMA = ("""
create table if not exists public.nation(
  n_nationkey integer not null encode mostly8 distkey sortkey,
  n_name char(25) not null encode bytedict,
  n_regionkey integer not null encode mostly8,
  n_comment varchar(152) not null encode lzo);
create table if not exists public.customer(
  c_custkey integer not null encode delta distkey sortkey,
  c_name varchar(25) not null encode lzo,
  c_address varchar(40) not null encode lzo,
  c_nationkey integer not null encode bytedict,
  c_phone char(15) not null encode lzo,
  c_acctbal decimal(12,2) not null encode lzo,
  c_mktsegment char(10) not null encode bytedict,
  c_comment varchar(117) not null encode text32k);
create table if not exists public.lineitem(
  l_orderkey bigint not null encode delta distkey,
  l_partkey integer not null encode lzo,
  l_suppkey integer not null encode lzo,
  l_linenumber integer not null encode mostly8,
  l_quantity decimal(12,2) not null encode bytedict,
  l_extendedprice decimal(12,2) not null encode mostly32,
  l_discount decimal(12,2) not null encode bytedict,
  l_tax decimal(12,2) not null encode bytedict,
  l_returnflag char(1) not null encode lzo,
  l_linestatus char(1) not null encode lzo,
  l_shipdate date not null encode delta,
  l_commitdate date not null encode delta,
  l_receiptdate date not null encode delta,
  l_shipinstruct char(25) not null encode bytedict,
  l_shipmode char(10) not null encode bytedict,
  l_comment varchar(44) not null encode text32k)
  sortkey (l_orderkey, l_quantity);
""")


@pytest.mark.serial_only
@pytest.mark.localhost_only
# Due to DP-50110 this test must
# be run in encrypted mode.
@pytest.mark.encrypted_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstUserPermCTASHop(BurstWriteTest):
    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions(dict(qlcg_enabled=["true", "false"]))

    def _setup_tables(self, cluster, cursor):
        cursor.execute(TPCH_SCHEMA)
        cursor.execute("grant all on nation to public;")
        cursor.execute("grant all on customer to public;")
        cursor.execute("grant all on lineitem to public;")

    def _get_ctas_qid(self, cursor):
        query = ("select query from stl_query where querytxt like '%{}%' "
                 "order by query desc limit 1;")
        cursor.execute(query.format("create table burst_ctas_hop as"))
        qid = cursor.fetch_scalar()
        return qid

    def _run_ctas_on_burst(self, cursor, cluster, should_burst):
        cursor.execute("set query_group to burst")
        log.info("Running CTAS")
        cursor.execute(UNKNOWNOID_CTAS_HOP)
        # Verify auto analyze was running on main
        self._check_last_query_didnt_burst_with_code(
            cluster, cursor, BurstStatus.in_eligible_query_type)
        # Verify CTAS query was running on burst
        qid = self._get_ctas_qid(cursor)
        log.info("Done CTAS qid: {}".format(qid))
        if should_burst == 'true':
            self.verify_query_bursted(cluster, qid)
        else:
            self.verify_query_didnt_bursted(cluster, qid)

    def _verify_ctas_catalog(self, cursor):
        catalog_check = ("select btrim(attname), atttypid, atttypmod "
                         "from pg_attribute where attrelid = "
                         "'burst_ctas_hop'::regclass::oid and attnum=2;")
        cursor.execute(catalog_check)
        res = cursor.fetchall()
        assert res == [
            (
                '_n1',
                1043,
                14,
            ),
        ], "CTAS table Catalog is not updated"

    def _get_table_contents(self, cursor, cluster, table, is_burst):
        if is_burst:
            cursor.execute("set query_group to burst")
        else:
            cursor.execute("set query_group to noburst")
        cursor.execute("SELECT * from {} order by 1,2".format(table))
        res = cursor.fetchall()
        if is_burst:
            self._check_last_query_bursted(cluster, cursor)
        return res

    def _verify_table_content(self, cursor, cluster):
        expect_res = [(1, 'AAAAAAAAAA'), (1000, 'BBBBBBBBBB')]
        res = self._get_table_contents(cursor, cluster, "burst_ctas_hop",
                                       False)
        assert res == expect_res
        res = self._get_table_contents(cursor, cluster, "burst_ctas_hop", True)
        assert res == expect_res

    def _perform_dmls(self, cursor, cluster):
        cursor.execute("set query_group to burst")
        cursor.execute("INSERT INTO burst_ctas_hop VALUES (1,'AAAAAAAAAA');")
        self._check_last_query_bursted(cluster, cursor)
        cursor.execute(
            "INSERT INTO burst_ctas_hop VALUES (1000,'BBBBBBBBBB');")
        self._check_last_query_bursted(cluster, cursor)

    def test_burst_user_perm_ctas_hop(self, cluster_session,
                                      cluster, db_session, vector):
        """
        This test triggers a CTAS that will hop and needs to validate and fix
        CTAS table metadata.
        """

        guc = dict(list(burst_write_basic_gucs.items()) + [
                  ('burst_enable_write_user_ctas', 'true'),
                  ('enable_burst_failure_handling', 'true'),
                  ('burst_enable_ctas_failure_handling', 'true'),
                  ('enable_ctas_hop', 'true'),
                  ('enable_reg_ctas_hop', 'true'),
                  ('enable_query_level_code_generation', vector.qlcg_enabled)])

        # This will cause main to reboot causing Super Simulated Mode to break,
        # so we need to restart SSM.
        with cluster_session(gucs=guc):
            prepare_burst({
                'burst_enable_write': 'true',
                'burst_enable_write_user_ctas': 'true',
                'enable_ctas_hop': 'true',
                'enable_reg_ctas_hop': 'true'
            })
            with db_session.cursor() as cursor:
                cursor.execute("SET query_group TO BURST;")
                self._setup_tables(cluster, cursor)
                self._start_and_wait_for_refresh(cluster)
                # If QLCG is disabled CTAS should only run on main.
                self._run_ctas_on_burst(cursor, cluster,
                                        vector.qlcg_enabled)
                self._start_and_wait_for_refresh(cluster)
                self._verify_ctas_catalog(cursor)
                self._perform_dmls(cursor, cluster)
                self._verify_table_content(cursor, cluster)
