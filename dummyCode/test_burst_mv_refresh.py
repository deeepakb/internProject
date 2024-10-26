# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import paramiko
import pytest

from itertools import product
from string import hexdigits
from random import randrange
from contextlib import contextmanager
from collections import namedtuple
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.common import retry
from raff.burst.burst_write import BurstWriteTest, burst_write_mv_gucs
from raff.common.cred_helper import get_key_auth_str
from raff.common.profile import Profiles
from raff.burst.burst_test import setup_teardown_burst
from raff.common.host_type import HostType
from raff.burst.burst_write import BURST_OWN, MAIN_UNDO, OWNERSHIP_STATE_CHECK_QUERY
import raff.burst.remote_exec_helpers as helpers

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst]

CREATE_MV = ("CREATE MATERIALIZED VIEW {} BACKUP YES {} {} AUTO REFRESH NO"
             " AS ({})")
VC_LEN = 64
# Added some more default columns for testing
CREATE_TABLE = ("CREATE TABLE IF NOT EXISTS {} ("
                " c0 int, c1 int, c2 varchar(" + str(VC_LEN) + "), "
                " c3 int default 2,"
                " c4 int default 3,"
                " c5 int default 5,"
                " c6 int default 7"
                " ) {} {}")
DELETE = "DELETE FROM {}"
DROP_MV = "DROP MATERIALIZED VIEW IF EXISTS {}"
DROP_TABLE = "DROP TABLE IF EXISTS {} CASCADE"
DROP_SCHEMA = "DROP SCHEMA IF EXISTS {} CASCADE"
GET_TABLE_ID = "SELECT '{}'::regclass::oid"
INSERT = ("INSERT INTO {} VALUES {}")
MV_STALENESS = ("select trim(name), trim(is_stale)"
                " from stv_mv_info where name = '{}';")
REFRESH_MV = "REFRESH MATERIALIZED VIEW {}"
SELECT = "SELECT * FROM {}"
MY_SCHEMA = "test_schema"
MY_USER = "test_user"
MY_TABLE = "test_table"
CREATE_TEST_TABLE = ("create table if not exists " + MY_TABLE + " (c0 int)"
                     " diststyle even")
UPDATE = "UPDATE {} SET c0 = c0 + 2"
GET_MV_REFRESH_QID = ("select query from stl_query where xid in "
                      " (select xid from stl_mv_refresh where mvoid in "
                      " (select '{}.{}'::regclass::oid) "
                      " order by endtime desc limit 1)"
                      " and userid in "
                      "(select usesysid from pg_user where usename = '{}')"
                      " and querytxt like '%{}%' "
                      " order by query")

GET_QUERY_CMD_TYPE = ("select query_cmd_type from stl_internal_query_details "
                      " where query = {}")
ROWID_CHECK = ("select oid, count(*) from {}.{} group by oid "
               "having count(*) > 1 order by 1 limit 20;")

SORTEDNESS_CHECK = ("set search_path to {0};"
                    " xpx 'sortedness_checker {1}'")

PAR_SORTEDNESS_CHECK = ("set search_path to {0};"
                        "xpx 'table_partition_sortedness_checker {1}'")

DIST_CHECK = ("set search_path to {0};"
              "xpx 'check_distribution {1}'")


# 1 = insert
# 2 = delete
# 4 = CTAS
# 9 = select
MV_REFRESH_QTYPES = {1, 2, 4, 9}

# Deprecated in favor of mv_configs_t
rdata = namedtuple('rdata', ['tables', 'mv', 'mv_query', 't_dist', 't_sort',
                             'mv_dist', 'mv_sort'])
mv_configs_t = namedtuple('mvc', [
    'tables', 't_dists', 't_sorts', 'mvs', 'mv_queries', 'mv_dists', 'mv_sorts'
])

# query = SQL to create MV
# num_tables = number of base tables of MV
mvq = namedtuple('mvq', ['query', 'num_tables'])

diststyle = ['diststyle even', "distkey(c0)"]
sortkey = ['sortkey(c0)', '']
DML = [
        "INSERT",
        "UPDATE",
        "DELETE",
        "COPY"
      ]
MV_QUERIES = [
                # Materialized views do not support ORDER BY clauses
                # GROUP BY does not as MV-refresh makes temp tables
                mvq("select * from {0}", 1),
                mvq(("select c0, 2*c1 as c1 from {0} t0"
                     " where c0 > 1 and c1 < 10"), 1),
                # except: does not burst since CTAS
                # mvq(("select * from {0} except select * from {1}"), 2),
                # nested: does not burst since CTAS
                # mvq(("select * from {0} where c0 in (select c1 from {1}"
                #     " where c1 > 5)"), 2),
                # coalesce
                mvq("select c0, coalesce(NULL, c2) from {0}", 1),
                # mod
                mvq("select mod(c1,3), c0 from {0}", 1),
                # union
                mvq("select * from {0} union all select * from {1}", 2),
                mvq(("select * from {0} where c0 < 5 union all"
                     " select * from {1} where c0 + c1 > 10 union all"
                     " select * from {2} where c0 > 5"), 3),
                # join
                mvq(("select t0.c0 as c0, t1.c1 as c1, t2.c2 as c2 "
                     " from {0} as t0, {1} as t1, {2} as t2"
                     " where t0.c0 = t1.c0 and t1.c1 = t2.c1"), 3),
                mvq(("select t0.c0 as c0, t1.c1 as c1, t1.c2 as c2"
                     " from {0} t0 inner join {1} t1 on t0.c0 = t1.c0"), 2),
                # does not burst since CTAS
                # mvq(("select t0.c0 from {0} t0 left join {1} t1 on"
                #     " t0.c0 = t0.c1"), 2),
                # cast
                mvq(("select cast(c0 as bpchar) as c0, cast(c1 as text) as c1"
                     " from {0} t0"), 1),
                # repeat, crc
                mvq(("select repeat('a', c0) as c0, crc32(c1) as c1,"
                     " len(c2) as c2 from {0} t0"), 1),
                # len
                mvq(("select * from {0} t0 where len(repeat('a', c0)) > 4"), 1)
             ]

BURST_DIRTY = [("Burst", "Dirty")]
BURST_OWN = [("Burst", "Owned")]
CLEAN = []
MAIN_OWN = [("Main", "Owned")]
MAIN_UNDO = [("Main", "Undo")]
STRUCT_CHANGE = [('Main', 'Structure Changed')]

MV_INTERNAL_TABLE = "mv_tbl__{}__0"

@pytest.mark.serial_only
@pytest.mark.skip_load_data
class TestBurstWriteMVBase(BurstWriteTest):

    # This controls if the test runs on burst or main
    # Running on main is useful for debugging
    run_on_burst = True

    # Controls whether to use legacy or unified Burst
    use_unified_burst = False

    def _check_mv_health(self, cluster, cursor, mv_configs, burst=True):
        try_burst = should_burst = burst
        for mvd in mv_configs:
            self._do_dmls(cluster, cursor, mvd,
                          try_burst, should_burst, ["INSERT"], ["COMMIT"])

    def _reset_conn_to_main(self, cluster):
        # sudo_sql creates and saves connection to a cluster. So if its used to
        # talk to a burst cluster, we need to reset the conn to main before
        # using it again.
        if cluster.host_type != HostType.CLUSTER:
            return
        return self.sudo_sql(cluster, "select now()", new_conn=True)

    def _check_last_copy_bursted(self, cluster, cursor):
        if not self.run_on_burst:
            return
        cursor.execute("select pg_last_copy_id();")
        qid = cursor.fetch_scalar()
        self._check_query_bursted(cluster, qid)

    def _check_last_copy_didnt_burst(self, cluster, cursor):
        cursor.execute("select pg_last_copy_id();")
        qid = cursor.fetch_scalar()
        self._check_query_didnt_burst(cluster, qid)

    def _check_last_query_didnt_burst(self, cluster, cursor):
        qid = self.last_query_id(cursor)
        return self._check_query_didnt_burst(cluster, qid)

    def _check_query_didnt_burst(self, cluster, qid):
        query = """
        select concurrency_scaling_status
        from stl_query
        where query = {} limit 1
        """.format(qid)
        res = 0
        if cluster.host_type == HostType.CLUSTER:
            nested_lst = self.sudo_sql(cluster, query)
            nested_lst_of_tuples = [tuple(nl) for nl in nested_lst]
            res = int(nested_lst_of_tuples[0][0])
        else:
            with self.db.cursor() as cursor:
                cursor.execute(query)
                res = cursor.fetch_scalar()
        assert res != 1, "Query {} not expected to burst".format(qid)
        return res

    def _check_last_query_bursted(self, cluster, cursor):
        if not self.run_on_burst:
            return
        qid = self.last_query_id(cursor)
        self._check_query_bursted(cluster, qid)

    def _get_burst_status_code(self, cluster, query_id):
        """
        Helper method to get the status of a burst query (code representation)
        """
        query = """
        select concurrency_scaling_status
        from stl_query
        where query = {} limit 1
        """.format(query_id)
        if cluster.host_type == HostType.CLUSTER:
            nested_lst = self.sudo_sql(cluster, query)
            nested_lst_of_tuples = [tuple(nl) for nl in nested_lst]
            res = int(nested_lst_of_tuples[0][0])
        else:
            with self.db.cursor() as cursor:
                cursor.execute(query)
                res = cursor.fetch_scalar()
        return res

    def _check_query_bursted(self, cluster, qid):
        query = """
        select concurrency_scaling_status
        from stl_query
        where query = {} limit 1
        """.format(qid)
        res = 0
        if cluster.host_type == HostType.CLUSTER:
            nested_lst = self.sudo_sql(cluster, query)
            nested_lst_of_tuples = [tuple(nl) for nl in nested_lst]
            res = int(nested_lst_of_tuples[0][0])
        else:
            with self.db.cursor() as cursor:
                cursor.execute(query)
                res = cursor.fetch_scalar()
        assert res == 1, "Query {} expected to burst".format(qid)

    def _rand_str(self, slen=64):
        return "".join([hexdigits[randrange(len(hexdigits)-1)]
                        for _ in range(slen)])

    def _values(self, num=10):
        return ",".join(["({},{},'{}')".format(i, i*2, self._rand_str(VC_LEN))
                         for i in range(num)])

    def _check_table(self, cluster, schema, table, diststyle):
        if not self.run_on_burst:
            return
        if cluster.host_type == HostType.LOCALHOST:
            self._validate_table(cluster, schema, table, diststyle)
        else:
            res = self.sudo_sql(cluster, ROWID_CHECK.format(schema, table))
            assert res == []
            self.sudo_sql(cluster, SORTEDNESS_CHECK.format(schema, table))
            self.sudo_sql(cluster, PAR_SORTEDNESS_CHECK.format(schema, table))
            if 'distkey' in diststyle:
                self.sudo_sql(cluster, DIST_CHECK.format(schema, table))

    def _check_ownership(self, schema, tbl, state, cluster=None):
        if not self.run_on_burst:
            return
        if cluster and cluster.host_type == HostType.LOCALHOST:
            self._validate_ownership_state(schema, tbl, state, self.use_unified_burst)
        else:
            query = OWNERSHIP_STATE_CHECK_QUERY.format(schema, tbl)
            res = self.sudo_sql(cluster, query)
            """
            The state arg can be empty like [] or non-empty like [(a,b)].
            But the results from run_bootstrap are either [] or [[a,b]]. So we
            need to handle the two cases separately. Case 1 is to handle empty
            lists and Case 2 is to convert results from run_bootstrap to a list
            of tuples. We cannot just do state == res like how we do for
            super-sim tests in the method _validate_ownership_state.

            state is like [(a,b)] or []
            res is like [[a,b]] or []
            """
            if len(state) == 0:
                assert state == res, \
                       "Ownership check failed for {}".format(tbl)
            else:
                assert state[0] == tuple(res[0]), \
                       "Ownership check failed for {}".format(tbl)

    def _get_query_type(self, cluster, cursor, qid):
        query = GET_QUERY_CMD_TYPE.format(qid)
        if cluster and cluster.host_type == HostType.LOCALHOST:
            cursor.execute(query)
            return cursor.fetch_scalar()
        else:
            _qtype = self.sudo_sql(cluster, query)
            return int(_qtype[0][0])

    def _check_last_mv_refresh_bursted(self, cluster, cursor, mv_name):
        if not self.run_on_burst:
            return
        mv_internal_table = MV_INTERNAL_TABLE.format(mv_name)
        query = GET_MV_REFRESH_QID.format(MY_SCHEMA, mv_name, MY_USER,
                                          mv_internal_table)
        if cluster and cluster.host_type == HostType.LOCALHOST:
            cursor.execute(query)
            query_ids = cursor.fetchall()
        else:
            query_ids = self.sudo_sql(cluster, query)
        log.info("MV-refresh query ids = {}".format(query_ids))
        for i, qid in enumerate(query_ids):
            self._check_query_bursted(cluster, qid[0])
            qtype = self._get_query_type(cluster, cursor, qid[0])
            assert qtype in MV_REFRESH_QTYPES, \
                    ("Unexpected query type = {} "
                     "in MV-refresh for query = {}").format(qtype, qid[0])

    def _check_last_mv_refresh_didnt_burst(self, cluster, cursor, mv_name):
        if not self.run_on_burst:
            return
        mv_internal_table = MV_INTERNAL_TABLE.format(mv_name)
        query = GET_MV_REFRESH_QID.format(MY_SCHEMA, mv_name, MY_USER,
                                          mv_internal_table)
        if cluster and cluster.host_type == HostType.LOCALHOST:
            cursor.execute(query)
            query_ids = cursor.fetchall()
        else:
            query_ids = self.sudo_sql(cluster, query)
        log.info("MV-refresh query ids = {}".format(query_ids))
        for i, qid in enumerate(query_ids):
            self._check_query_didnt_burst(cluster, qid[0])
            qtype = self._get_query_type(cluster, cursor, qid[0])
            assert qtype in MV_REFRESH_QTYPES, \
                    ("Unexpected query type = {} "
                     "in MV-refresh for query = {}").format(qtype, qid[0])

    def _check_mv_stale_on_main(self, cluster, cursor, mv, is_stale):
        """
            Validates MV is stale or not on main cluster
            mv - name of mv to check
            is_stale - State the MV should be in. True if MV expected to be
            stale. False if MV is not expected to be stale.
        """
        expected = [(mv, 't' if is_stale else 'f')]
        query = MV_STALENESS.format(mv)
        if cluster and cluster.host_type == HostType.LOCALHOST:
            cursor.execute(query)
            res = cursor.fetchall()
        else:
            res = self.sudo_sql(cluster, query)
        log.info('mv_staleness = {}'.format(res))
        # This mapping is needed because of RAFF. Rows from localhost are
        # returned as a list of tuples but rows from cluster are returned as a
        # list of lists.
        assert expected == list([tuple(i) for i in res]), \
               ("[FAIL]: Expected MV {} to be {} on main"
                " cluster").format(mv, "stale" if is_stale else "refreshed")

    def _check_state(self, table_state, mv_state, mv_configs, cluster):
        """
            Checks if base tables of MV and the MV are in a given state.
            Fails the test if they are not in the given state.
            Args:
                table_state (str): Burst-state of base-table
                mv_state (str): Burst-state of MV
                mv_configs (list): List of MVs and related info
                cluster (obj): Cluster object
        """
        assert isinstance(mv_configs, mv_configs_t)
        for tid, table in enumerate(mv_configs.tables):
            self._check_ownership(MY_SCHEMA, table, table_state, cluster)
            self._check_table(cluster, MY_SCHEMA, table,
                              mv_configs.t_dists[tid])

        for mvid, mv in enumerate(mv_configs.mvs):
            mv_internal_table = MV_INTERNAL_TABLE.format(mv)
            self._check_ownership(MY_SCHEMA, mv_internal_table, mv_state,
                                  cluster)
            self._check_table(cluster, MY_SCHEMA, mv_internal_table,
                              mv_configs.mv_dists[mvid])

    def _print_diff_on_main(self, cluster, cursor, mv_configs):
        """
            Prints the diff between base-tables and MV
            ONLY FOR DEBUGGING.
        """

        # Turn off automatic query re-writing using MV
        cursor.execute('show mv_enable_aqmv_for_session')
        aqmv = cursor.fetch_scalar()
        cursor.execute('set mv_enable_aqmv_for_session to false')

        # Set to run on main
        cursor.execute("show query_group")
        qg = cursor.fetch_scalar()
        cursor.execute("set query_group to metrics")

        for mvid, mv in enumerate(mv_configs.mvs):
            mv_query = mv_configs.mv_queries[mvid]
            log.info("mv, mv_query = {}, {}".format(mv, mv_query))

            cursor.execute("({}) except (select * from {})".format(
                mv_query, mv))
            res = cursor.fetchall()
            self._check_last_query_didnt_burst(cluster, cursor)
            log.info("Diff base-table & MV = {}".format(res))

            cursor.execute("(select * from {}) except ({})".format(
                mv, mv_query))
            res = cursor.fetchall()
            self._check_last_query_didnt_burst(cluster, cursor)
            log.info("Diff MV & base-table = {}".format(res))

        cursor.execute("set query_group to {}".format(qg))
        cursor.execute('set mv_enable_aqmv_for_session to {}'.format(aqmv))

    def _check_data_is_same(self, cluster, cursor, tables, mv, mv_query,
                            check_burst=True):
        """
            Validates that following 4 tables have same rows
                - table on main
                - MV on main
                - table on burst
                - MV on burst
        """
        if not self.run_on_burst:
            return

        log.info("Validating data is same on main and burst clusters")

        # Turn off automatic query re-writing using MV
        cursor.execute('show mv_enable_aqmv_for_session')
        aqmv = cursor.fetch_scalar()
        cursor.execute('set mv_enable_aqmv_for_session to false')

        # Compare data between table and materialized view on main cluster
        cursor.execute("set query_group to metrics")
        cursor.execute("({}) except (select * from {})".format(mv_query, mv))
        res = cursor.fetchall()
        self._check_last_query_didnt_burst(cluster, cursor)
        assert res == [], ("[FAIL]: Expected {} and {} "
                           "to have same data").format(tables, mv)

        cursor.execute("(select * from {}) except ({})".format(mv, mv_query))
        res = cursor.fetchall()
        self._check_last_query_didnt_burst(cluster, cursor)
        assert res == [], ("[FAIL]: Expected {} and {} "
                           "to have same data").format(tables, mv)

        cursor.execute(SELECT.format(mv))
        mv_rows_on_main = tuple(sorted(cursor.fetchall()))
        self._check_last_query_didnt_burst(cluster, cursor)

        cursor.execute(mv_query)
        table_rows_on_main = tuple(sorted(cursor.fetchall()))
        self._check_last_query_didnt_burst(cluster, cursor)

        if not check_burst:
            assert mv_rows_on_main == table_rows_on_main, \
                   ("[FAIL]: Expected same MV & table data on main cluster")
            cursor.execute("set query_group to burst")
            return

        # Compare data between table and materialized view on burst cluster
        cursor.execute("set query_group to burst")
        cursor.execute("({}) except (select * from {})".format(mv_query, mv))
        res = cursor.fetchall()
        self._check_last_query_bursted(cluster, cursor)
        assert res == [], ("[FAIL]: Expected {} and {} "
                           "to have same data").format(tables, mv)

        cursor.execute("(select * from {}) except ({})".format(mv, mv_query))
        res = sorted(cursor.fetchall())
        self._check_last_query_bursted(cluster, cursor)
        assert res == [], ("[FAIL]: Expected {} and {} "
                           "to have same data").format(tables, mv)

        cursor.execute(SELECT.format(mv))
        mv_rows_on_burst = tuple(sorted(cursor.fetchall()))
        self._check_last_query_bursted(cluster, cursor)

        cursor.execute(mv_query)
        table_rows_on_burst = tuple(sorted(cursor.fetchall()))
        self._check_last_query_bursted(cluster, cursor)

        assert mv_rows_on_main == mv_rows_on_burst, \
               ("[FAIL]: Expected same MV-rows on main & burst clusters")
        assert table_rows_on_main == table_rows_on_burst, \
               ("[FAIL]: Expected same table-rows on main & burst clusters")

        # restore aqmv value
        cursor.execute('set mv_enable_aqmv_for_session to {}'.format(aqmv))

    def _do_dmls(self, cluster, cursor, mvd, try_burst,
                 should_burst, DML, TXN_END):
        """
            tables - base tables of MV
            mv - mv name
            mv_query - Query used to create MV using base tables
            try_burst - True if try to burst, else False
            should_burst - True if query should burst, else false
            DML - dmls to perform on base tables
            TXN_END - commit or abort

            Runs INSERT/DELETE/UPDATE/COPY on the cluster
            It does the following:
                1. Validates data is same on table+MV on main+burst
                2. Begins transaction
                3. Runs DML on burst
                4. Checks MV is not stale on main
                5. Refreshes MV on burst
                6. Checks MV is not stale on main
                7. Commits or Aborts transaction
                8. Checks MV is stale or not on main, depends on commit/abort
                9. Refreshes MV on burst
                10. Checks MV is not stale on main
                11. Validates data is same on table+MV on main+burst
        """

        tables, mv, mv_query, t_dist, t_sort, mv_dist, mv_sort = mvd
        try_burst = try_burst and self.run_on_burst
        log.info("try_burst = {}, should_burst = {}".format(try_burst,
                                                            should_burst))
        if try_burst:
            cursor.execute("set query_group to burst")
        else:
            cursor.execute("set query_group to metrics")
        for dml, txn_end in product(DML, TXN_END):
            cursor.execute("BEGIN;")
            for table in tables:
                if dml == "COPY":
                    self._do_copy(cursor, table)
                elif dml == "INSERT":
                    cursor.execute(INSERT.format(table, self._values()))
                elif dml == "UPDATE":
                    cursor.execute(UPDATE.format(table))
                elif dml == "DELETE":
                    cursor.execute(DELETE.format(table))

                if dml == "COPY":
                    if try_burst and should_burst:
                        self._check_last_copy_bursted(cluster, cursor)
                    elif try_burst and not should_burst:
                        self._check_last_copy_didnt_burst(cluster, cursor)
                else:
                    if try_burst and should_burst:
                        self._check_last_query_bursted(cluster, cursor)
                    elif try_burst and not should_burst:
                        self._check_last_query_didnt_burst(cluster, cursor)

            if txn_end == "ABORT":
                cursor.execute("ABORT;")
                for table in tables:
                    self._check_ownership(MY_SCHEMA, table, MAIN_UNDO,
                                          cluster)
                self._check_mv_stale_on_main(cluster, cursor, mv, False)
                # Note that the MV is not stale at this point, so the refresh
                # below is a no-op and it will not burst regardless whether
                # we are following legacy or unified Burst path.
                cursor.execute(REFRESH_MV.format(mv))
                self._check_last_mv_refresh_didnt_burst(cluster,
                                                        self.db.cursor(),
                                                        mv)
                self._check_mv_stale_on_main(cluster, cursor, mv, False)
            else:
                cursor.execute("COMMIT;")
                self._check_mv_stale_on_main(cluster, cursor, mv, True)
                cursor.execute(REFRESH_MV.format(mv))
                # SIM: https://issues.amazon.com/RedshiftDP-32343
                # This check is flaky. The MV can be falsely marked stale even
                # though its not because of inflight transactions on other random
                # tables. We check for data equality below anyways.
                # self._check_mv_stale_on_main(cluster, cursor, mv, False)
                if try_burst and should_burst:
                    for table in tables:
                        self._check_ownership(MY_SCHEMA, table,
                                              BURST_OWN, cluster)
                    self._check_last_mv_refresh_bursted(cluster,
                                                        self.db.cursor(),
                                                        mv)
                    self._check_data_is_same(cluster, cursor, tables, mv,
                                             mv_query)
                    if self.use_unified_burst:
                        helpers.validate_storage_oid_on_burst_against_main(
                            cluster, remote_host_name="main_cluster",
                            remote_db_name="dev",
                            remote_schema_name=MY_SCHEMA,
                            remote_table_name=MV_INTERNAL_TABLE.format(mv))
                elif try_burst and not should_burst:
                    for table in tables:
                        self._check_ownership(MY_SCHEMA, table,
                                              [("Main", "Undo")], cluster)
                    self._check_last_mv_refresh_didnt_burst(cluster,
                                                            self.db.cursor(),
                                                            mv)

    def _copy_from_s3(self, cluster, cursor, table, s3_bucket):
        # _cookie_core_account_id = '589416946172'
        credential = get_key_auth_str(profile=Profiles.COOKIE_CORE)
        log.info("credential = {}".format(credential))
        source = 's3://{}/{}/'.format(s3_bucket, table)
        cursor.run_copy(table, source, credential, COMPUPDATE="OFF",
                        DELIMITER=",")

    def _unload_to_s3(self, cluster, cursor, table, s3_bucket):
        # _cookie_core_account_id = '589416946172'
        credential = get_key_auth_str(profile=Profiles.COOKIE_CORE)
        log.info("credential = {}".format(credential))
        unload_sql = ("unload ('select * from {0}') to 's3://{1}/{0}/' {2} "
                      "ALLOWOVERWRITE delimiter ',';").format(
                              table, s3_bucket, credential)
        cursor.execute(unload_sql)

    def _verify_tables(self, cluster, schema, tables, dist):
        for tbl in tables:
            self._check_table(cluster, schema, tbl, dist)

    def _verify_owner(self, cluster, schema, tables, owner):
        for tbl in tables:
            self._check_ownership(schema, tbl, owner, cluster)

    def _do_copy(self, cursor, table):
        """
            Copies data from external source into Redshift table
        """
        row_count = 10
        credential = get_key_auth_str(profile=Profiles.COOKIE_CORE)
        log.info("credential = {}".format(credential))
        # _cookie_core_account_id = '589416946172'
        cursor.run_copy(
                table,
                "dynamodb://burst_mv_test",
                credential,
                readratio=100,
                COMPUPDATE="OFF")
        assert cursor.last_copy_row_count() == row_count, \
               ("[FAIL]: Expected {} rows from COPY command".format(row_count))

    def mv_data(self, prefix, diststyle, sortkey, mv_queries):
        """
            Return a list with info to create MVs. Each item in the list has a
            list of base tables, MV name, query to create MV from base tables,
            diststyle and sortkey for each MV. There is one item per Mv in the
            list.

            prefix - common name prefix for MV and its base tables
            mv_queries - Queries used to create MV
        """
        for mv_id, mv_q in enumerate(mv_queries):

            # create internal tables for MVs
            tables = []
            for tbl_id in range(mv_q.num_tables):
                table = "_".join([prefix, "table", str(mv_id), str(tbl_id)])
                tables.append(table)
            mv = "_".join([prefix, "mv", str(mv_id)])

            # Just pick some diststyle and sortkey for both MV and tables
            mv_dist = t_dist = diststyle[mv_id % len(diststyle)]
            mv_sort = t_sort = sortkey[mv_id % len(sortkey)]

            # Make the query to create MVs
            mv_query = mv_q.query.format(*tables)
            res = rdata(tables, mv, mv_query, t_dist, t_sort, mv_dist, mv_sort)
            log.info(res)
            yield res

    @retry(
        expected_exceptions=(paramiko.SSHException, AssertionError),
        retries=10,
        delay_seconds=60)
    def _check_burst_cluster_health(self, cluster, cursor, table):
        """
            Validates burst cluster is healthy by inserting and reading from a
            known test table
        """
        if not self.run_on_burst:
            return
        # Ignore given cursor as cluster may have rebooted since the last time
        # we did a health check. Make a new cursor so that we don't run into
        # stale cursor or unavailable cursor issues.
        health_check_session = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))
        cursor = health_check_session.cursor()
        # Take backup because if the function fails the first time and the
        # health query goes to main then it can't run on burst in the next try.
        cursor.execute("set query_group to metrics")
        cursor.execute(DELETE.format(table))
        self._start_and_wait_for_refresh(cluster)
        log.info("Validate burst cluster is healthy")
        cursor.execute("set query_group to burst")
        cursor.execute(INSERT.format(table, "(4000)"))
        self._check_last_query_bursted(cluster, cursor)
        cursor.execute(SELECT.format(table))
        assert [(4000,)] == cursor.fetchall(), \
               "[FAIL]: Burst cluster is unhealthy"
        self._check_last_query_bursted(cluster, cursor)
        cursor.execute("set query_group to metrics")
        cursor.execute(DELETE.format(table))
        self._print_burst_cluster_info(cluster)

    def _refresh_all_mv(self,
                        try_burst,
                        should_burst,
                        mv_configs,
                        cluster,
                        cursors,
                        mv_owner=None,
                        check_data_on_burst=False,
                        is_stale=None):
        """
            This refreshes all MVs in the test and checks if its bursts or not.

            try_burst (bool) :
                True iff we should try to burst mv-refresh

            should_burst (bool):
                True iff mv-refresh should run on burst

            mv_configs (mv_configs_t):
                List of mv_configs of type mv_configs_t

            cluster:
                Main cluster

            mv_owner:
                State of MV after a mv-refresh

            check_data_on_burst:
                True iff we need to compare data on MV and base-tables
                across both main and burst clusters

            is_stale (bool):
                True iff MV should be stale before mv-refresh.
                If MV is not stale before mv-refresh, then mv-refresh is no-op
                and no write queries will be issued.

        """
        main_cur1, check_cur, bs_cur = cursors
        assert isinstance(mv_configs, mv_configs_t)

        main_cur1.execute("show query_group")
        query_group = main_cur1.fetch_scalar()

        if try_burst:
            main_cur1.execute("set query_group to burst")
        else:
            main_cur1.execute("set query_group to metrics")

        for mvid, mv in enumerate(mv_configs.mvs):
            mv_internal_table = MV_INTERNAL_TABLE.format(mv)
            if is_stale is not None:
                self._check_mv_stale_on_main(cluster, check_cur, mv, is_stale)
            main_cur1.execute(REFRESH_MV.format(mv))
            if try_burst and should_burst:
                self._check_last_mv_refresh_bursted(cluster, bs_cur, mv)
                if check_data_on_burst:
                    self._check_data_is_same(cluster, main_cur1,
                                             mv_configs.tables, mv,
                                             mv_configs.mv_queries[mvid])
            elif not try_burst or (try_burst and not should_burst):
                self._check_last_mv_refresh_didnt_burst(cluster, bs_cur, mv)
            # SIM: https://issues.amazon.com/RedshiftDP-32343
            # This check is flaky. The MV can be falsely marked stale even
            # though its not because of inflight transactions on other random
            # tables. We check for data equality below anyways.
            # self._check_mv_stale_on_main(cluster, check_cur, mv, False)
            if mv_owner is not None:
                self._check_ownership(MY_SCHEMA, mv_internal_table, mv_owner,
                                      cluster)

        main_cur1.execute("set query_group to {}".format(query_group))

    def _do_sql(self,
                sql,
                try_burst,
                should_burst,
                tables,
                cluster,
                cursors,
                num_val=5):
        """
            Runs SQL on given tables. Then checks if it bursts or not.

            sql (str):
                Can be one of: select, copy, insert, delete, update
                example:
                "select insert" will run select and insert on given tables

                To check the order of the SQLs, see the loop below.
                The SQLs are run only once. So "select insert select" is same
                as "select insert"

            try_burst (bool) :
                True iff we should try to burst mv-refresh

            should_burst (bool):
                True iff mv-refresh should run on burst


        """
        main_cur1, = cursors

        main_cur1.execute("show query_group")
        query_group = main_cur1.fetch_scalar()

        if try_burst:
            main_cur1.execute("set query_group to burst")
        else:
            main_cur1.execute("set query_group to metrics")

        for table in tables:
            if "insert" in sql:
                main_cur1.execute(INSERT.format(table, self._values(num_val)))
                if try_burst and should_burst:
                    self._check_last_query_bursted(cluster, main_cur1)
                elif not try_burst or (try_burst and not should_burst):
                    self._check_last_query_didnt_burst(cluster, main_cur1)

            if "delete" in sql:
                main_cur1.execute(DELETE.format(table))
                if try_burst and should_burst:
                    self._check_last_query_bursted(cluster, main_cur1)
                elif not try_burst or (try_burst and not should_burst):
                    self._check_last_query_didnt_burst(cluster, main_cur1)

            if "copy" in sql:
                self._do_copy(main_cur1, table)
                if try_burst and should_burst:
                    self._check_last_copy_bursted(cluster, main_cur1)
                elif not try_burst or (try_burst and not should_burst):
                    self._check_last_copy_didnt_burst(cluster, main_cur1)

            if "select" in sql:
                main_cur1.execute(SELECT.format(table))
                if try_burst and should_burst:
                    self._check_last_query_bursted(cluster, main_cur1)
                elif not try_burst or (try_burst and not should_burst):
                    self._check_last_query_didnt_burst(cluster, main_cur1)

            if "update" in sql:
                main_cur1.execute(UPDATE.format(table))
                if try_burst and should_burst:
                    self._check_last_query_bursted(cluster, main_cur1)
                elif not try_burst or (try_burst and not should_burst):
                    self._check_last_query_didnt_burst(cluster, main_cur1)

        main_cur1.execute("set query_group to {}".format(query_group))

    @contextmanager
    def setup_views_with_shared_tables(self,
                                       cluster,
                                       cursor,
                                       relprefix,
                                       diststyle,
                                       sortkey,
                                       mv_queries,
                                       clean_on_exit=True,
                                       query_group='burst'):
        """
            This creates a set of base tables and multiple materialized views
            on them. All MVs share the set of base tables. This is much faster
            to setup and test. This is useful when tests are large since RAFF
            has a timeout of 5400s and 7200s for cluster tests and simulated
            tests respectively. We can now alter one table and test refresh on
            MVs.

            Args:
                cluster: Cluster object
                cursor: Cursor object
                relprefix (str): Common prefix shared by table and MV
                diststyle (list): A list of distribution style of base-tables
                    and MVs
                sortkey (list): A list of sortkeys for base-tables and MVs
                mv_queries (list): A list of queries to create MVs.
                    See MV_QUERIES.
                clean_on_exit (bool): If true, table and MV are deleted when
                test fails. Else, they are not deleted.
        """
        if query_group == 'burst':
            cursor.execute('set query_group to burst;')
        elif query_group == 'noburst':
            cursor.execute('set query_group to noburst;')
        # Create test table for health check later
        cursor.execute(DROP_TABLE.format(MY_TABLE))
        cursor.execute(CREATE_TEST_TABLE)

        # Create some base tables
        tables, t_dists, t_sorts = [], [], []
        num_base_tables = max([mvq.num_tables for mvq in mv_queries])
        for tbl_id in range(num_base_tables):
            table = "_".join([relprefix, "table", str(tbl_id)])
            t_dist = diststyle[tbl_id % len(diststyle)]
            t_sort = sortkey[tbl_id % len(sortkey)]
            cursor.execute(DROP_TABLE.format(table))
            cursor.execute(CREATE_TABLE.format(table, t_dist, t_sort))
            cursor.execute(INSERT.format(table, self._values(10)))
            tables.append(table)
            t_dists.append(t_dist)
            t_sorts.append(t_sort)

        # Create MVs on the base tables
        # All MVs share the base tables
        mvs, _mv_queries, mv_dists, mv_sorts = [], [], [], []
        for mv_id, mv_q in enumerate(mv_queries):
            mv = "_".join([relprefix, "mv", str(mv_id)])
            mv_dist = diststyle[mv_id % len(diststyle)]
            mv_sort = sortkey[mv_id % len(sortkey)]
            mv_query = mv_q.query.format(*tables)
            cursor.execute(DROP_MV.format(mv))
            cursor.execute(CREATE_MV.format(mv, mv_dist, mv_sort, mv_query))
            mvs.append(mv)
            _mv_queries.append(mv_query)
            mv_dists.append(mv_dist)
            mv_sorts.append(mv_sort)

        mv_configs = mv_configs_t(tables, t_dists, t_sorts, mvs, _mv_queries,
                                  mv_dists, mv_sorts)
        try:
            self._check_burst_cluster_health(cluster, cursor, MY_TABLE)
        except Exception as e:
            assert False, ("Test setup failed. "
                           "Burst clusters acquired = {} "
                           "{}").format(cluster.list_acquired_burst_clusters(),
                                        e)
        yield mv_configs
        if cluster.host_type == HostType.CLUSTER:
            cluster.release_all_burst_clusters()
        if clean_on_exit:
            cursor.execute(DROP_SCHEMA.format(MY_SCHEMA))

    # Deprecated in favor of setup_views_with_shared_tables
    @contextmanager
    def setup_mv(self, cluster, cursor, relprefix, diststyle, sortkey,
                 mv_queries, clean_on_exit=True):
        """
            Sets up table and MV for the test.

            Args:
                relprefix (str): Common prefix shared by table and MV
                clean_on_exit (bool): If true, table and MV are deleted when
                test fails. Else, they are not deleted.
        """

        # Create test table for health check later
        sql = ";".join([DROP_TABLE.format(MY_TABLE), CREATE_TEST_TABLE])
        cursor.execute(sql)

        mv_configs = list(
            self.mv_data(relprefix, diststyle, sortkey, mv_queries))
        for mvd in mv_configs:
            tables, mv, mv_query, t_dist, t_sort, mv_dist, mv_sort = mvd
            sql = ""
            for table in tables:
                sql = ";".join([
                    sql,
                    DROP_TABLE.format(table),
                    CREATE_TABLE.format(table, t_dist, t_sort)
                ])
                cursor.execute(sql)
                # Issue the DML in a separate txn from the table creation DDL
                # to allow the DML to burst (if the other condidtions are met).
                # Note that a DML on a table created within the same txn cannot
                # burst.
                sql = INSERT.format(table, self._values(2))
                cursor.execute(sql)
            sql = CREATE_MV.format(mv, mv_dist, mv_sort, mv_query)
            cursor.execute(sql)
        try:
            self._check_burst_cluster_health(cluster, cursor, MY_TABLE)
        except Exception as e:
            assert False, ("Test setup failed. "
                           "Burst clusters acquired = {} "
                           "{}").format(cluster.list_acquired_burst_clusters(),
                                        e)
        yield mv_configs
        if cluster.host_type == HostType.CLUSTER:
            cluster.release_all_burst_clusters()
        # Save schema for debugging
        if clean_on_exit:
            cursor.execute(DROP_SCHEMA.format(MY_SCHEMA))

    def base_test_burst_mv_refresh(self, cluster):
        """
            This test provides coverage for materialized view on burst cluster.
            This test covers following test case:
              1) creates table and corresponding materialized view
              2) runs dml on table in burst cluster
              3) check equivalence between table and materialized view
                 before and after refreshing materialized view.
        """
        relprefix = "dp29456"
        session = DbSession(cluster.get_conn_params(),
                            session_ctx=SessionContext(username=MY_USER,
                                                       schema=MY_SCHEMA))
        cursor = session.cursor()
        syscur = self.db.cursor()
        with self.setup_mv(cluster, cursor, relprefix, diststyle, sortkey,
                           MV_QUERIES, clean_on_exit=False):
            if self.run_on_burst:
                cursor.execute("set query_group to burst")
            syscur.execute("set search_path to {}".format(MY_SCHEMA))
            for mvd in self.mv_data(relprefix, diststyle, sortkey, MV_QUERIES):

                log.info("==== TEST {}".format(mvd))

                log.info("Run DML on burst cluster & it should burst")
                self._do_dmls(cluster, cursor, mvd,
                              True, True, DML, ["COMMIT"])

                log.info("Run DML on main cluster")
                self._do_dmls(cluster, cursor, mvd,
                              False, False, ["INSERT"], ["COMMIT"])

                log.info(("Run DML with ABORT on burst cluster"
                          " & it should not burst"))
                # In legacy Burst, base tables are owned by Main and there is no
                # SB refresh prior to DML below, therefore DML should not burst.
                # In unified Burst, SB refresh is not needed for the DML to burst.
                dml_should_burst = self.use_unified_burst
                self._do_dmls(cluster, cursor, mvd, True, dml_should_burst,
                              ["UPDATE"], ["ABORT"])

                for tbl in mvd.tables:
                    self._check_table(cluster, MY_SCHEMA, tbl, mvd.t_dist)


@pytest.mark.cluster_only
@pytest.mark.custom_burst_gucs(
    gucs=dict(list(burst_write_mv_gucs.items()) + [(
        'burst_percent_threshold_to_trigger_backup', '100'), (
            'burst_cumulative_time_since_stale_backup_threshold_s',
            '86400'), ('enable_burst_async_acquire', 'false'), (
                'burst_commit_refresh_check_frequency_seconds',
                '-1'), ('enable_burst_lag_based_background_refresh', 'false')])
)
@pytest.mark.usefixtures("setup_teardown_burst")
class TestBurstWriteMVCluster(TestBurstWriteMVBase):
    def test_burst_mv_refresh_cluster(self, cluster):
        self.base_test_burst_mv_refresh(cluster)


@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=list(burst_write_mv_gucs.items()) +
                               [('slices_per_node', '3')])
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteMVSS(TestBurstWriteMVBase):
    def test_burst_mv_refresh_ss(self, cluster):
        self.base_test_burst_mv_refresh(cluster)
