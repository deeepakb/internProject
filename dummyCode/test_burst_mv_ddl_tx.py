# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.common.dimensions import Dimensions
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.burst.burst_write import burst_write_basic_gucs
from raff.burst.burst_write import burst_write_mv_gucs
from raff.burst.burst_test import setup_teardown_burst
from raff.storage.storage_test import disable_all_autoworkers
from test_burst_mv_refresh import TestBurstWriteMVBase
from test_burst_mv_refresh import MV_QUERIES
from test_burst_mv_refresh import BURST_OWN, CLEAN, MAIN_UNDO

log = logging.getLogger(__name__)
__all__ = ["super_simulated_mode", "setup_teardown_burst"]

DELETE = "delete from {} where c0 > 2 and c1 < 9"
DELETE_ALL = "delete from {}"
INSERT = "insert into {0} values {1}"
REFRESH_MV = "refresh materialized view {0}"
SELECT = "select * from {0} order by 1,2"
UPDATE = "update {} set c0 = c0 + 2 where c0 > 4"
MY_SCHEMA = "test_schema"
MY_USER = "test_user"

# If a distkey/sortkey is unspecified, then padb picks distall & auto sortkey
# by default. This info doesn't appear in svv_table_info until the first insert
# sql on the table.
diststyle = ['distkey(c0)']
# keep the sortkey c0 else some create-mv fail with this error
# err: sortkey c1 must be included in the select list
sortkey = ['sortkey(c0)']

# We split the cmds to avoid RAFF timeouts. RAFF has a timeout of 5400s and
# 7200s for cluster and simulated tests respectively for each dimension. With
# the below split, each sub-list with 2-3 commands is one dimension.
ddl_cmds = [
    [
        'add_column',
        'rename_column',
        'drop_column',
    ],
    [
        'alter_distall',
        'alter_disteven',
    ],
]

ddl_cmds_part2 = [
    [
        'alter_distauto',
        'alter_distkey',
        'alter_distsort_all',
    ],
    [
        'alter_sortkey',
        'alter_sortkey_auto',
        'alter_encode',
        # if u rename table, MV is un-refreshable even on main cluster
        # 'rename_table'
    ],
]

dimensions_txn = Dimensions(
    dict(case=list(range(len(ddl_cmds))), txn_end=["abort", "commit"]))

part1_len = len(ddl_cmds)

ddl_cmds += ddl_cmds_part2

dimensions_txn_part2 = Dimensions(
    dict(case=list(range(part1_len, len(ddl_cmds))), txn_end=["abort", "commit"]))

# These ddls cannot run in a txn
ddl_cmds_no_txn = [
    'alter_sortkey_no_sort',
    'alter_sortkey_none',
    'truncate',
    'alter_column_type',
]
ddl_cmds += [ddl_cmds_no_txn]
dimensions_no_txn = Dimensions(
    dict(case=[len(ddl_cmds) - 1], txn_end=['commit']))

# These ddls alter column
ddl_cmds_alter_col = [
    'add_column',
    'rename_column',
    'drop_column',
    'alter_column_type',
]

class TestBurstWriteMVDDLBase(TestBurstWriteMVBase):
    def _get_ddl_cmd(self, table, cmd_type):
        """
            Given a DDL command type and table name,
            this method returns the exact SQL for the DDL
            to be performed on the table.
        """
        if cmd_type == 'alter_distall':
            cmd = "alter table {} alter diststyle all;"
        elif cmd_type == 'alter_disteven':
            cmd = "alter table {} alter diststyle even;"
        elif cmd_type == 'alter_distauto':
            cmd = "alter table {} alter diststyle auto;"
        elif cmd_type == 'alter_distkey':
            cmd = "alter table {} alter distkey c3"
        elif cmd_type == 'alter_sortkey':
            cmd = "alter table {} alter sortkey(c4)"
        elif cmd_type == 'alter_sortkey_no_sort':
            cmd = "alter table {} alter sortkey(c5) no sort"
        elif cmd_type == 'alter_encode':
            cmd = "alter table {} alter column c2 encode zstd;"
        elif cmd_type == 'alter_distsort_all':
            cmd = ("alter table {} alter diststyle all, alter sortkey(c6)")
        elif cmd_type == 'alter_sortkey_none':
            cmd = "alter table {} alter sortkey none;"
        elif cmd_type == 'alter_sortkey_auto':
            cmd = "alter table {} alter sortkey auto;"
        elif cmd_type == 'truncate':
            cmd = "truncate {};"
        elif cmd_type == 'add_column':
            cmd = ("alter table {0} add column c11 varchar(10) "
                   "default ('test');")
        elif cmd_type == 'alter_column_type':
            # Cannot alter a column the MV depends on. So have to add a
            # new column that the MV does not depend on.
            cmd = ("alter table {0} add column c12 varchar(10) "
                   "default ('test');")
            cmd += "alter table {0} alter column c12 type varchar(512);"
        elif cmd_type == 'rename_column':
            cmd = ("alter table {0} add column c13 varchar(10) "
                   "default ('test');")
            cmd += "alter table {0} rename column c13 to c130;"
        elif cmd_type == 'drop_column':
            cmd = ("alter table {0} add column c14 varchar(10) "
                   "default ('test');")
            cmd += "alter table {0} drop column c14;"
        elif cmd_type == 'rename_table':
            cmd = "alter table {0} rename to {0}_rename;"
            cmd += "alter table {0}_rename rename to {0};"

        return cmd.format(table)

    def _do_ddl(self, ddl_cmd, tables, cluster, cursors):
        """
            Given a DDL cmd and a list of tables, this method executes the DDL
            on all tables in the list.
            Args:
                ddl_cmd (str): DDL cmd
                tables (list): List of tables
        """
        main_cur1, = cursors
        for table in tables:
            _ddl_cmds = self._get_ddl_cmd(table, ddl_cmd)
            # InternalError: ALTER TABLE ALTER COLUMN cannot run inside a
            # multiple commands statement. Hence, split and run them.
            for ddl in _ddl_cmds.split(";"):
                if len(ddl) > 0:
                    main_cur1.execute(ddl)

    def _test_burst_mv_ddl(self, cluster, cursors, mv_configs, vector):
        """
            This runs a series of DDLs on the base tables of the MV and checks
            if MV-REFRESH bursts or not.

            insert on base-tables on burst cluster
            mv-refresh, should burst

            begin
            DDL on base tables of MV, should not burst
            copy/insert/update/delete on base tables of MV, should not burst
            commit/abort
            mv-refresh, should not burst because of DDL

            backup and burst-refresh
            copy/insert/update/delete on base tables of MV
            mv-refresh, should burst
        """

        main_cur1, check_cur, bs_cur = cursors
        shared_base_tables = mv_configs.tables
        self._do_sql("insert", True, True, shared_base_tables, cluster,
                     [main_cur1])
        self._refresh_all_mv(
            True, True, mv_configs, cluster, cursors, BURST_OWN, is_stale=True)
        self._check_state(BURST_OWN, BURST_OWN, mv_configs, cluster)

        # True as MV was previously burst-owned
        is_mv_burst_own = True
        start_txn = False
        col_altered = False
        # Run DDL on each base-table of MV followed by MV-refresh
        for ddl_cmd in ddl_cmds[vector.case]:

            # Some DDLs don't support txn, so we need to conditionally begin a
            # transaction only for those DDLs that can run in a transaction
            start_txn = ddl_cmd not in ddl_cmds_no_txn
            if start_txn:
                main_cur1.execute("begin")

            # Do DDL on all base tables so they have same number of columns
            # else MVs with UNION stmt fail if the DDL adds/drops col(s)
            self._do_ddl(ddl_cmd, shared_base_tables, cluster, [main_cur1])
            self._do_sql("copy update insert delete", True, False,
                         shared_base_tables, cluster, [main_cur1])
            # Commit or Abort
            # If there was no BEGIN, this is a no-op.
            main_cur1.execute(vector.txn_end)
            if start_txn and vector.txn_end == 'abort':
                # After abort on main, base-tables are MAIN_UNDO
                self._check_state(MAIN_UNDO, BURST_OWN if is_mv_burst_own else
                                  CLEAN, mv_configs, cluster)

            # MV remains burst-owned after MV-refresh iff
            # 1. It was previously burst-owned
            # 2. We did not commit any txn on any base table
            # If DML on base table is aborted, then mv is not stale, thus the
            # refresh is no-op and MV's state remains BURST_OWN.
            is_mv_burst_own = is_mv_burst_own and (start_txn and
                                                   vector.txn_end == "abort")
            col_altered = col_altered or (ddl_cmd in ddl_cmds_alter_col)
            # _refresh_all_mv refreshes all MVs that depend on the altered
            # base-table(s) and asserts the following checks:
            # 1. MV-refresh does not burst even if it attempts to burst
            # 2. State of MV-internal table is either BURST_OWN or CLEAN
            # 3. MV is stale if base-table(s) were updated
            self._refresh_all_mv(
                # try_burst
                True,
                # should_burst
                False,
                mv_configs,
                cluster,
                cursors,
                BURST_OWN if is_mv_burst_own else CLEAN,
                # If DDL alters num of col or type of col on base-tables on
                # main cluster, then we cannot compare data between base-tables
                # and MVs across main and burst clusters
                check_data_on_burst=not col_altered,
                # MV is stale if one of the following holds:
                # 1. We committed a txn on any base-table of MV
                # 2. It was an auto-commit txn
                is_stale=((start_txn and vector.txn_end != 'abort')
                          or not start_txn))

        # Restore the distkey/sortkey of each base-table
        for table in shared_base_tables:
            try:
                main_cur1.execute(
                    "alter table {} alter distkey c0".format(table))
                main_cur1.execute(
                    "alter table {} alter sortkey(c0)".format(table))
            except Exception as e:
                msgs = [
                    "Can not alter table to same distribution style",
                    "cannot alter to the same distkey",
                    "cannot alter to the same sort keys",
                    "This table is already SORTKEY NONE"
                ]
                log.error(e)
                # so that python does not complain 'e' is undefined
                ex = e
                if any([m in str(ex) for m in msgs]):
                    pass
                else:
                    raise e
        # MV gets reconstructed using CTAS after DDL and CTAS does not burst
        # currently. So we mv-refresh on main and take a backup.
        # Just insert some data to trigger mv-refresh.
        self._do_sql("insert", False, False, shared_base_tables, cluster,
                     [main_cur1])
        self._refresh_all_mv(
            # try_burst
            False,
            # should_burst
            False,
            mv_configs,
            cluster,
            cursors,
            CLEAN,
            check_data_on_burst=not col_altered,
            is_stale=True)
        self._start_and_wait_for_refresh(cluster)
        self._do_sql("copy update insert delete", True, True,
                     shared_base_tables, cluster, [main_cur1])
        self._check_state(BURST_OWN, CLEAN, mv_configs, cluster)
        self._refresh_all_mv(
            # try_burst
            True,
            # should_burst
            True,
            mv_configs,
            cluster,
            cursors,
            BURST_OWN,
            check_data_on_burst=not col_altered,
            is_stale=True)

    def base_test_burst_mv_ddl(self, cluster, vector):
        """
            This test performs DDL on a base table of MV and then checks if MV
            refresh bursts or not.
            Refer _test_burst_mv_ddl for the actual test.
        """
        relprefix = "ddl"
        session1 = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))
        session3 = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))
        main_cur1 = session1.cursor()
        check_cur = session3.cursor()
        bs_cur = self.db.cursor()
        with self.setup_views_with_shared_tables(
                cluster, check_cur, relprefix, diststyle, sortkey, MV_QUERIES,
                False) as mv_configs:
            bs_cur.execute("set search_path to {}".format(MY_SCHEMA))
            cursors = [main_cur1, check_cur, bs_cur]
            self._test_burst_mv_ddl(cluster, cursors, mv_configs, vector)


@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(
    gucs=list(burst_write_basic_gucs.items()) + [('slices_per_node', '3')])
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.serial_only
@pytest.mark.skip_load_data
class TestBurstWriteMVDDLSS(TestBurstWriteMVDDLBase):
    @classmethod
    def modify_test_dimensions(cls):
        return dimensions_txn

    def test_burst_mv_ddl_with_txn_ss(self, cluster, vector):
        self.base_test_burst_mv_ddl(cluster, vector)
