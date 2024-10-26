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
from test_burst_mv_refresh import BURST_OWN, BURST_DIRTY, CLEAN, STRUCT_CHANGE, \
                                  MAIN_UNDO

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst, disable_all_autoworkers]

DELETE = "delete from {} where c0 > 2 and c1 < 9"
DELETE_ALL = "delete from {}"
INSERT = "insert into {0} values {1}"
REFRESH_MV = "refresh materialized view {0}"
SELECT = "select * from {0} order by 1,2"
UPDATE = "update {} set c0 = c0 + 2 where c0 > 4"
MY_SCHEMA = "test_schema"
MY_USER = "test_user"

diststyle = ['distkey(c0)', 'diststyle even']
# If there is not sortkey, then vacuum won't sort
sortkey = ['sortkey(c0)']

dimensions = Dimensions(
    dict(
        vacuum_cmd=['SORT ONLY', 'DELETE ONLY', 'FULL'],
        txn_end=['abort', 'commit']))

guc = dict(list(burst_write_basic_gucs.items()) + [('slices_per_node', '3')])
# Test checks queries bursted by the last qid. For CTAS this qid could
# be from auto analyze which will not burst. So we disable ctas_auto_analyze
# here.
guc.update([('ctas_auto_analyze', 'false')])


class TestBurstWriteMVVacuumBase(TestBurstWriteMVBase):
    def _do_vacuum(self, tables, table_state, cluster, cursors, vector):
        main_cur1, = cursors
        for tbl in tables:
            sql = "VACUUM {} {}.{} to 100 percent".format(
                vector.vacuum_cmd, MY_SCHEMA, tbl)
            main_cur1.execute(sql)
        self._verify_owner(cluster, MY_SCHEMA, tables, table_state)

    def _test_burst_mv_vacuum(self, cluster, cursors, mv_configs, vector):
        """
            This runs vacuum on the base tables of the MV and checks
            if MV-REFRESH bursts or not. It does the following:

            1. Burst DML on all base-tables
            2. Begin
            3. Burst DML on all base-tables
            4. Commit or Abort
            5. Vacuum all base-tables
            6. MV-refresh
            7. DMLs on base-tables on main-cluster
            8. Take backup
            9. MV-refresh, should burst

            Here are cases when MV-REFRESH can burst after VACUUM:
                1. MV-refresh can burst after VACUUM SORT ONLY on base-tables
                iff base-tables were BURST-OWN before VACUUM. This is because
                SELECT can burst on base-tables and MV-refresh does a SELECT on
                base-tables.
        """

        main_cur1, check_cur, bs_cur = cursors
        shared_base_tables = mv_configs.tables
        # Make all base-tables BURST_OWN
        self._do_sql("insert copy", True, True, shared_base_tables, cluster,
                     [main_cur1])
        # MV must be stale beyond this point since we just inserted data

        # Begin xact
        main_cur1.execute('begin')
        # DML on all base-tables, leaving the rest as they are
        self._do_sql("copy update select delete insert", True, True,
                     shared_base_tables, cluster, [main_cur1])
        # Commit or abort
        main_cur1.execute(vector.txn_end)

        # VACUUM cannot be executed inside a transaction block.
        if vector.txn_end == 'commit':
            # Vacuum all base-tables, leaving the rest as they are
            self._do_vacuum(shared_base_tables, BURST_OWN + STRUCT_CHANGE,
                            cluster, [main_cur1], vector)
            # select on all base tables should burst
            self._do_sql("select", True, True, shared_base_tables, cluster,
                         [main_cur1])
            # For regular vacuum and vacuum-delete-only, the MV-refresh uses
            # CTAS to recompute MVs and CTAS does not burst. Only in-case of
            # vacuum-sort-only, can the mv-refresh burst because it just uses
            # insert/delete cmds.
            check_data_on_burst = (vector.vacuum_cmd == ('SORT ONLY'))
            # CTAS recompute flow will look like this:
            # 1. CREATE TABLE mv_tbl__vacuum_mv_*_*_recomputed AS ...
            #    This part of recompute will be able to run on burst.
            # 2. CREATE OR REPLACE VIEW vacuum_mv_* AS
            #    SELECT ... FROM mv_tbl__vacuum_mv_*_*_recomputed
            #    Creating views through CTAS is not supported on burst
            #    so this part will run on main.
            # 3. DROP TABLE mv_tbl__vacuum_mv_*__*
            # 4. ALTER TABLE mv_tbl__vacuum_mv_*__*_recomputed
            #    RENAME TO mv_tbl__vacuum_mv_*__*
            #    Not able to run on burst.
            # 5. CREATE OR REPLACE VIEW vacuum_mv_0 AS SELECT ...
            #    FROM mv_tbl__vacuum_mv_*__*
            # Since this all happens in the same transaction, the
            # internal table mv_tbl__vacuum_mv_*__* will be marked
            # as dirty.
            ctas_recompute = (vector.vacuum_cmd != ('SORT ONLY'))
            self._refresh_all_mv(
                # try_burst
                True,
                # should_burst
                True,
                mv_configs,
                cluster,
                cursors,
                BURST_OWN if not ctas_recompute else BURST_DIRTY,
                check_data_on_burst=check_data_on_burst,
                is_stale=True)

        elif vector.txn_end == 'abort':
            # Vacuum all base-tables, leaving the rest as they are
            self._do_vacuum(shared_base_tables, STRUCT_CHANGE + MAIN_UNDO,
                            cluster, [main_cur1], vector)
            # select on base table should'nt burst as DML on base table aborted
            self._do_sql("select", True, False, shared_base_tables, cluster,
                         [main_cur1])
            # MV-refresh should not burst because DML on base-tables aborted
            self._refresh_all_mv(
                # try_burst
                True,
                # should_burst
                False,
                mv_configs,
                cluster,
                cursors,
                CLEAN,
                check_data_on_burst=False,
                is_stale=True)

        # Write query on the vacuumed base-tables cannot burst
        self._do_sql("copy update insert delete", True, False,
                     shared_base_tables, cluster, [main_cur1])
        # Select cannot burst because of DML above
        self._do_sql("select", True, False, shared_base_tables, cluster,
                     [main_cur1])
        self._start_and_wait_for_refresh(cluster)
        self._refresh_all_mv(
            # try_burst
            True,
            # should_burst
            True,
            mv_configs,
            cluster,
            cursors,
            BURST_OWN,
            check_data_on_burst=True,
            is_stale=True)

    def base_test_burst_mv_vacuum(self, cluster, vector):
        relprefix = "vacuum"
        session1 = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))
        session2 = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))
        main_cur1 = session1.cursor()
        check_cur = session2.cursor()
        bs_cur = self.db.cursor()
        with self.setup_views_with_shared_tables(
                cluster, check_cur, relprefix, diststyle, sortkey, MV_QUERIES,
                False) as mv_configs:
            bs_cur.execute("set search_path to {}".format(MY_SCHEMA))
            cursors = [main_cur1, check_cur, bs_cur]
            self._test_burst_mv_vacuum(cluster, cursors, mv_configs, vector)


@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(gucs=guc)
@pytest.mark.custom_local_gucs(gucs=guc)
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.serial_only
@pytest.mark.skip_load_data
class TestBurstWriteMVVacuumSS(TestBurstWriteMVVacuumBase):
    @classmethod
    def modify_test_dimensions(cls):
        return dimensions

    def test_burst_mv_vacuum_ss(self, cluster, vector):
        self.base_test_burst_mv_vacuum(cluster, vector)
