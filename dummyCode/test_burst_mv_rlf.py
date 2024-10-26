# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_test import setup_teardown_burst
from raff.burst.burst_write import burst_write_basic_gucs
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.burst.burst_write import burst_write_mv_gucs
from raff.storage.storage_test import disable_all_autoworkers
from test_burst_mv_refresh import TestBurstWriteMVBase, MV_QUERIES
log = logging.getLogger(__name__)
__all__ = [disable_all_autoworkers, super_simulated_mode, setup_teardown_burst]

DELETE = "delete from {} where c0 > 5 and c0 < 9"
DELETE_ALL = "delete from {}"
INSERT = "insert into {} values {}"
REFRESH_MV = "refresh materialized view {}"
SELECT = "select * from {} order by 1,2"
UPDATE = "update {} set c0 = c0 + 2 where c0 > 4"
MY_SCHEMA = "test_schema"
MY_USER = "test_user"

diststyle = ['diststyle even', 'distkey(c0)']
sortkey = ['sortkey(c0)', '']
DML = ["COPY", "INSERT", "DELETE", "UPDATE"]


@pytest.mark.serial_only
@pytest.mark.skip_load_data
@pytest.mark.usefixtures("disable_all_autoworkers")
class TestBurstWriteMVRLFBase(TestBurstWriteMVBase):

    def base_test_burst_mv_rlf(self, cluster):
        """
            This test checks if row-level filter is turned off for materialized
            views on burst cluster. (X) means the statement is outside a
            transaction and auto-commits. It does the following:

                Session 1               Session 2

                INSERT (9,2)
                BEGIN
                INSERT (9,2)/DELETE/
                /UPDATE
                                        (REFRESH MV)
                                        (SELECT * FROM MV) - must return (9,2)
                COMMIT


            The select statement in session 2 must return the rows inserted in
            the table before the transaction in session 1.
        """
        relprefix = "dp29739"
        # can't use super as user_type as bootstrap quseries do not burst
        # can't burst drop-recreate MV as drop/create not supported for burst
        session1 = DbSession(cluster.get_conn_params(),
                             session_ctx=SessionContext(username=MY_USER,
                                                        schema=MY_SCHEMA))
        session2 = DbSession(cluster.get_conn_params(),
                             session_ctx=SessionContext(username=MY_USER,
                                                        schema=MY_SCHEMA))
        cur1 = session1.cursor()
        cur2 = session2.cursor()
        syscur = self.db.cursor()
        with self.setup_mv(cluster, cur1, relprefix, diststyle,
                           sortkey, MV_QUERIES, False) as _mv_data:
            if self.run_on_burst:
                cur1.execute("set query_group to burst")
                cur2.execute("set query_group to burst")
            syscur.execute("set search_path to {}".format(MY_SCHEMA))

            # for each MV
            for dml in DML:
                for mvd in _mv_data:
                    tables, mv, mv_query, _, _, _, _ = mvd

                    for tbl in tables:
                        cur1.execute(DELETE_ALL.format(tbl))
                        self._check_last_query_bursted(cluster, cur1)
                        cur1.execute(INSERT.format(tbl, self._values()))
                        self._check_last_query_bursted(cluster, cur1)

                    cur1.execute("BEGIN")

                    for tbl in tables:
                        if dml == "INSERT":
                            cur1.execute(INSERT.format(tbl, self._values()))
                        elif dml == "UPDATE":
                            cur1.execute(UPDATE.format(tbl))
                        elif dml == "DELETE":
                            cur1.execute(DELETE.format(tbl))
                        elif dml == "COPY":
                            self._do_copy(cur1, tbl)

                        if dml == "COPY":
                            self._check_last_copy_bursted(cluster, cur1)
                        else:
                            self._check_last_query_bursted(cluster, cur1)

                    cur2.execute(REFRESH_MV.format(mv))
                    self._check_last_mv_refresh_bursted(cluster, syscur, mv)
                    cur2.execute(mv_query)
                    table_rows = sorted(cur2.fetchall())
                    self._check_last_query_bursted(cluster, cur2)
                    cur2.execute(SELECT.format(mv))
                    mv_rows = sorted(cur2.fetchall())
                    self._check_last_query_bursted(cluster, cur2)
                    assert mv_rows == table_rows

                    # We only need test for commit and not abort.
                    # The point of this test is to check RLF in the middle of a
                    # transaction. It doesn't matter whether the txn commits or
                    # aborts later.
                    cur1.execute("COMMIT")
                    cur1.execute(REFRESH_MV.format(mv))
                    self._check_last_mv_refresh_bursted(cluster, syscur,
                                                        mv)
                    for table in tables:
                        self._check_ownership(MY_SCHEMA, table,
                                              [("Burst", "Owned")],
                                              cluster)
                    self._check_data_is_same(cluster, cur1, tables, mv,
                                             mv_query)


@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(
    gucs=dict(list(burst_write_basic_gucs.items()) + [('slices_per_node', '3')]))
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstWriteMVRLFSS(TestBurstWriteMVRLFBase):
    def test_burst_mv_rlf_ss(self, cluster):
        self.base_test_burst_mv_rlf(cluster)
