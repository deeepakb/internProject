# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest

from contextlib import contextmanager
from psycopg2 import InternalError
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode,\
        load_tpcds_data_tables
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.common.dimensions import Dimensions
from raff.storage.storage_test import create_thread

__all__ = [super_simulated_mode]
log = logging.getLogger(__name__)

TPCDS_Q1_SQL = """
      WITH /* TPC-DS query1.tpl 0.12 */ customer_total_return AS
          (SELECT sr_customer_sk AS ctr_customer_sk,
                  sr_store_sk AS ctr_store_sk,
                  sum(SR_STORE_CREDIT) AS ctr_total_return
           FROM store_returns,
                date_dim
           WHERE sr_returned_date_sk = d_date_sk
             AND d_year =2000
           GROUP BY sr_customer_sk,
                    sr_store_sk)
        SELECT /* TPC-DS query1.tpl 0.12 */ top 100 c_customer_id
        FROM customer_total_return ctr1,
             store,
             customer
        WHERE ctr1.ctr_total_return >
            (SELECT avg(ctr_total_return)*1.2
             FROM customer_total_return ctr2
             WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
          AND s_store_sk = ctr1.ctr_store_sk
          AND s_state = 'MI'
          AND ctr1.ctr_customer_sk = c_customer_sk
        ORDER BY c_customer_id;
"""
TPCDS_Q1_SQL_INSERT = """
      INSERT INTO {}
      WITH /* TPC-DS query1.tpl 0.12 */ customer_total_return AS
          (SELECT sr_customer_sk AS ctr_customer_sk,
                  sr_store_sk AS ctr_store_sk,
                  sum(SR_STORE_CREDIT) AS ctr_total_return
           FROM store_returns,
                date_dim
           WHERE sr_returned_date_sk = d_date_sk
             AND d_year =2000
           GROUP BY sr_customer_sk,
                    sr_store_sk)
        SELECT /* TPC-DS query1.tpl 0.12 */ top 100 c_customer_id
        FROM customer_total_return ctr1,
             store,
             customer
        WHERE ctr1.ctr_total_return >
            (SELECT avg(ctr_total_return)*1.2
             FROM customer_total_return ctr2
             WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
          AND s_store_sk = ctr1.ctr_store_sk
          AND s_state = 'MI'
          AND ctr1.ctr_customer_sk = c_customer_sk
        ORDER BY c_customer_id;
"""
SELECT = "Select * from {} order by 1;"


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(gucs={'burst_enable_write': 'true'})
class TestBurstWriteVoltTT(BurstWriteTest):
    @contextmanager
    def burst_cursor(self, db_session, burst):
        with db_session.cursor() as cursor:
            if (burst):
                cursor.execute("set query_group to burst")
            else:
                cursor.execute("set query_group to noburst")
            yield cursor
            cursor.execute("reset query_group")

    @classmethod
    def modify_test_dimensions(cls):
        return Dimensions({
            "guard": [
                'burst:in_sticky_session:stage_one',
                'burst:in_sticky_session:stage_two',
                'burst:in_sticky_session:stage_three'
            ],
            "is_commit": [True, False],
            "is_burst": [True, False],
        })

    def _do_background_query(self, cluster, cursor, can_burst):
        cursor.execute('set query_group to burst;')
        if can_burst:
            cursor.execute(TPCDS_Q1_SQL)
            self._check_last_query_bursted(cluster, cursor)
        else:
            cursor.execute(TPCDS_Q1_SQL)
            self._check_last_query_didnt_burst(cluster, cursor)

    def _do_background_query_dml(self, cluster, cursor, target, can_burst):
        cursor.execute('set query_group to burst;')
        if can_burst:
            cursor.execute(TPCDS_Q1_SQL_INSERT.format(target))
            self._check_last_query_bursted(cluster, cursor)
        else:
            try:
                cursor.execute(TPCDS_Q1_SQL_INSERT.format(target))
            except InternalError:
                pass

    def _dml_txn(self, cluster, is_burst):
        if is_burst:
            db_session = DbSession(cluster.get_conn_params(user='master'))
            with self.burst_cursor(db_session, True) as cursor:
                cursor.execute("insert into store values (0,'a');")
                self._check_last_query_bursted(cluster, cursor)
        else:
            with self.db.cursor() as bootstrap_cursor:
                bootstrap_cursor.execute('set query_group to burst;')
                bootstrap_cursor.execute("insert into store values (0,'a');")
                self._check_last_query_didnt_burst(cluster, bootstrap_cursor)

    def _abort_dml_txn(self, cluster, is_burst):
        if is_burst:
            db_session = DbSession(cluster.get_conn_params(user='master'))
            with self.burst_cursor(db_session, True) as cursor:
                cursor.execute("insert into store values (0,'a');")
                self._check_last_query_bursted(cluster, cursor)
        else:
            with self.db.cursor() as bootstrap_cursor:
                bootstrap_cursor.execute("begin;")
                bootstrap_cursor.execute("insert into store values (0,'a');")
                bootstrap_cursor.execute("abort;")

    def _setup_tables(self, cluster):
        load_tpcds_data_tables(cluster, 'store_returns')
        load_tpcds_data_tables(cluster, 'store')
        load_tpcds_data_tables(cluster, 'date_dim')
        load_tpcds_data_tables(cluster, 'customer')
        with self.db.cursor() as cursor:
            try:
                cursor.execute(
                        "alter table store_returns alter diststyle even;")
                cursor.execute(
                        "alter table store alter diststyle even;")
            except:
                pass
        self._start_and_wait_for_refresh(cluster)

    def test_burst_write_volt_tt_max_concurrency(self, cluster, db_session):
        """
        Test: Verify Insert with volt temp table can burst in transaction
              with write query that bursts table referred by it.
        """
        BURST_WRITE_MAX_CONCURRENCY = "EtTestBurstWriteMaxConcurrentQueries"
        cluster.unset_event(BURST_WRITE_MAX_CONCURRENCY)
        self._setup_tables(cluster)
        with self.burst_cursor(db_session, True) as cursor:
            # write to a table that will be read later on...
            cursor.execute("insert into store values (0,'a');")
            self._check_last_query_bursted(cluster, cursor)
            # read the modified table while in a sticky session
            cursor.execute(TPCDS_Q1_SQL)
            self._check_last_query_bursted(cluster, cursor)
            cluster.set_event(BURST_WRITE_MAX_CONCURRENCY)
            cursor.execute(TPCDS_Q1_SQL)
            self._check_last_query_bursted(cluster, cursor)
            cluster.unset_event(BURST_WRITE_MAX_CONCURRENCY)

    def test_burst_write_volt_tt_qualification(self, cluster, db_session):
        """
        Test: Verify Insert with volt temp table can burst in transaction
              with write query that bursts table referred by it.
        """
        self._setup_tables(cluster)
        with self.burst_cursor(db_session, True) as cursor:
            cursor.execute("BEGIN;")
            # write to a table that will be read later on...
            cursor.execute("insert into store values (0,'a');")
            self._check_last_query_bursted(cluster, cursor)
            # read the modified table while in a sticky session
            cursor.execute(TPCDS_Q1_SQL)
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("END;")
            # write to a table that will be read later on...
            cursor.execute("insert into store values (0,'a');")
            self._check_last_query_bursted(cluster, cursor)
            # read the modified table while in a sticky session
            cursor.execute(TPCDS_Q1_SQL)
            self._check_last_query_bursted(cluster, cursor)

    def test_burst_write_volt_tt_dml_qualification(self, cluster, db_session):
        """
        Test: Verify Insert with volt temp table can not burst.
        """
        with self.burst_cursor(db_session, True) as cursor:
            cursor.execute(
                    "create table result(c_id varchar(32)) diststyle even")
            self._setup_tables(cluster)
            cursor.execute("BEGIN;")
            # write to a table that will be read later on...
            cursor.execute("insert into store values (0,'a');")
            self._check_last_query_bursted(cluster, cursor)
            # read the modified table while in a sticky session
            cursor.execute(TPCDS_Q1_SQL_INSERT.format("result"))
            self._check_last_query_didnt_burst(cluster, cursor)
            cursor.execute("END;")
            # write to a table that will be read later on...
            cursor.execute("insert into store values (0,'a');")
            self._check_last_query_bursted(cluster, cursor)
            # read the modified table while in a sticky session
            cursor.execute(TPCDS_Q1_SQL_INSERT.format("result"))
            self._check_last_query_didnt_burst(cluster, cursor)

    @pytest.mark.parametrize("new_sb_version_tbl", ['t0', 't1'])
    def test_burst_write_volt_tt_dml_on_main(self, cluster, db_session,
                                             new_sb_version_tbl):
        """
        Test: Run a DML on main that is not a concurrent DML.
        This means the DML has committed when the volt query begins.
        This test confirms that in this case, the volt query will not
        burst because the sb version is stale after the DML.
        """
        with db_session.cursor() as cursor:
            cursor.execute('CREATE TABLE t0 (col1 int) diststyle even')
            cursor.execute('CREATE TABLE t1 (col1 int) diststyle even')
            cursor.execute("insert into t0 values (0), (1), (2)")
            cursor.execute("insert into t1 values (0), (1), (2)")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute("set query_group to noburst")
            cursor.execute("insert into {} values (4), (5), (6)".format(
                new_sb_version_tbl))
            self._check_last_query_didnt_burst(cluster, cursor)
            cursor.execute("set query_group to burst")
            cursor.execute("""With cte as (select * from t0) select * from cte,
                t1 where cte.col1 = t1.col1;""")
            self._check_last_query_didnt_burst(cluster, cursor)

    @pytest.mark.parametrize("is_commit", [True, False])
    def test_burst_write_volt_tt_with_concurrent_dml(
            self, cluster, db_session, is_commit):
        """
        Test: Run write query in background when volt query is running on burst
              cluster. Check whether volt query can be bursted or not based
              on the write query ran on main/burst and is committed or aborted.
              In this test, burst cluster doesn't own the target table.
        """
        self._setup_tables(cluster)
        xen_guard = self._create_xen_guard("burst:in_sticky_session:stage_one")
        with self.burst_cursor(db_session, True) as cursor:
            params = (cluster, cursor, True)
            with create_thread(self._do_background_query, params) as thread,\
                    xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks()
                if is_commit:
                    self._dml_txn(cluster, False)
                else:
                    self._abort_dml_txn(cluster, False)
                xen_guard.disable()
            cursor.execute("insert into store values (0,'a');")

    def test_burst_write_volt_tt_owned_tables_with_concurrent_dml(
            self, cluster, db_session, vector):
        """
        Test: Run write query in background when volt query is running on burst
              cluster. Check whether volt query can be bursted or not based
              on the write query ran on main/burst and is committed or aborted.
              In this test, burst cluster owns the target table of write query.
        """
        self._setup_tables(cluster)
        can_burst = (vector.guard == "burst:in_sticky_session:stage_three" or \
                     vector.is_burst)
        xen_guard = self._create_xen_guard(vector.guard)
        with self.burst_cursor(db_session, True) as cursor:
            cursor.execute("insert into store values (0,'a');")
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("insert into store values (0,'a');")
            self._check_last_query_bursted(cluster, cursor)
            params = (cluster, cursor, True)
            with create_thread(self._do_background_query, params) as thread,\
                    xen_guard:
                thread.start()
                xen_guard.wait_until_process_blocks()
                if vector.is_commit:
                    self._dml_txn(cluster, vector.is_burst)
                else:
                    self._abort_dml_txn(cluster, vector.is_burst)
                xen_guard.disable()
            cursor.execute("insert into store values (0,'a');")
