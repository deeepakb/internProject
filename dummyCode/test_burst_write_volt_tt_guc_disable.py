# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest

from contextlib import contextmanager
from raff.common.db.redshift_db import RedshiftDb
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode,\
        load_tpcds_data_tables, get_burst_conn_params
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.util.utils import is_commit_on_sync_undo_guc_enabled

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

AGG_SQL = ("select count(*), max(c_customer_sk), max(c_current_cdemo_sk), "
           "max(c_current_addr_sk), max(c_first_shipto_date_sk),"
           "max(c_salutation), max(c_first_name), max(c_last_name) "
           "from customer;")


class BurstWriteVoltTTGucDisabledBase(BurstWriteTest):
    @contextmanager
    def burst_cursor(self, db_session, burst):
        with db_session.cursor() as cursor:
            if (burst):
                cursor.execute("set query_group to burst")
            else:
                cursor.execute("set query_group to noburst")
            yield cursor
            cursor.execute("reset query_group")

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

    def run_burst_write_volt_tt_qualification(self, cluster, db_session):
        """
        Test: Verify volt temp table can burst in different cases.
        """
        with self.db.cursor() as cursor:
            cursor.execute("show burst_enable_write")
            assert cursor.fetch_scalar() == 'off'

        self._setup_tables(cluster)
        with self.burst_cursor(db_session, True) as cursor:
            cursor.execute(TPCDS_Q1_SQL)
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(AGG_SQL)
            self._check_last_query_bursted(cluster, cursor)
            log.info("Volt temp table query can burst.")
            cursor.execute("insert into store values (0,'a');")
            cursor.execute(TPCDS_Q1_SQL)
            self._check_last_query_didnt_burst(cluster, cursor)
            cursor.execute(AGG_SQL)
            self._check_last_query_bursted(cluster, cursor)
            log.info("Volt temp table query can not burst after dml")
            self._start_and_wait_for_refresh(cluster)
            cursor.execute(TPCDS_Q1_SQL)
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(AGG_SQL)
            self._check_last_query_bursted(cluster, cursor)
            log.info("Volt temp table query can burst after dml and refresh")

            cursor.execute("begin;")
            cursor.execute("insert into store values (0,'a');")
            cursor.execute(TPCDS_Q1_SQL)
            self._check_last_query_didnt_burst(cluster, cursor)
            cursor.execute(AGG_SQL)
            self._check_last_query_bursted(cluster, cursor)
            log.info("Volt temp table query can not burst after dml in txn.")

            cursor.execute("abort;")
            if is_commit_on_sync_undo_guc_enabled(cluster):
                # If commit_on_sync_undo guc is set, we issue an
                # explicit commit on sync undo. So, after aborting
                # issue a burst refresh.
                self._start_and_wait_for_refresh(cluster)
            cursor.execute(TPCDS_Q1_SQL)
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(AGG_SQL)
            self._check_last_query_bursted(cluster, cursor)

            self._start_and_wait_for_refresh(cluster)
            cursor.execute(TPCDS_Q1_SQL)
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute(AGG_SQL)
            self._check_last_query_bursted(cluster, cursor)
            log.info("Volt temp table query can burst after dml and refresh")


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(gucs={'burst_enable_write': 'false'})
@pytest.mark.custom_local_gucs(gucs={'burst_enable_write': 'false'})
class TestBurstWriteVoltTTBothGucDisabled(BurstWriteVoltTTGucDisabledBase):
    def test_burst_write_volt_tt_both_guc_disable(self, cluster, db_session):
        """
        Test: Disable burst_enable_write guc on both main and burst cluster,
              and verify volt temp table passed.
        """
        burst_db = RedshiftDb(conn_params=get_burst_conn_params())
        with burst_db.cursor() as burst_cursor:
            burst_cursor.execute("show burst_enable_write")
            assert burst_cursor.fetch_scalar() == 'off'
        self.run_burst_write_volt_tt_qualification(cluster, db_session)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.custom_burst_gucs(gucs={'burst_enable_write': 'true'})
@pytest.mark.custom_local_gucs(gucs={'burst_enable_write': 'false'})
class TestBurstWriteVoltTTBurstGucEnable(BurstWriteVoltTTGucDisabledBase):
    def test_burst_write_volt_tt_burst_guc_enable(self, cluster, db_session):
        """
        Test: Disable burst_enable_write guc on main and enable it on burst
              cluster, and verify volt temp table passed.
        """
        burst_db = RedshiftDb(conn_params=get_burst_conn_params())
        with burst_db.cursor() as burst_cursor:
            burst_cursor.execute("show burst_enable_write")
            assert burst_cursor.fetch_scalar() == 'on'
        self.run_burst_write_volt_tt_qualification(cluster, db_session)
