# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.burst.burst_status import BurstStatus

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

CREATE_MV_AS_STMT = ("CREATE MATERIALIZED VIEW bw_ctas_mv "
                     "diststyle key distkey(i) AS "
                     "(select * from ctas_test_table where i > 7)")
CTAS_INTERLEAVED_SORTKEY_STMT = ("CREATE TABLE bw_ctas_isk "
                                 "interleaved sortkey(i) AS "
                                 "(select * from ctas_test_table where i > 7)")
CTAS_DISTALL_STMT = ("CREATE TABLE bw_ctas_distall "
                     "DISTSTYLE all AS "
                     "(select * from ctas_test_table where i > 7)")
CTAS_TEMP_STMT = ("CREATE TEMP TABLE bw_ctas_temp "
                  "diststyle key distkey(i) AS "
                  "(select * from ctas_test_table where i > 7)")


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(
    gucs={
        'burst_enable_write': 'true',
        'burst_enable_write_user_ctas': 'true',
        'burst_enable_write_user_temp_ctas': 'false',
        'ctas_auto_analyze': 'false'
    })
@pytest.mark.custom_local_gucs(
    gucs={
        'burst_enable_write': 'true',
        'burst_enable_write_user_ctas': 'true',
        'burst_enable_write_user_temp_ctas': 'false',
        'ctas_auto_analyze': 'false'
    })
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstUserPermCTAS(BurstWriteTest):
    def _setup_tables(self, cursor):
        tbl_def = (
            "CREATE TABLE ctas_test_table (i int) diststyle key distkey(i)")
        cursor.execute(tbl_def)
        cursor.execute("INSERT INTO ctas_test_table values (1), (7), (10);")

    def test_burst_ctas_unsupported_cases(self, cluster, db_session):
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor:
            cursor.execute("SET query_group TO BURST;")
            self._setup_tables(cursor)
            self._start_and_wait_for_refresh(cluster)

            # case 1: create MV as
            cursor.execute("set query_group to burst")
            log.info("Running create MV as")
            cursor.execute(CREATE_MV_AS_STMT)
            # Verify cs_status code
            self._check_last_query_didnt_burst_with_code(
                cluster, cursor, BurstStatus.burst_write_for_mv_ctas_disabled)

            # case 2: create interleaved sortkey table
            log.info("Running create interleaved sortkey table")
            cursor.execute(CTAS_INTERLEAVED_SORTKEY_STMT)
            # Verify cs_status code
            self._check_last_query_didnt_burst_with_code(
                cluster, cursor,
                BurstStatus.burst_write_for_interleaved_sortkey_ctas_disabled)

            # case 3: create distall table
            log.info("Running create distall table")
            cursor.execute(CTAS_DISTALL_STMT)
            # Verify cs_status code
            self._check_last_query_didnt_burst_with_code(
                cluster, cursor, BurstStatus.write_query_on_dist_all)

            # case 4: create temp table
            log.info("Running create temp table")
            cursor.execute(CTAS_TEMP_STMT)
            # Verify cs_status code
            self._check_last_query_didnt_burst_with_code(
                cluster, cursor,
                BurstStatus.burst_write_for_user_temp_ctas_disabled)
