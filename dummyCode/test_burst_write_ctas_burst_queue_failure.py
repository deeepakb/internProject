# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import time
import threading

from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.burst.burst_status import BurstStatus
from raff.burst.burst_super_simulated_mode_helper import get_burst_conn_params
from raff.common.db.redshift_db import RedshiftDb

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

CTAS_STMT = ("CREATE TABLE ctas1 AS"
             " (select * from ctas_test_table where i > 7)")


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.custom_burst_gucs(
    gucs={
        'burst_enable_write': 'true',
        'burst_enable_write_user_ctas': 'true',
        'burst_enable_ctas_failure_handling': 'true',
        'enable_burst_failure_handling': 'true',
    })
@pytest.mark.custom_local_gucs(
    gucs={
        'burst_enable_write': 'true',
        'burst_enable_write_user_ctas': 'true',
        'burst_enable_ctas_failure_handling': 'true',
        'enable_burst_failure_handling': 'true',
        'always_burst_eligible_query': 'false',
    })
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstCTASQueueFailure(BurstWriteTest):
    def _setup_tables(self, cursor):
        tbl_def = (
            "CREATE TABLE ctas_test_table (i int) diststyle key distkey(i)")
        cursor.execute(tbl_def)
        cursor.execute("INSERT INTO ctas_test_table values (1), (7), (10);")

    def _verify_ctas_table(self, cursor, res):
        cursor.execute("set query_group to noburst")
        cursor.execute("SELECT * FROM ctas1")
        expected_res = cursor.fetchall()
        assert expected_res == res

    def _get_ctas_qids(self, cursor):
        query = ("select query from stl_query where querytxt like '%{}%' "
                 "and concurrency_scaling_status != 6 "
                 "order by query desc;")
        cursor.execute(query.format(CTAS_STMT))
        qids = cursor.fetchall()
        return qids

    def _run_query(self, cursor, query):
        cursor.execute(query)

    def _run_query_on_background(self, cursor, query):
        thread = threading.Thread(target=self._run_query, args=(cursor, query))
        thread.start()
        return thread

    def _run_ctas_and_verify(self, cursor, cluster, burst_db):
        cursor.execute("set query_group to burst")
        log.info("Running CTAS")
        ctas_bg_thread = self._run_query_on_background(cursor, CTAS_STMT)
        log.info("Wait until CTAS hit burst error kBurstAutoWlmQueueing = 65")
        conn_params = cluster.get_conn_params()
        session_ctx = SessionContext(user_type='bootstrap')
        with DbSession(conn_params, session_ctx=session_ctx) as db_session, \
                db_session.cursor() as bs_cursor:
            timeout = 10
            while True:
                bs_cursor.execute(
                    "select count(*) from stl_query where querytxt like '%{}%'"
                    " and concurrency_scaling_status=65;".format(CTAS_STMT))
                error_count = bs_cursor.fetch_scalar()
                if error_count > 0:
                    break
                else:
                    time.sleep(6)
                timeout = timeout - 1
                assert timeout >= 0, "CTAS did not hit kBurstAutoWlmQueueing"
        # Unset EtWlmBurstRouteFailure and CTAS should be able to burst
        with burst_db.cursor() as burst_cursor:
            burst_cursor.execute("xpx 'event unset EtWlmBurstRouteFailure'")
        ctas_bg_thread.join()
        # Verify CTAS query was running on burst
        qids = self._get_ctas_qids(cursor)
        log.info("Done CTAS qid: {}".format(qids))
        # There would be multiple CTAS:
        # The last rerun CTAS should succeed on burst cluster
        # The rest of CTAS failed with error burst wlm queue disabled
        assert len(qids) >= 2, "There would be multiple rerun CTAS"
        self.verify_query_bursted(cluster, qids[0][0])
        for i in range(1, len(qids)):
            self.verify_query_didnt_bursted_with_code(
                cluster, qids[i][0], BurstStatus.burst_auto_wlm_queueing)

    def _drop_ctas_table(self, cursor):
        cursor.execute("DROP TABLE ctas1;")

    def test_burst_ctas_busrt_queue_failure(self, cluster):
        with self.db.cursor() as cursor:
            cursor.execute("SET query_group TO BURST;")
            self._setup_tables(cursor)
            self._start_and_wait_for_refresh(cluster)
            burst_db = RedshiftDb(conn_params=get_burst_conn_params())
            with burst_db.cursor() as burst_cursor:
                burst_cursor.execute("xpx 'event set EtWlmBurstRouteFailure'")
            cursor.execute("set always_burst_eligible_query to true;")
            self._run_ctas_and_verify(cursor, cluster, burst_db)
            self._drop_ctas_table(cursor)
