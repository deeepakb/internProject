# Copyright 2024 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import getpass
import logging
import uuid
import pytest

from raff.burst.burst_super_simulated_mode_helper \
    import super_simulated_mode, get_burst_conn_params
from raff.burst.burst_test import BurstTest
from raff.common.db_connection import ConnectionInfo
from raff.common.db.redshift_db import RedshiftDb
from raff.common.db.session import DbSession
from raff.data_loaders.functional import create_external_schema

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))

VOLT_CTAS_QUERY = """
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

SPECTRUM_QUERY = """
SELECT cbigint
FROM S3.partitioned_alltypes_parquet inner join S3.alltypes_parquet
USING (cbigint)
WHERE cbigint > 5;;
"""
SPECTRUM_QUERY_STATS = {'cache_key': '3624504879325469886',
                        'tuples': 210, 'nrows': -1, 'priority': 3}


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.load_tpcds('store_returns', 'date_dim', 'store', 'customer')
@pytest.mark.custom_local_gucs(
    gucs={
        'wlm_json_configuration': (
                '[{"auto_wlm": true, "concurrency_scaling":"auto"}]'),
        'autowlm_max_heavy_slots': 20,
        'try_burst_first': 'true',
        'stats_cache_persist_frequency_sec': 1,
        'enable_stats_cache': 'true',
        'enable_spectrum_stats_cache': 'true',
        'enable_burst_query_completion_stats': 2,
    })
@pytest.mark.custom_burst_gucs(
    gucs={
        'autowlm_max_heavy_slots': 20,
        'enable_burst_auto_wlm': 'true',
        'stats_cache_persist_frequency_sec': 1,
        'enable_stats_cache': 'true',
        'enable_spectrum_stats_cache': 'true',
        'enable_burst_query_completion_stats': 2,
    })
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestQueryCompletionStatTransfer(BurstTest):
    def verify_stats_cache_message(self, cursor, msg, ts):
        query = ("select count(*) from stl_event_trace "
                 "where event_name = 'EtLogSpectrumStatsCache' "
                 "and message ilike '%{}%' and eventtime >= '{}'"
                 .format(msg, ts))
        cursor.execute(query)
        assert cursor.fetch_scalar() == 1, "{} not found".format(msg)

    def verify_spectrum_stats(self, burst_cursor, main_qid, burst_qid, ts):
        cache_key = SPECTRUM_QUERY_STATS.get('cache_key')
        tuples = SPECTRUM_QUERY_STATS.get('tuples')
        nrows = SPECTRUM_QUERY_STATS.get('nrows')
        priority = SPECTRUM_QUERY_STATS.get('priority')
        self.verify_stats_cache_message(
            burst_cursor,
            f'PutIntoCacheAndLog: Query: {burst_qid}, key: '
            f'{cache_key}, % replaced with {tuples}, {nrows}, {priority}',
            ts)
        self.verify_stats_cache_message(
            self.db.cursor(),
            f'PutIntoCacheAndLog: Query: {main_qid}, key: '
            f'{cache_key}, % replaced with {tuples}, {nrows}, {priority}',
            ts)

    def get_transferred_query_completion_stats(self, qid):
        with self.db.cursor() as cursor:
            query = ("select reported_peak_usage_bytes, "
                     "exec_time_wo_compilation, spectrum_file_count, "
                     "num_pce_segs from stl_burst_query_execution "
                     f"where query = {qid} and action ilike '%FETCH_TOTAL%'")
            cursor.execute(query)
            return cursor.fetchall()

    def get_burst_query_id(self, burst_cursor, main_qid):
        query = ("select concurrency_scaling_query "
                 "from stl_concurrency_scaling_query_mapping "
                 f"where primary_query = {main_qid}")
        burst_cursor.execute(query)
        return burst_cursor.fetch_scalar()

    def verify_transferred_stats(self, burst_cursor, qid, ts, is_spectrum):
        log.info('Query id: %d' % qid)
        transferred_stats = self.get_transferred_query_completion_stats(qid)
        log.info(transferred_stats)
        burst_query_id = self.get_burst_query_id(burst_cursor, qid)
        log.info('Burst query id: %d' % burst_query_id)
        burst_query = ("select reported_peak_usage_bytes, "
                       "exec_time_wo_compilation, spectrum_file_count, "
                       "num_pce_segs from stl_internal_query_details "
                       f"where query = {burst_query_id}")
        burst_cursor.execute(burst_query)
        burst_stats = burst_cursor.fetchall()
        log.info(burst_stats)
        assert transferred_stats[0] == burst_stats[0]
        if is_spectrum:
            self.verify_spectrum_stats(burst_cursor, qid, burst_query_id, ts)

    def test_s3_stats_transfer_to_main(self, cluster):
        """
        Tests if s3 scan cached stats are transferred from burst to main.
        """
        conn_params = cluster.get_conn_params(user='master')
        db_session = DbSession(conn_params)
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        create_external_schema(ConnectionInfo(**conn_params))
        with cluster.event('EtLogStatsCache', 'level=ElDebug5'), \
                cluster.event('EtLogSpectrumStatsCache', 'level=ElDebug5'), \
                cluster.event('EtDisableS3TableStats'), \
                db_session.cursor() as cursor, \
                burst_session.cursor() as burst_cursor:
            # reset stats cache.
            burst_cursor.execute("xpx 'reset_stats_cache'")
            burst_cursor.execute(
                "xpx 'event set EtLogStatsCache,level=ElDebug5'")
            burst_cursor.execute(
                "xpx 'event set EtLogSpectrumStatsCache,level=ElDebug5'")
            # burst a query.
            for query_sql, is_spectrum in [(SPECTRUM_QUERY, True),
                                           (VOLT_CTAS_QUERY, False)]:
                cursor.execute("SELECT SYSDATE")
                ts = cursor.fetch_scalar()
                cursor.execute("set query_group to burst;")
                cursor.execute(query_sql)
                qid = cursor.last_query_id()
                self.verify_transferred_stats(burst_cursor, qid, ts,
                                              is_spectrum)
