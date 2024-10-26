# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import getpass
import logging
import uuid
import pytest

from raff.burst.burst_super_simulated_mode_helper \
    import super_simulated_mode, get_burst_conn_params
from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst
from raff.common.base_test import run_priviledged_query
from raff.common.db.redshift_db import RedshiftDb

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))

BURST_QUERY_MAPPING_SQL = ("select concurrency_scaling_query from "
                           "stl_concurrency_scaling_query_mapping where "
                           "primary_query = {}")

BURST_QUERY_SQL = (
    "select reported_peak_usage_bytes, exec_time_wo_compilation, wlm_query_memory "
    "from stl_internal_query_details "
    "where query = {}"
)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.load_tpcds('web_sales', 'web_returns', 'catalog_sales',
                        'catalog_returns', 'store_sales', 'store_returns',
                        'date_dim')
@pytest.mark.custom_local_gucs(
    gucs={
        'wlm_json_configuration':
        ('[{"auto_wlm": true, "concurrency_scaling":"auto"}]'),
        'try_burst_first':
        'true',
        'enable_short_query_bias':
        'false',
        'enable_sqa_by_default':
        'false',
        'enable_burst_auto_wlm':
        'true',
    })
@pytest.mark.custom_burst_gucs(
    gucs={
        'autowlm_concurrency': 'true',
        'wlm_json_configuration': '[{"auto_wlm": "true"}]',
        'autowlm_auto_concurrency_heavy': 'true',
        'wlm_enable_memory_override': 'true',
        'autowlm_enable_flexible_memory_allocation': 'true',
        'autowlm_enable_on_demand_slot_creation': 'true',
        'enable_burst_auto_wlm': 'true',
    })
@pytest.mark.usefixtures("super_simulated_mode")
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
class TestBurstSendingFeaturesToMain(BurstTest):
    def test_burst_transfer_to_main(self, cluster, db_session):
        """
        Tests if peak_mem_needed on burst is the same as peak_mem_needed on main.
        """
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        with cluster.event('EtPredictorClient', 'level=ElDebug5'), \
                cluster.event('EtFetchSpectrumFileStats'), \
                cluster.event('EtFetchPrecompiledExecutorUsage'), \
                burst_session.cursor() as burst_cursor:
            burst_cursor.execute(
                "xpx 'event set EtSetStaticPrediction, memory=10'")
            burst_cursor.execute(
                "xpx 'event set EtFetchSpectrumFileStats, file_count=10'")

            burst_cursor.execute(
                "xpx 'event set EtFetchPrecompiledExecutorUsage, pce_usage=123'")

            # Run the burst test query
            self.execute_test_file('burst_tpcds_query49', session=db_session)
            with db_session.cursor() as cursor:
                cursor.execute('select pg_last_query_id()')
                qid = cursor.fetch_scalar()
                self.verify_query_bursted(cluster, qid)

            file_count_query = (
                "select spectrum_file_count from stl_internal_query_details "
                "where query = {}".format(qid))

            pce_usage_query = (
                "select num_pce_segs from stl_internal_query_details "
                "where query = {}".format(qid))

            with self.db.cursor() as cursor:
                rows = run_priviledged_query(cluster, cursor,
                                             BURST_QUERY_SQL.format(qid))
                main_reported_peak_usage_bytes = rows[0][0]
                main_wlm_query_memory = rows[0][2]

                rows = run_priviledged_query(cluster, cursor, file_count_query)
                file_count = int(rows[0][0])

                rows = run_priviledged_query(cluster, cursor, pce_usage_query)
                pce_usage = int(rows[0][0])

            burst_cursor.execute(BURST_QUERY_MAPPING_SQL.format(qid))
            burst_qid = burst_cursor.fetch_scalar()

            burst_cursor.execute(BURST_QUERY_SQL.format(burst_qid))
            burst_query = burst_cursor.fetchall()
            burst_reported_peak_usage_bytes = burst_query[0][0]
            burst_wlm_query_memory = burst_query[0][2]

            assert main_reported_peak_usage_bytes == burst_reported_peak_usage_bytes
            assert main_wlm_query_memory == burst_wlm_query_memory
            assert file_count == 10
            assert pce_usage == 123
