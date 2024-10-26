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
from raff.common.db.redshift_db import RedshiftDb

log = logging.getLogger(__name__)
__all__ = [super_simulated_mode, setup_teardown_burst]

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))

BURST_QUERY_MAPPING_SQL = (
    "select concurrency_scaling_query from "
    "stl_concurrency_scaling_query_mapping where "
    "primary_query = {}"
)

BURST_QUERY_MEMORY_SQL = (
    "select wlm_query_memory from stl_internal_query_details "
    "where query = {}"
)

BURST_PERSONALIZATION_TOTAL_MEMORY_CHECK_SQL = (
    "select auto_wlm_total_memory_mb from stl_burst_manager_personalization "
    "limit 1"
)

BURST_PERSONALIZATION_MAX_MEMORY_CHECK_SQL = (
    "select auto_wlm_per_query_max_memory_mb "
    "from stl_burst_manager_personalization limit 1"
)


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.load_tpcds('call_center')
@pytest.mark.custom_local_gucs(gucs={
    'wlm_json_configuration':
        '[{"query_concurrency": 5, "concurrency_scaling":"auto"}]',
    'try_burst_first': 'true',
    'enable_short_query_bias': 'false',
    'enable_sqa_by_default': 'false',
    'enable_burst_auto_wlm': 'true',
})
@pytest.mark.custom_burst_gucs(gucs={
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
class TestBurstWlmFlexibleMemoryDisabled(BurstTest):
    def test_burst_wlm_flexible_memory_allocation_disabled(self, db_session):
        """
        Tests that when using manual wlm on main, flexible memory allocation
        on burst auto wlm is disabled.

        1. On burst set events so that flexible memory is enforced with
           specific memory assignment (10mb) if it is enabled.
        2. Submit a query to burst (note try_burst_first is enabled).
        3. Check the memory assignment on burst cluster is not 10mb.
        4. Check that burst cluster is set up without flexible memory
           during personalization.
        """
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        with burst_session.cursor() as burst_cursor:
            # Skip wlm readjust to make sure test is stable regardless of wlm
            # loop calling Readjust.
            burst_cursor.execute(
                "xpx 'event set EtForceSkipWLMConfigReadjust'")
            burst_cursor.execute(
                "xpx 'event set EtFlexibleMemoryAlwaysOn'")
            burst_cursor.execute(
                "xpx 'event set EtSetStaticPrediction, memory=10'")
            # First run of the burst query will make sure Readjust is called
            # by FinishWlmQuery..
            self.execute_test_file('burst_query', session=db_session)
            # Run the burst query again for the purpose of the test.
            self.execute_test_file('burst_query', session=db_session)
            with db_session.cursor() as cursor:
                cursor.execute('select pg_last_query_id()')
                qid = cursor.fetch_scalar()
            burst_cursor.execute(BURST_QUERY_MAPPING_SQL.format(qid))
            burst_qid = burst_cursor.fetch_scalar()
            burst_cursor.execute(BURST_QUERY_MEMORY_SQL.format(burst_qid))
            burst_query_memory = burst_cursor.fetch_scalar()
            assert burst_query_memory != 10 * 1024 * 1024
            with self.db.cursor() as cursor:
                cursor.execute(BURST_PERSONALIZATION_TOTAL_MEMORY_CHECK_SQL)
                total_memory_mb = cursor.fetch_scalar()
                assert total_memory_mb == 0
                cursor.execute(BURST_PERSONALIZATION_MAX_MEMORY_CHECK_SQL)
                max_memory_mb = cursor.fetch_scalar()
                assert max_memory_mb == 0


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.super_simulated_mode
@pytest.mark.super_simulated_precommit
@pytest.mark.load_tpcds('call_center')
@pytest.mark.custom_local_gucs(gucs={
    'wlm_json_configuration': (
            '[{"auto_wlm": true, "concurrency_scaling":"auto"}]'),
    'try_burst_first': 'true',
    'enable_short_query_bias': 'false',
    'enable_sqa_by_default': 'false',
    'enable_burst_auto_wlm': 'true',
})
@pytest.mark.custom_burst_gucs(gucs={
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
class TestBurstWlmFlexibleMemoryEnabled(BurstTest):
    def test_burst_wlm_flexible_memory_allocation_enabled(self, db_session):
        """
        Tests that when using auto wlm on main, flexible memory allocation on
        burst auto wlm is enabled.

        1. On burst set events so that flexible memory is enforced with
           specific memory assignment (10mb) if it is enabled.
        2. Submit a query to burst (note try_burst_first is enabled).
        3. Check the memory assignment on burst cluster is 10mb.
        4. Check that burst cluster is set up with flexible memory during
           personalization.
        """
        burst_session = RedshiftDb(conn_params=get_burst_conn_params())
        with burst_session.cursor() as burst_cursor:
            # Skip wlm readjust to make sure test is stable regardless of wlm
            # loop calling Readjust.
            burst_cursor.execute(
                "xpx 'event set EtForceSkipWLMConfigReadjust'")
            burst_cursor.execute(
                "xpx 'event set EtFlexibleMemoryAlwaysOn'")
            burst_cursor.execute(
                "xpx 'event set EtSetStaticPrediction, memory=10'")
            # First run of the burst query will make sure Readjust is called
            # by FinishWlmQuery..
            self.execute_test_file('burst_query', session=db_session)
            # Run the burst query again for the purpose of the test.
            self.execute_test_file('burst_query', session=db_session)
            with db_session.cursor() as cursor:
                cursor.execute('select pg_last_query_id()')
                qid = cursor.fetch_scalar()
            burst_cursor.execute(BURST_QUERY_MAPPING_SQL.format(qid))
            burst_qid = burst_cursor.fetch_scalar()
            burst_cursor.execute(BURST_QUERY_MEMORY_SQL.format(burst_qid))
            burst_query_memory = burst_cursor.fetch_scalar()
            assert burst_query_memory == 10 * 1024 * 1024
            with self.db.cursor() as cursor:
                cursor.execute(BURST_PERSONALIZATION_TOTAL_MEMORY_CHECK_SQL)
                total_memory_mb = cursor.fetch_scalar()
                assert total_memory_mb != 0
                cursor.execute(BURST_PERSONALIZATION_MAX_MEMORY_CHECK_SQL)
                max_memory_mb = cursor.fetch_scalar()
                assert max_memory_mb != 0
