# Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
import logging
from raff.mv.autorefresh_utils import AutoRefreshTest
from time import sleep

import pytest
from raff.burst.burst_test import (BurstTest, setup_teardown_burst,
                                   verify_query_bursted)
from raff.mv.auto_tune_task_utils import OperatingMode

log = logging.getLogger(__name__)
# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst, verify_query_bursted]


@pytest.mark.load_tpcds_data
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(
    gucs={
        'manual_mv_auto_refresh_run_with_user_queries': '0',
        'manual_mv_auto_refresh_threshold_type': '1',
        'operating_mode': OperatingMode.PerformanceMode.value,
        "xen_guard_enabled": "true",
        "try_burst_first": "false",
        "multi_az_enabled": "false",
        "auto_tuner_enable_dynamic_config": "true"
    })
@pytest.mark.localhost_only
@pytest.mark.serial_only
class TestAutoTunerDynamicToggling(BurstTest, AutoRefreshTest):
    def user_query_expected_behavior_perf_mode(self, db_session, cursor, cur,
                                               cluster):
        """
        Check if user query's behavior is as expected in performance
        mode, i.e., user query gets bursted in lieu of auto tasks.

        Args:
            db_session: db_session object
            cursor: Database cursor for the current db session
            for user query execution
            cur: Database cursor for the current db for xpx
            command execution
            cluster: current db cluster
        """
        # Submit user query.
        self.execute_test_file('burst_query', session=db_session)
        qid = self.last_query_id(cursor)
        # Wait for the report to occur.
        sleep(2)
        self.verify_query_bursted(cluster, qid)
        cur.execute(("select COUNT(*) "
                     "from stl_internal_query_details where query = {}"
                     " AND is_burst_for_at_task = 1").format(qid))
        res = cur.fetch_scalar()
        # The query is bursted in lie of auto tasks and is recorded
        # in stl_concurrency_scaling_usage
        assert res > 0

    def user_query_expected_behavior_free_mode(self, db_session, cursor, cur,
                                               cluster):
        """
        Check if user query's behavior is as expected in free
        mode, i.e., user query does not get bursted in lieu of
        auto tasks.

        Args:
            db_session: db_session object
            cursor: Database cursor for the current db session
            for user query execution
            cur: Database cursor for the current db for xpx
            command execution
            cluster: current db cluster
        """
        # Submit user query.
        self.execute_test_file('burst_query', session=db_session)
        qid = self.last_query_id(cursor)
        # Wait for the report to occur.
        sleep(2)
        self.verify_query_didnt_bursted(cluster, qid)
        cur.execute(("select COUNT(*) "
                     "from stl_internal_query_details where query = {}"
                     " AND is_burst_for_at_task = 1").format(qid))
        res = cur.fetch_scalar()
        assert res == 0

    def test_query_bursting_change_in_perf_free_switch(self, db_session,
                                                       cluster):
        """
        This test verifies:
        1. When AutoTuner is switched from Performance mode to Free mode, user
        query will no longer be bursted in lieu of auto tasks;
        2. When AutoTuner is switched later from Free mode back to Performance
        mode, user query will be bursted in lieu of auto tasks;
        During testing, we use event EtAutoTunerQueryCannotRunOnMain to simulate
        a busy cluster that user query should be bursted if in performance mode
        for auto tasks.

        Args:
            db_session: db_session object
            cluster: current db cluster
        """

        # Set Event to simulate a busy cluster when user query cannot run on main
        # in Performance mode as it should leave the resource for auto task
        # execution on main.
        with cluster.event('EtAutoTunerQueryCannotRunOnMain'):
            with db_session.cursor() as cursor, self.db.cursor() as cur:
                self.user_query_expected_behavior_perf_mode(
                    db_session, cursor, cur, cluster)
                cur.execute(
                    "xpx 'update_auto_tuner_config_in_memory_working_mode Free';"
                )
                self.user_query_expected_behavior_free_mode(
                    db_session, cursor, cur, cluster)
                cur.execute(
                    "xpx 'update_auto_tuner_config_in_memory_working_mode Performance';"
                )
                self.user_query_expected_behavior_perf_mode(
                    db_session, cursor, cur, cluster)
