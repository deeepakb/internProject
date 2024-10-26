import logging
import pytest
import uuid
import getpass
from time import sleep

from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.monitoring.monitoring_test import MonitoringTestSuite


log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]

burst_cw_metrics_file =\
    "/dev/shm/redshift/monitoring/burst/stats"

SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_burst_gucs(
        gucs={'burst_enable_write': 'true',
              'burst_enable_write_user_ctas': 'true'})
@pytest.mark.custom_local_gucs(
        gucs={'burst_enable_write': 'true',
              'burst_enable_write_user_ctas': 'true'})
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstPrepareMetrics(MonitoringTestSuite, BurstWriteTest):
    def _setup_base_table(self, cursor, base_table):
        cursor.execute("CREATE TABLE {} (i int) diststyle even;"
                       .format(base_table))
        cursor.execute("INSERT INTO {} values (1), (7), (10);"
                       .format(base_table))

    def _execute_ctas(self, cursor, base_table, ctas_table):
        cursor.execute("set query_group to burst;")
        cursor.execute("""CREATE TABLE {} AS (SELECT * from {})"""
                       .format(ctas_table, base_table))

    def test_burst_prepare_metrics(self, cluster):
        """
        CTAS doesn't burst when table to read is dirty and
        we can use this to verify prepare metrics.

        Due to CTAS attempting to burst but not being able
        to when the table to read is dirty, we should expect
        ConcurrencyScalingPrepareFailures to have a precise
        value of 1. And since we attempt to burst and spend
        time in Prepare(), we should expect
        ConcurrencyScalingPrepareTime to be greater than 0.

        If either of these expectations are not met, then we
        raise an assertion.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        base_table = "test_table"
        ctas_table = "ctas_table"
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst")
            self._setup_base_table(cursor, base_table)
            self._start_and_wait_for_refresh(cluster)
            # make table dirty
            cursor.execute("begin;")
            cursor.execute("insert into {} values (0),(1),(2),(3)"
                           .format(base_table))
            self._check_last_query_bursted(cluster, cursor)
            cursor.execute("abort;")
            # attempt to read from dirty table in ctas
            self._execute_ctas(cursor, base_table, ctas_table)
            self._check_last_ctas_didnt_burst(cluster)
            # time to allow metrics to populate
            timeout = 60
            iteration_delay = 5
            counter = 0
            isfile = self.check_if_file_exists(burst_cw_metrics_file, 60, True)
            log.info("File exists: {}".format(isfile))
            while counter < timeout:
                try:
                    prepare_time = self.read_value_for_metric(
                        burst_cw_metrics_file,
                        "value",
                        ["name", "ConcurrencyScalingPrepareTime"])
                    prepare_failures = self.read_value_for_metric(
                        burst_cw_metrics_file,
                        "value",
                        ["name", "ConcurrencyScalingPrepareFailures"])
                    if prepare_time is not None and prepare_failures is not None:
                        break
                    else:
                        sleep(iteration_delay)
                        counter += iteration_delay
                except Exception:
                    sleep(iteration_delay)
                    counter += iteration_delay
            log.info("prepare_time={} & prepare_failures={}".format(
                prepare_time, prepare_failures))
            if (prepare_time is None or prepare_failures is None):
                raise Exception("Failed to read metrics")
            assert prepare_time > 0 and prepare_failures == 1
