# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid
import getpass
import time
import datetime
from raff.burst.burst_write import BurstWriteTest
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.base_test import run_priviledged_query
log = logging.getLogger(__name__)
__all__ = [super_simulated_mode]
SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))
BURST_LAG_BASED_BACKGROUND_REFRESH_SECONDS = 30


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.skip_load_data
@pytest.mark.super_simulated_mode
@pytest.mark.backup_and_cold_start(backup_id=SNAPSHOT_IDENTIFIER)
@pytest.mark.custom_burst_gucs(
    gucs={
        'enable_burst_s3_commit_based_refresh':
        'true',
        'enable_burst_lag_based_background_refresh':
        'true',
        'burst_lag_based_background_refresh_seconds':
        BURST_LAG_BASED_BACKGROUND_REFRESH_SECONDS,
        'burst_max_idle_time_seconds':
        600,
        'enable_burst_async_acquire':
        'false',
        'burst_use_ds_localization':
        'false',
        'enable_burst_s3_commit_based_cold_start':
        'false'
    },
    initdb_before=True,
    initdb_after=True)
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstStaticTimeBasedBackgroundRefresh(BurstWriteTest):
    def _setup_table(self, cursor):
        # We need to set query_group to burst so as to ensure that
        # a burst cluster is able to be acquired when the
        # insert query is run.
        cursor.execute("set query_group to burst")
        cursor.execute("begin;")
        cursor.execute("CREATE TABLE t1 (col int) diststyle even;")
        cursor.execute("INSERT INTO t1 values (1), (2), (3);")
        cursor.execute("commit;")

    def test_burst_static_time_Lag_satisfied(self, cluster, db_session):
        """
        This test checks that when the refresh condition is met
        i.e. when the burst cluster's time lag is met - the previous refresh
        ended more than BURST_LAG_BASED_BACKGROUND_REFRESH_SECONDS seconds ago,
        then a refresh is triggered on that burst cluster.
        """
        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor, \
             self.db.cursor() as bootstrap_cursor:
            self._setup_table(cursor)
            self._check_and_start_personalization(cluster)
            # setup_tables(cursor) already sets the BC commit
            # version behind main.
            # Now we sleep for the required time.
            # We add 15 seconds to eliminate any flakiness.
            time.sleep(BURST_LAG_BASED_BACKGROUND_REFRESH_SECONDS + 15)
            query = """
                    select count(*) from stl_burst_manager_refresh
                    where action ilike '%RefreshStart%' and eventtime >= '{}';
                    """.format(start_str)
            refresh_count_after_personalization = int(
                run_priviledged_query(cluster, bootstrap_cursor, query)[0][0])
            # The setup_table function performs a commit and then we sleep for
            # BURST_LAG_BASED_BACKGROUND_REFRESH_SECONDS seconds after
            # personalization which satisfies the conditions for a refresh.
            assert refresh_count_after_personalization == 1

            personalization_endtime_query = """
                    select endtime
                    from stl_burst_manager_personalization
                    where
                        endtime >= '{}';""".format(start_str)
            bootstrap_cursor.execute(personalization_endtime_query)
            personalization_endtime = bootstrap_cursor.fetchall()
            refresh_start_time_query = """
                    select eventtime
                    from stl_burst_manager_refresh
                    where action ilike '%RefreshStart%' and
                        eventtime >= '{}';""".format(start_str)
            bootstrap_cursor.execute(refresh_start_time_query)
            refresh_start_time = bootstrap_cursor.fetchall()
            # Confirms that the first refresh is triggered when the cluster
            # has been personalized.
            assert (
                (personalization_endtime[0][0] + datetime.timedelta(
                    seconds=BURST_LAG_BASED_BACKGROUND_REFRESH_SECONDS)) <=
                refresh_start_time[0][0] <=
                (personalization_endtime[0][0] + datetime.timedelta(
                    seconds=BURST_LAG_BASED_BACKGROUND_REFRESH_SECONDS + 2)))

            # This will satisfy the condition that the burst cluster's sb
            # version must be behind the main cluster's sb version.
            cursor.execute("Create table t2 (col int) diststyle even;")
            cursor.execute("INSERT into t2 values (7), (8), (9);")

            # This will satisfy the static time condition.
            time.sleep(BURST_LAG_BASED_BACKGROUND_REFRESH_SECONDS + 15)
            query = """
                    select count(*) from stl_burst_manager_refresh
                    where action ilike '%RefreshStart%' and eventtime >= '{}';
                    """.format(start_str)
            refresh_count = int(
                run_priviledged_query(cluster, bootstrap_cursor, query)[0][0])
            # At least one commit and
            # BURST_LAG_BASED_BACKGROUND_REFRESH_SECONDS seconds satisfy the
            # conditions for a refresh.
            assert refresh_count == 2
            query = """
                    select s.eventtime - e.eventtime
                    from stl_burst_manager_refresh as s,
                         stl_burst_manager_refresh as e
                    where
                         s.action ilike '%RefreshStart%' and
                         e.action ilike '%RefreshEnd%' and
                         s.eventtime > e.eventtime;"""
            bootstrap_cursor.execute(query)
            difference_between_refresh_times = bootstrap_cursor.fetchall()
            # Confirms that the difference between the 2 refreshes is
            # at least BURST_LAG_BASED_BACKGROUND_REFRESH_SECONDS seconds.
            assert (datetime.timedelta(
                seconds=BURST_LAG_BASED_BACKGROUND_REFRESH_SECONDS) <=
                    difference_between_refresh_times[0][0])
            cursor.execute("DROP TABLE IF EXISTS t1;")
            cursor.execute("DROP TABLE IF EXISTS t2;")

    def test_burst_static_time_without_commits(self, cluster, db_session):
        """
        This test checks that when the time lag is satisfied on
        burst cluster, i.e. the time since the previous refresh is
        greater than or equal to BURST_LAG_BASED_BACKGROUND_REFRESH_SECONDS seconds
        but the burst cluster's sb-version is still the same as the main
        cluster's sb-version then no refresh is triggered on that burst cluster.
        """
        start_time = datetime.datetime.now().replace(microsecond=0)
        start_str = start_time.isoformat(' ')
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor, \
             self.db.cursor() as bootstrap_cursor:
            self._setup_table(cursor)
            self._check_and_start_personalization(cluster)
            cursor.execute("set query_group to burst")
            self._start_and_wait_for_refresh(cluster)
            # We burst a query to avoid releasing the burst cluster
            # due to idle timeout which could fail the test at a later step.
            cursor.execute("INSERT INTO t1 values (4), (5), (6);")
            self._check_last_query_bursted(cluster, cursor)
            time.sleep(BURST_LAG_BASED_BACKGROUND_REFRESH_SECONDS + 15)
            burst_clusters = cluster.list_acquired_burst_clusters()
            assert len(burst_clusters) == 1, "No burst clusters acquired"
            refresh_end_time_query = """
                        select max(eventtime)
                        from stl_burst_manager_refresh
                        where action ilike '%RefreshEnd%' and
                            eventtime >= '{}';""".format(start_str)
            bootstrap_cursor.execute(refresh_end_time_query)
            previous_refresh_end_time = bootstrap_cursor.fetchall()
            log.info("printing : {}".format(previous_refresh_end_time[0][0]))
            refresh_check_time = previous_refresh_end_time[0][0]
            time.sleep(BURST_LAG_BASED_BACKGROUND_REFRESH_SECONDS + 15)
            query = """
                    select count(*) from stl_burst_manager_refresh
                    where action ilike '%RefreshStart%' and eventtime >= '{}'
                    """.format(refresh_check_time)
            refresh_count = int(
                run_priviledged_query(cluster, bootstrap_cursor, query)[0][0])
            # Time condition met but min commits not met to satisfy the
            # conditions for a refresh the second time.
            assert refresh_count == 0

    def test_burst_static_time_lag_not_satisfied(self, cluster, db_session):
        """
        This test checks that even when the burst cluster's sb-version
        has fallen behind the main cluster's sb-version but the time
        lag is not met i.e the time since the previous refresh is still
        less than BURST_LAG_BASED_BACKGROUND_REFRESH_SECONDS seconds, then no
        refresh is triggered on that burst cluster.
        """
        db_session = DbSession(cluster.get_conn_params(user='master'))
        with db_session.cursor() as cursor, \
             self.db.cursor() as bootstrap_cursor:
            self._setup_table(cursor)
            self._check_and_start_personalization(cluster)
            time.sleep(BURST_LAG_BASED_BACKGROUND_REFRESH_SECONDS + 15)
            burst_clusters = cluster.list_acquired_burst_clusters()
            assert len(burst_clusters) == 1, "No burst clusters acquired"
            cursor.execute("CREATE TABLE t2 (col int) diststyle even;")
            cursor.execute("INSERT into t2 values (7), (8), (9);")
            refresh_check_time = datetime.datetime.now()
            query = """
                    select count(*) from stl_burst_manager_refresh
                    where action ilike '%RefreshStart%' and eventtime >= '{}'
                    """.format(refresh_check_time)
            refresh_count = int(
                run_priviledged_query(cluster, bootstrap_cursor, query)[0][0])
            # Commits done but time condition not met to satisfy the
            # conditions for a refresh.
            assert refresh_count == 0
