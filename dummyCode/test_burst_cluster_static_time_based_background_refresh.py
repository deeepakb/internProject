# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import uuid
import getpass
import time
import datetime
from raff.burst.burst_write import BurstWriteTest
from raff.common.db.session import DbSession
from raff.burst.burst_test import setup_teardown_burst
from raff.common.base_test import run_priviledged_query
from raff.common.db.session_context import SessionContext
from raff.common.host_type import HostType

log = logging.getLogger(__name__)
__all__ = [setup_teardown_burst]
SNAPSHOT_IDENTIFIER = "{}-{}".format(getpass.getuser(), str(uuid.uuid4().hex))

BURST_LAG_BASED_BG_REFRESH_SECONDS = 30

BURST_GUCS = dict(
    burst_mode='3',
    burst_max_idle_time_seconds='1800',
    enable_burst_s3_commit_based_refresh='true',
    enable_burst_async_acquire='false',
    enable_burst_lag_based_background_refresh='true',
    burst_lag_based_background_refresh_seconds=BURST_LAG_BASED_BG_REFRESH_SECONDS)


@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=BURST_GUCS)
class TestBurstBackgroundRefreshMultiCluster(BurstWriteTest):
    def _setup_table(self, cursor):
        cursor.execute("SET query_group TO BURST;")
        cursor.execute("CREATE TABLE t1 (col int) diststyle even;")
        cursor.execute("INSERT INTO t1 values (1), (2), (3);")

    def test_burst_time_satisfied_both_clusters(self, cluster):
        """
        This test checks that when the refresh condition is met
        i.e. when the burst cluster's sb-version has fallen behind
        the main cluster's sb-version and when the time lag is met
        i.e the previous refresh ended more than
        BURST_LAG_BASED_BG_REFRESH_SECONDS
        seconds ago, for each burst clusters attached, then a refresh is
        triggered on the burst clusters. The refresh will be triggered
        independently for each cluster depending on whenever each cluster
        satisifes the refresh condition.
        """
        starttime = datetime.datetime.now().replace(microsecond=0)
        start_str = starttime.isoformat(' ')
        db_session = DbSession(cluster.get_conn_params(user='master'))
        bs_session = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(
                user_type=SessionContext.SUPER, host_type=HostType.CLUSTER))
        with db_session.cursor() as cursor, \
             bs_session.cursor() as bootstrap_cursor:
            self._setup_table(cursor)
            # Personalize the first acquired burst cluster.
            arn1 = self.get_latest_acquired_cluster(cluster)
            cluster.personalize_burst_cluster(arn1)
            # Acquire the second burst cluster.
            cluster.run_xpx("burst_acquire")
            arn2 = self.get_latest_acquired_cluster(cluster)
            time.sleep(BURST_LAG_BASED_BG_REFRESH_SECONDS + 15)
            # Personalize burst cluster
            cluster.personalize_burst_cluster(arn2)
            cursor.execute("Create table t2 (col int) diststyle even;")
            cursor.execute("INSERT into t2 values (7), (8), (9);")
            # Sleep for the second burst cluster.
            time.sleep(BURST_LAG_BASED_BG_REFRESH_SECONDS + 15)
            query = """
                    select count(*) from stl_burst_manager_refresh
                    where action ilike '%RefreshStart%' and eventtime >= '{}'
                    and cluster_arn ilike '%{}%'""".format(start_str, arn1)
            refresh_count = int(
                run_priviledged_query(cluster, bootstrap_cursor, query)[0][0])
            # We have 2 asserts here because the first cluster waits a long time
            # while the second cluster is acquired and rebooted.
            # So, that time duration and at least one commit difference between
            # the burst and main clusters accounts for the first refresh.
            # The next commit and a sleep for atleast
            # BURST_LAG_BASED_BG_REFRESH_SECONDS seconds accounts for the second
            # refresh.
            assert refresh_count == 2
            query = """
                    select count(*) from stl_burst_manager_refresh
                    where action ilike '%RefreshStart%' and eventtime >= '{}'
                    and cluster_arn ilike '%{}%'""".format(start_str, arn2)
            start = int(
                run_priviledged_query(cluster, bootstrap_cursor, query)[0][0])
            # For the second burst cluster, at least one commit
            # and BURST_LAG_BASED_BG_REFRESH_SECONDS seconds satisfy the
            # conditions for a refresh.
            assert start == 1
            cluster.release_all_burst_clusters()
