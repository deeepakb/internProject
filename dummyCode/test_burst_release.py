# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

from __future__ import division
from collections import namedtuple
import pytest
import time
import logging
import datetime
import random
import uuid
from dateutil.parser import parse
from raff.burst.burst_test import (BurstTest, modify_param_group)
from raff.common.base_test import run_priviledged_query
from raff.common.base_test import run_priviledged_query_scalar_int
from raff.common.base_test import FailedTestException
from raff.common.dimensions import Dimensions


log = logging.getLogger(__name__)

BURST_MAX_IDLE_TIME_SECONDS = 480

COMMON_GUCS = {
    'burst_mode': '3',
    'max_concurrency_scaling_clusters': '2',
    'try_burst_first': 'true',
    'enable_result_cache': 'false',
    'enable_burst_async_acquire': 'false',
    'burst_max_idle_time_seconds': BURST_MAX_IDLE_TIME_SECONDS
}

def get_active_clusters_info(cluster, cursor):
    query = ("""
             SELECT cluster_arn,
             backup_version,
             last_used,
             last_heartbeat_check,
             active_pids
             FROM stv_burst_manager_cluster_info
             """)
    return run_priviledged_query(cluster, cursor, query)


def verify_service_client(cluster,
                          cursor,
                          clusters_to_acquire,
                          starttime,
                          action,
                          reason=None):
    start_str = starttime.replace(microsecond=0).isoformat(' ')
    # Verify we acquired the right number of clusters.
    query = ("""
             SELECT cluster_arn FROM stl_burst_service_client where
             action='{}' and len(btrim(error)) = 0 and eventtime >= '{}'
             """.format(action, start_str))
    if reason is not None:
        query = query + " and reason = '{}'".format(reason)
    raw_results = run_priviledged_query(cluster, cursor, query)

    if clusters_to_acquire != 0 and raw_results != ([]):
        burst_arn = raw_results[0][0]
        log.info("Burst cluster {}D with arn: {}".format(action, burst_arn))

    count = len(raw_results)
    if count != clusters_to_acquire:
        raise FailedTestException("{}D {} clusters instead of {}".format(
            action, count, clusters_to_acquire))


def verify_released(cluster,
                    cursor,
                    clusters_to_acquire,
                    starttime,
                    reason=None):
    verify_service_client(cluster, cursor, clusters_to_acquire, starttime,
                          "RELEASE", reason)


def verify_acquired(cluster, cursor, clusters_to_acquire, starttime):
    verify_service_client(cluster, cursor, clusters_to_acquire, starttime,
                          "ACQUIRE")


# We can't use the burst fixture because we need to play with the idle timeout
# which means we can't patch a burst cluster (it will be released too fast)
@pytest.mark.cluster_only
@pytest.mark.serial_only
@pytest.mark.load_tpcds_data
class TestBurstIdleRelease(BurstTest):
    @classmethod
    def modify_test_dimensions(cls):
        # Run test_burst_idle_release with commit based refresh guc off and on
        # separately for first two runs. Run test_burst_idle_release with
        # commit based refresh guc on and stale backup for the third run.
        TestProperty = namedtuple('TestProperty',
                                  ['cbr_guc', 'make_backup_stale'])
        return Dimensions(
            dict(test_property=[
                TestProperty('false', False),
                TestProperty('true', False),
                TestProperty('true', True)
            ]))

    def test_burst_idle_release(self, cluster, db_session, cluster_session,
                                vector):
        '''
        Test that verifies if clusters are correctly released due to idleness.
        '''
        start_time = datetime.datetime.now().replace(microsecond=0)
        last_used_ts = ""
        modify_param_group(cluster)
        gucs = COMMON_GUCS
        gucs[
            'enable_burst_s3_commit_based_refresh'] = vector.test_property.cbr_guc
        with cluster_session(gucs=gucs), db_session.cursor() as cur:

            # Trigger a backup since latest backup info is not persisted
            # across restarts
            backup_id = ("{}-{}".format(cluster.cluster_identifier,
                                        str(uuid.uuid4().hex)))
            cluster.backup_cluster(backup_id)

            if vector.test_property.make_backup_stale:
                # DML to make the backup stale.
                cur.execute("insert into call_center values(90, '90');")
                # Verify that the insert query didn't run on the burst cluster.
                queryid = self.last_query_id(cur)
                self.verify_query_didnt_bursted(cluster, queryid)

            # When make_backup_stale is false, iterate 5 times to catch
            # possible timing issues.
            # When it's true, we only want to execute it once. This is because
            # we want to verify that stale burst clusters can be released after
            # idle timeout. If we execute it more than once, it will be
            # refreshed which is not the intent for the stale backup case.
            iteration_times = 1 if vector.test_property.make_backup_stale else 5
            for i in range(iteration_times):
                querystart = datetime.datetime.now().replace(microsecond=0)
                self.execute_test_file('burst_query', session=db_session)
                queryend = datetime.datetime.now().replace(
                    microsecond=0) + datetime.timedelta(0, 1)
                queryid = self.last_query_id(cur)
                burst_status = self.get_burst_status_txt(cluster, queryid)
                verify_acquired(cluster, cur, 1, start_time)
                verify_released(cluster, cur, 0, start_time)
                if not vector.test_property.make_backup_stale:
                    raw_results = get_active_clusters_info(cluster, cur)
                    log.info("Active clusters info: {}".format(raw_results))
                    last_used_ts = parse(raw_results[0][2])
                message = "Query {} {} {}-{}. Last used {}".format(
                    queryid, burst_status, querystart, queryend, last_used_ts)
                log.info(message)
                if vector.test_property.make_backup_stale:
                    # Verify that the query ran on main cluster because burst
                    # is stale.
                    self.verify_query_didnt_bursted(cluster, queryid)
                else:
                    # Check that the query updated the last used timestamp
                    assert last_used_ts >= querystart, message
                    assert last_used_ts <= queryend, message
                # Sleep a random amount of seconds
                # to make sure the cluster is not released
                # when idle for short period of times
                time.sleep(random.randint(1, BURST_MAX_IDLE_TIME_SECONDS // 8))
                verify_acquired(cluster, cur, 1, start_time)
                verify_released(cluster, cur, 0, start_time)
                if vector.test_property.make_backup_stale:
                    # Verify that there is a StaleBackup error in stl_burst_prepare
                    # StaleBackup is defined in src/util/result.hpp as 11.
                    stale_backup_prepare_count_query = """
                        select count(*) from stl_burst_prepare
                        where code = 11 and starttime >= '{}';
                    """.format(start_time.isoformat(' '))
                    stale_backup_prepare_count = run_priviledged_query_scalar_int(
                        cluster, cur, stale_backup_prepare_count_query)
                    assert stale_backup_prepare_count >= 1
                else:
                    used_ts = parse(get_active_clusters_info(cluster, cur)[0][2])
                    assert last_used_ts == used_ts, message
                    cluster.run_xpx('burst_reenable')

            success = False

            # Sleep for the idle timeout duration to verify cluster is released
            for i in range(0, 2 * BURST_MAX_IDLE_TIME_SECONDS):
                time.sleep(1)
                # Get both the active cluster arn and last used timestamp in one query
                # to minimize timing issues
                active_cluster_info = get_active_clusters_info(cluster, cur)
                if active_cluster_info == ([]):
                    verify_released(cluster, cur, 1, start_time, "IdleTimeout")
                    success = True
                    break
                elif not vector.test_property.make_backup_stale:
                    used_ts = parse(active_cluster_info[0][2])
                    assert last_used_ts == used_ts, (
                        "Last used timestamp changed but it shouldn't")

            assert success, (
                "Cluster was not released in {} seconds. Active clusters: {}".
                format(2 * BURST_MAX_IDLE_TIME_SECONDS,
                       get_active_clusters_info(cluster, cur)))
