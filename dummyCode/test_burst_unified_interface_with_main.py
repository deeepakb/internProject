# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import json
import logging
import os
import pytest
import sqlparse
import time

from contextlib import contextmanager
from time import sleep

from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst
from raff.common.db.db_exception import Error
from raff.common.db.redshift_db import RedshiftDb
from raff.util.utils import run_bootstrap_sql, PropagatingThread
from io import open

log = logging.getLogger(__name__)
__all__ = [setup_teardown_burst]
S3_BUCKET = 'redshift-monitoring-qa'
XEN_ROOT = os.environ['XEN_ROOT']
PATH_BASE = os.path.join(XEN_ROOT, "test/raff/burst/testfiles")
BURST_QUERIES_FILE = os.path.join(
    PATH_BASE, "test_burst_stcs_data_fidelity_burst_queries.sql")
DB_USER = 'stcs_test_user'
DB_USER_PW = 'Testing1234'

WLM_CFG = [{
    "query_group": ["burst"],
    "user_group": ["burst"],
    "concurrency_scaling": "auto",
    "query_concurrency": 2
}, {
    "query_group": ["noburst"],
    "user_group": ["noburst"],
    "query_concurrency": 5
}]

# TODO: Change the is_arcadia_cluster and enable_arcadia GUCS
# back to true once we have a r5dn on Jenkins job
GUCS = {
    'wlm_json_configuration': json.dumps(WLM_CFG),
    'burst_mode': '3',
    's3_stl_bucket': S3_BUCKET,
    'try_burst_first': 'true',
    'enable_short_query_bias': 'false',
    'burst_max_idle_time_seconds': 3600,
    'systable_subscribed_log_rotation_time_in_seconds': '10',
    'monitoring_connector_interval_seconds': '10',
    'enable_monitoring_connector_main_cluster': 'true',
    'enable_monitoring_connector': 'true',
    'enable_burst_async_acquire': 'false',
    "monitoring_connector_interval_main_cluster_seconds": 10,
    "log_connector_max_batch_size_mb": 3,
    "enable_scan_burst_log_from_s3": "true",
    "enable_query_sys_table_log_from_S3": "true",
    "stl_enable_CN_fetching_LN_STL_from_s3": "true",
    "is_arcadia_cluster": "false",
    "enable_arcadia": "false"
}

PREPARE_USER_QUERIES = [
    'drop user if exists {};'.format(DB_USER),
    'create user {} with password \'{}\';'.format(DB_USER, DB_USER_PW),
    'grant all on all tables in schema public to {};'.format(DB_USER)
]


@pytest.mark.load_data
@pytest.mark.load_tpcds_data
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.serial_only
@pytest.mark.cluster_only
@pytest.mark.custom_burst_gucs(gucs=GUCS)
class TestBurstUnifedInterfaceWithMain(BurstTest):
    @contextmanager
    def acquire_one_burst(self, cluster):
        '''
        Make sure we are only aquire one burst cluster.
        '''
        self.ensure_only_one_burst(cluster)
        yield

    def ensure_only_one_burst(self, cluster):
        arns = cluster.list_acquired_burst_clusters()
        if arns and len(arns) == 1:
            return
        cluster.release_all_burst_clusters()
        cluster.acquire_burst_cluster()

    @contextmanager
    def main_queue_full(self, cluster, seconds=10):
        '''
        Freeze the queue to make sure burst queries can be actually bursted.
        '''
        with cluster.event("EtBurstFreezeQueueForTesting",
                           "seconds={},group=burst".format(seconds)):
            for _ in range(2):
                PropagatingThread(
                    target=lambda: self.run_blocking_query(cluster)).start()
        sleep(2)
        log.info("Events are set")
        yield

    def run_blocking_query(self, cluster, search_path="", group="burst"):
        db = RedshiftDb(cluster.get_conn_params())
        with db.cursor() as cursor:
            cursor.execute("set enable_result_cache_for_session to off;")
            if search_path and search_path != "":
                cursor.execute("set search_path to {};".format(search_path))
            if group and group != "":
                cursor.execute("set query_group to {};".format(group))
            log.info("Query cmd in")
            cursor.execute("select count(*) from web_sales;")
            log.info("Blocking query started")
            cursor.fetchall()
            log.info("Blocking query finished.")

    def run_burstable_query(self, cluster):
        userid_query = ("select usesysid from pg_user where usename = \'{"
                        "}\';").format(DB_USER)
        userid = int(run_bootstrap_sql(cluster, userid_query)[0][0])
        log.info("User {} id is {}".format(DB_USER, userid))
        count_query = ("select count(distinct(query)) from "
                       "stl_burst_query_execution where userid = {"
                       "};").format(userid)
        num_query_before = int(run_bootstrap_sql(cluster, count_query)[0][0])
        query_count = 0

        conn_params = cluster.get_conn_params(
            user=DB_USER, password=DB_USER_PW)
        with RedshiftDb(conn_params) as db_conn:
            with db_conn.cursor() as cursor:
                # step 2: run burst query
                cursor.execute("set query_group to burst;")
                # for query in BURST_QUERIES_FILE:
                with open(BURST_QUERIES_FILE, 'r') as sql_file_obj:
                    sproc_stmts = sqlparse.split(sql_file_obj.read())
                    for stmt in sproc_stmts:
                        if len(stmt) > 1:
                            format_stmt = sqlparse.format(stmt)
                            query_count = query_count + 1
                            try:
                                cursor.execute(format_stmt)
                            except Error as e:
                                log.error("Failed to execute burst queries.")
                                raise e
        num_query_after = int(run_bootstrap_sql(cluster, count_query)[0][0])
        log_rotation_in_seconds = int(
            GUCS["systable_subscribed_log_rotation_time_in_seconds"])
        connector_interval_in_seconds = int(
            GUCS["monitoring_connector_interval_seconds"])
        executed_query = num_query_after - num_query_before
        max_wait_seconds = (
            log_rotation_in_seconds + connector_interval_in_seconds) * 5
        end_time = time.time() + max_wait_seconds
        # wait for burst log to be uploaded to s3
        while time.time() < end_time:
            continue
        err_msg = ("Expect {} of queries bursted, but only see {} from"
                   " stl_burst_query_execution.").format(
                       query_count, executed_query)
        assert executed_query == query_count, err_msg
        return query_count, userid

    def test_burst_unified_interface_with_main(self, cluster):
        '''
        Run burst queries and verify we can fetch them from main, we'll just
        re-use the query prepared for test_burst_stcs_data_fidelity,
        and verify the logs are query-able from main.
        '''
        for query in PREPARE_USER_QUERIES:
            run_bootstrap_sql(cluster, query)
        with self.acquire_one_burst(cluster):
            with self.main_queue_full(cluster):
                bursted_query_count, userid = self.run_burstable_query(cluster)
                query = ("select count(*) "
                         "from stl_query "
                         "where concurrency_scaling_status=1 and userid={};"
                         ).format(userid)
                results = int(run_bootstrap_sql(cluster, query)[0][0])
                log.info("Burst query count: {}".format(results))
                err_msg = "Burst table should be query-able from main"
                assert bursted_query_count == results, err_msg
