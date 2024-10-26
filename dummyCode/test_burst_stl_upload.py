# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import hashlib
import logging
import os
import time
import uuid
from contextlib import contextmanager

import pytest
import sqlparse
from raff.burst.burst_test import (BurstTest, setup_teardown_burst,
                                   verify_query_bursted)
from raff.common.db.db_exception import Error
from raff.monitoring.monitoring_test import MonitoringTestSuite
from io import open
from raff.common.db_connection import ConnectionInfo
from raff.data_loaders.functional import (load_data, create_external_schema)

log = logging.getLogger(__name__)
__all__ = ["setup_teardown_burst", "verify_query_bursted"]
S3_BUCKET = 'padb-monitoring-test'
XEN_ROOT = os.environ['XEN_ROOT']
PATH_BASE = os.path.join(XEN_ROOT, "test/raff/burst/testfiles")
BURST_QUERIES_FILE = os.path.join(
  PATH_BASE, "test_burst_stcs_data_fidelity_burst_queries.sql")
GUCS = {
    's3_stl_bucket': S3_BUCKET,
    'monitoring_connector_interval_seconds': '10',
    "enable_query_sys_table_log_from_S3": "true",
    "stl_enable_CN_fetching_LN_STL_from_s3": "true",
    'systable_subscribed_log_rotation_time_in_seconds': '10',
    "enable_stl_immutable_cluster_id_partition": "false",
    "enable_stl_upload_to_immutable_id_partition_during_transition": "true",
    "immutable_cluster_id": str(uuid.uuid4()),
    "cluster_id": str(uuid.uuid4().int & 0xFFFFF)
}

STL_TO_VERIFY = "stl_concurrency_scaling_query_mapping"


@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.custom_burst_gucs(gucs=GUCS, initdb_before=True, initdb_after=True)
class TestBurstStlUpload(BurstTest):
    @contextmanager
    def burst_db_session(self, db_session):
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to burst")
            yield cursor
            cursor.execute("reset query_group")

    def run_burstable_query(self, cluster, cursor):
        query_count = 0
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
        log_rotation_in_seconds = int(
            GUCS["systable_subscribed_log_rotation_time_in_seconds"])
        connector_interval_in_seconds = int(
            GUCS["monitoring_connector_interval_seconds"])
        max_wait_seconds = (
            log_rotation_in_seconds +
            connector_interval_in_seconds) * 5
        end_time = time.time() + max_wait_seconds
        # Wait burst to upload STL to S3
        while time.time() < end_time:
            continue
        assert query_count > 0, "no query bursted"

    def get_s3_key_prefix(self, cluster, use_immutable_cluster_id=True):
        account_id = cluster.get_padb_conf_value("cluster_owner_aws_acct_id")
        account_id_sha256 = hashlib.sha256(
            account_id.encode("utf-8")).hexdigest()[:8]
        cluster_id = cluster.get_padb_conf_value("cluster_id")
        immutable_cluster_id = \
            cluster.get_padb_conf_value("immutable_cluster_id")
        return '{}/{}/{}/{}'.format(
            account_id_sha256, account_id, STL_TO_VERIFY,
            immutable_cluster_id if use_immutable_cluster_id else cluster_id)

    def test_burst_upload_stl_to_desired_partition(
            self, cluster, db_session, s3_client, verify_query_bursted):
        '''
        Run burst queries and verify both immutable id partition and virtual id
        partition are used and contain same num of logs
        '''
        s3_stl_bucket = GUCS["s3_stl_bucket"]
        conn_params = cluster.get_conn_params()
        connection_info = ConnectionInfo(**conn_params)
        load_data(connection_info)
        create_external_schema(connection_info)
        SNAPSHOT_IDENTIFIER = ("{}-{}".format(cluster.cluster_identifier,
                               str(uuid.uuid4().hex)))
        cluster.backup_cluster(SNAPSHOT_IDENTIFIER)
        with self.burst_db_session(db_session) as cursor:
            self.run_burstable_query(cluster, cursor)
        key_prefix_vid = self.get_s3_key_prefix(
            cluster,
            use_immutable_cluster_id=False)
        key_prefix_immid = self.get_s3_key_prefix(
            cluster,
            use_immutable_cluster_id=True)

        attempts = 5
        log_rotation_in_seconds = int(
            GUCS["systable_subscribed_log_rotation_time_in_seconds"])
        connector_interval_in_seconds = int(
            GUCS["monitoring_connector_interval_seconds"])
        while attempts > 0:
            obj_keys_vid_partition = s3_client.list_objects_v2(
                Bucket=s3_stl_bucket,
                Prefix=key_prefix_vid)
            obj_keys_immid_partition = s3_client.list_objects_v2(
                Bucket=s3_stl_bucket,
                Prefix=key_prefix_immid)
            # Test passed
            if obj_keys_vid_partition["KeyCount"] > 0 and \
                int(obj_keys_vid_partition["KeyCount"]) == \
                    int(obj_keys_immid_partition["KeyCount"]):
                break
            # Fail the test if the s3 keys are still inconsistent in the final try
            if attempts == 1 and int(obj_keys_vid_partition["KeyCount"]) != \
                    int(obj_keys_immid_partition["KeyCount"]):
                log.info("cluster id: {}, s3 keys: {}".format(
                   GUCS["cluster_id"], obj_keys_vid_partition["Contents"]))
                log.info("immutable cluster id: {}, s3 keys: {}".format(
                   GUCS["immutable_cluster_id"], obj_keys_immid_partition["Contents"]))
                pytest.fail("S3 logs should be consistent in two partitions")
            time.sleep(log_rotation_in_seconds + connector_interval_in_seconds)
            attempts -= 1

    def test_burst_upload_stl_with_object_taggings(
            self, cluster, db_session, s3_client):
        '''
        Run burst queries and verify the following object taggings are added:
        - tagging #1: immutable cluster id
        - tagging #2: virtual cluster id
        - tagging #3: min log timestamp in the current compaction batch
        - tagging #4: max log timestamp in the current compaction batch
        '''
        cluster.set_event('EtMonitoringTracing,level=ElDebug5')
        cluster_id = cluster.get_padb_conf_value("cluster_id")
        immutable_cluster_id = \
            cluster.get_padb_conf_value("immutable_cluster_id")
        with self.burst_db_session(db_session) as cursor:
            self.run_burstable_query(cluster, cursor)
        s3_stl_bucket = GUCS["s3_stl_bucket"]
        obj_key_prefix = self.get_s3_key_prefix(cluster)
        MonitoringTestSuite.verify_log_externalization_result(
            s3_client, s3_stl_bucket, obj_key_prefix,
            cluster_id,
            immutable_cluster_id,
            is_subscribed_log=True)
