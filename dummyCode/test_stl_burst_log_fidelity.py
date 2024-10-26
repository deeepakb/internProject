# Copyright 2021 Amazon.com, Inc. or its affiliates All Rights Reserved
import hashlib
import logging
import random
import time
from datetime import date, timedelta
import uuid

import pytest
from raff.common.base_test import run_priviledged_query
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.monitoring.monitoring_test import MonitoringTestSuite
from raff.system_tests.suites.system_table_outside_cluster_helper import \
    SystemTableOutsideClusterHelper

log = logging.getLogger(__name__)

CONNECTOR_DEFAULT_GUCS = {
    "enable_monitoring_connector_main_cluster": "true",
    "monitoring_connector_interval_main_cluster_seconds": 10,
    "enable_stl_immutable_cluster_id_partition": "true",
    "log_connector_max_batch_size_mb": 3,
    "monitoring_connector_interval_seconds": 60,
    "enable_query_sys_table_log_from_S3": "true",
    "s3_stl_bucket": "padb-monitoring-test",
    "stl_enable_CN_fetching_LN_STL_from_s3": "true",
    "enable_scan_burst_log_from_s3": "true",
    "enable_arcadia": "true",
    "is_arcadia_cluster": "true",
    "cluster_id": str(uuid.uuid4().int & 0xFFFFF),
    "immutable_cluster_id": str(uuid.uuid4()),
}
TEST_STL_NAME = "stl_concurrency_scaling_query_mapping"
TEST_STCS_NAME = "stcs_concurrency_scaling_query_mapping"
TEST_USER_ID = "123"
TEST_QID = 100000
TEST_CS_QUERY = "100002"


@pytest.mark.serial_only
@pytest.mark.localhost_only
class TestStlBurstLogFidelity(MonitoringTestSuite):
    session_ctx = SessionContext(user_type=SessionContext.BOOTSTRAP)

    def _generate_7days_burst_log(self, cluster_obj):
        '''
        Generate temp burst logs and upload to s3:
        stl_concurrency_scaling_query_mapping
        123|12345|100002|{intance_id}|
        '''
        s3_stl_bucket = cluster_obj.get_padb_conf_value("s3_stl_bucket")
        s3_client = self.get_s3_client(cluster_obj)
        account_id = cluster_obj.get_padb_conf_value(
            "cluster_owner_aws_acct_id")
        # Use a fake cluster_id to differentiate from
        # the main/MAZ primary cluster_id
        cluster_id = str(random.randint(10000, 20000))
        immutable_cluster_id = cluster_obj.get_padb_conf_value(
            "immutable_cluster_id")
        cluster_id_in_partition =\
            MonitoringTestSuite.get_stl_cluster_identifier_partition(
                cluster_obj, immutable_cluster_id, cluster_id)
        account_id_sha256 = hashlib.sha256(
            account_id.encode("utf-8")).hexdigest()[:8]
        date_list = []
        for day in range(7):
            date_list.append(
                (date.today() - timedelta(day)).strftime("%Y%m%d"))
        query_count = 0
        for stl_date in date_list:
            now_padb_time = (int(time.time()) - 946684800) * (10**6)
            now_suffix = ".{0}.gz".format(now_padb_time)
            time_prefix = '{}/{}/{}/{}/{}'.format(
                account_id_sha256, account_id, TEST_STL_NAME,
                cluster_id_in_partition,
                stl_date)
            # clean original s3 folder to avoid flakiness
            MonitoringTestSuite.cleanup_s3_directory(s3_client,
                                                     s3_stl_bucket,
                                                     time_prefix)
            s3_key = '{}/{}/{}/{}/{}/cs/{}/raw/{}'.format(
                account_id_sha256, account_id, TEST_STL_NAME,
                cluster_id_in_partition,
                stl_date, cluster_id,
                'LN_stl_concurrency_scaling_query_mapping' + now_suffix)
            log.info("S3 location of external STL is: {}".format(s3_key))
            SystemTableOutsideClusterHelper.\
                create_file_with_content_push_to_s3(
                    s3_client, "{}|{}|{}|{}\n".format(
                        TEST_USER_ID, TEST_QID + query_count, TEST_CS_QUERY,
                        cluster_id), TEST_STL_NAME, s3_key, s3_stl_bucket,
                    cluster_id)
            query_count += 1
        assert query_count == 7, "Should generate 7 days log"

    def test_stl_burst_log_fidelity(self, cluster, cluster_session):
        '''
        In serverless, customer will query both burst logs and main logs via
        unified interface. We verify that logic in
        test_burst_unified_interface_with_main.
        We'll also need to make sure that all 7 days burst logs are processed,
        and we can successfully retrieve all burst logs from main.
        This test verifies two things:
            1. 7 days burst logs can be successfully retrieved.
            2. STL tables in burst logs should be consistent with the result
               we've retrieved in main cluster.
        To mitigate above two verifications, this test runs following steps:
            1. Generate 7 days logs locally and upload to S3 with burst
               partition. The upload fidelity is ensured by
               test_s3_connector_externalization and
               test_burst_stcs_data_fidelity.
            2. Retrieve above burst logs from main and verify the results are
               consistent.
        '''
        with cluster_session(
                gucs=CONNECTOR_DEFAULT_GUCS,
                clean_db_before=True,
                clean_db_after=True):
            conn_params = cluster.get_conn_params()
            with DbSession(
                    conn_params, session_ctx=self.session_ctx) as db_session:
                self._generate_7days_burst_log(cluster)
                res = self.run_query(
                    db_session,
                    cluster,
                    "select * from {} where userid={};".format(
                                        TEST_STL_NAME, TEST_USER_ID))
                # There should be 7 rows in the result.
                query_id_pool = [TEST_QID + count for count in range(7)]
                found_queries = 0
                for row in res:
                    if (str(row[0]) == TEST_USER_ID and
                            row[1] in query_id_pool and
                            str(row[2]) == TEST_CS_QUERY):
                        found_queries += 1
                        query_id_pool.remove(row[1])
                    if len(query_id_pool) == 0:
                        break
                err_msg = "Expected 7 results but only get {} results" \
                          "Query id left in the pool: {}"
                assert len(query_id_pool) == 0, err_msg.format(found_queries,
                                                               query_id_pool)

    def test_get_burst_stl_from_s3_with_multiple_versions(
            self, cluster, cluster_session, db_session):
        """
        This test is to verify if we are pulling the most up-to-date version
        of an s3 object if there are multiple versions. We may meet this case
        when we upload a burst log compaction for multiple times with the same
        s3 key (the name of first STL file in compaction) until it is rotated,
        which populated multiple versions for the s3 object, therefore we need
        to ensure we only scan the latest version of it.
        """
        test_user_id = random.randint(1000, 2000)
        test_query_id = 5678910
        s3_stl_bucket = "padb-monitoring-test"
        s3_client = self.get_s3_client(cluster)
        account_id = cluster.get_padb_conf_value("cluster_owner_aws_acct_id")
        cluster_id = str(random.randint(10000, 20000))
        immutable_cluster_id = str(uuid.uuid4())
        account_id_sha256 = hashlib.sha256(
            account_id.encode("utf-8")).hexdigest()[:8]
        stl_date = date.today().strftime("%Y%m%d")
        now_padb_time = (int(time.time()) - 946684800) * (10**6)
        now_suffix = ".{0}.gz".format(now_padb_time)
        s3_key = '{}/{}/{}/{}/{}/cs/{}/raw/{}'.format(
                account_id_sha256, account_id, TEST_STL_NAME,
                immutable_cluster_id, stl_date, cluster_id,
                'LN_stl_concurrency_scaling_query_mapping' + now_suffix)
        test_query_stl = "select * from {} where userid = {};".format(
                TEST_STL_NAME, test_user_id)
        test_query_stcs = "select * from {} where userid = {};".format(
                TEST_STCS_NAME, test_user_id)
        test_gucs = {
            "s3_stl_bucket": s3_stl_bucket,
            "enable_query_sys_table_log_from_S3": "true",
            "stl_enable_CN_fetching_LN_STL_from_s3": "true",
            "enable_scan_burst_log_from_s3": "true",
            "cluster_id": str(uuid.uuid4().int & 0xFFFFF),
            "immutable_cluster_id": immutable_cluster_id,
            "enable_monitoring_connector_main_cluster": "true",
            "enable_stl_immutable_cluster_id_partition": "true",
            "enable_arcadia": "true",
            "is_arcadia_cluster": "true",
        }
        with cluster_session(
                gucs=test_gucs, clean_db_before=True, clean_db_after=True):
            version_count = 10
            stl_content = ""
            # Do upload with the same s3 key for N times (N = version_count)
            # each time we add one more record into the content, then we check
            # the number of records by querying the test stl table to verify if
            # it's consistent with the most recent version.
            for i in range(version_count):
                stl_content += "{}|{}|{}|{}\n".format(
                    test_user_id, test_query_id + i, TEST_CS_QUERY, cluster_id)
                SystemTableOutsideClusterHelper.\
                    create_file_with_content_push_to_s3(
                        s3_client, stl_content, TEST_STL_NAME, s3_key,
                        s3_stl_bucket, cluster_id, immutable_cluster_id)
                # Verify if both STL and STCS view can get the correct data.
                for query in [test_query_stl, test_query_stcs]:
                    res = run_priviledged_query(
                        cluster, self.db.cursor(), query)
                    log.info("Results: {}".format(res))
                    assert len(res) == (i + 1), \
                        "There should be {} records in results of ({})".format(
                            (i + 1), query)

    def test_get_large_number_of_stl_objects_from_s3(
            self, cluster, cluster_session, db_session):
        """
        This test is to verify if there are large number of objects in an s3
        partition, the system table iterator should be able to scan all objects
        without hitting any exception, e.g. when the number of s3 objects in
        current partition is larger than list_stl_s3_page_size so the
        STL iterator has to scan multiple pages.
        SIM: https://issues.amazon.com/RedshiftDP-52266
        """
        test_user_id = random.randint(1000, 2000)
        test_query_id = 5678910
        s3_stl_bucket = "padb-monitoring-test"
        s3_client = self.get_s3_client(cluster)
        account_id = cluster.get_padb_conf_value("cluster_owner_aws_acct_id")
        cluster_id = str(random.randint(10000, 20000))
        immutable_cluster_id = str(uuid.uuid4())
        account_id_sha256 = hashlib.sha256(
            account_id.encode("utf-8")).hexdigest()[:8]
        stl_date = date.today().strftime("%Y%m%d")

        # When query from stl_ view, main LN would also scan burst stl log from
        # s3 if enable_scan_burst_log_from_s3 == true
        test_query_stl = "select * from {} where userid = {};".format(
                TEST_STL_NAME, test_user_id)
        # Also check if stcs_ view, which is designed specifically for burst
        # log in s3, can return correct results.
        test_query_stcs = "select * from {} where userid = {};".format(
                TEST_STCS_NAME, test_user_id)

        list_stl_s3_page_size = 100
        test_gucs = {
            "s3_stl_bucket": s3_stl_bucket,
            "enable_query_sys_table_log_from_S3": "true",
            "stl_enable_CN_fetching_LN_STL_from_s3": "true",
            "enable_scan_burst_log_from_s3": "true",
            "cluster_id": str(uuid.uuid4().int & 0xFFFFF),
            "immutable_cluster_id": immutable_cluster_id,
            "enable_monitoring_connector_main_cluster": "true",
            "enable_stl_immutable_cluster_id_partition": "true",
            "enable_arcadia": "true",
            "is_arcadia_cluster": "true",
            "list_stl_s3_page_size": list_stl_s3_page_size,
        }
        with cluster_session(
                gucs=test_gucs, clean_db_before=True, clean_db_after=True):
            object_count = list_stl_s3_page_size * 2 + 10
            now_padb_time = (int(time.time()) - 946684800) * (10**6)
            for i in range(object_count):
                now_suffix = ".{0}.gz".format(now_padb_time - i)
                s3_key = '{}/{}/{}/{}/{}/cs/{}/raw/{}'.format(
                        account_id_sha256, account_id, TEST_STL_NAME,
                        immutable_cluster_id, stl_date, cluster_id,
                        'LN_stl_concurrency_scaling_query_mapping' + now_suffix)
                stl_content = "{}|{}|{}|{}\n".format(
                    test_user_id, test_query_id + i, TEST_CS_QUERY, cluster_id)
                SystemTableOutsideClusterHelper.\
                    create_file_with_content_push_to_s3(
                        s3_client, stl_content, TEST_STL_NAME, s3_key,
                        s3_stl_bucket, cluster_id, immutable_cluster_id)
            # Verify if both STL and STCS view can get the correct data.
            for query in [test_query_stl, test_query_stcs]:
                res = run_priviledged_query(
                    cluster, self.db.cursor(), query)
                log.info("Results: {}".format(res))
                assert len(res) == object_count, \
                    "There should be {} records in results of ({})".format(
                        object_count, query)
