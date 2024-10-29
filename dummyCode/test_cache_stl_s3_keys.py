# Copyright 2021 Amazon.com, Inc. or its affiliates All Rights Reserved
from datetime import datetime, timedelta
import logging
import pytest
import random
import time
import uuid

from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.monitoring.monitoring_test import MonitoringTestSuite, ClusterType,\
    NodeType, leader_log, compute_0_log, compute_1_log, compute_2_log

log = logging.getLogger(__name__)

@pytest.mark.localhost_only
@pytest.mark.serial_only
class TestCacheSTLS3Keys(MonitoringTestSuite):

    """
    Test for STL S3 key cache.
    """
    test_stl_table_name = "stl_print"
    padb_epoc_sec_diff = 946684800

    def get_date_str(self, number_of_days_ago):
        """
        Returns the date string in the format YYYYMMDD for the date which is
        number_of_days_ago
        """
        return datetime.fromtimestamp(self.stl_file_base_time - \
                number_of_days_ago * 86400).strftime("%Y%m%d")

    def clean_stl_files(self, guc, history_days, cluster, s3_client):
        # In case other tests generated log files, we clean history_days + 7.
        MonitoringTestSuite.clean_stl_log_files_on_s3_for_dates(
            cluster, s3_client, guc,
            self.test_stl_table_name, ClusterType.Main,
            [self.get_date_str(day) for day in range(history_days + 7, 0, -1)])

        # Clean up local STL files
        self.cleanup_log_files_by_type(self.test_stl_table_name)
        for d in [leader_log, compute_0_log, compute_1_log, compute_2_log]:
            MonitoringTestSuite.cleanup_directory(d)

    def prepare_stl_data(self, guc, days, cluster, s3_client):
        """
        Create mocked STL data on cluster local disk.
        """
        self.clean_stl_files(guc, days, cluster, s3_client)
        files_list = []
        total_rows = 0
        log.info("cluster:{} message:{}".format(self.cluster_id, self.message))
        for day in range(days, 1, -1):
            for node_type in NodeType:
                # process|pid|node|slice|recordtime|message
                row = "999|999|{}|0|{}|{}\n".format(node_type,
                        (self.stl_file_base_time - day * 86400 - \
                        self.padb_epoc_sec_diff) * (10**6), self.message)
                total_rows += 1
                files = MonitoringTestSuite.\
                        create_test_file_with_log_time_by_node_type(
                                self.test_stl_table_name, node_type, [day * 24],
                                self.stl_file_base_time, row)
                log.info("created_files:{}".format(files))
                [files_list.append(f) for f in files]

        self.cleanup_watermark_directory()
        # Wait and upload local STL to S3 with timeout 300 seconds.
        time.sleep(guc['monitoring_connector_interval_seconds'] * 20)
        self.wait_cleanup_for_teardown_to_complete(cluster, 300)
        return total_rows, files_list

    def get_query_result(self, db_session, query_text):
        with db_session.cursor() as cursor:
            cursor.execute(query_text)
            result = cursor.fetchall()
            row_count = int(result[0][0])
            return row_count

    def fetch_stl_test_rows_count(self, db_session):
        query_text = "select count(*) from {} where message = '{}';".format(
                self.test_stl_table_name, self.message)
        return self.get_query_result(db_session, query_text)

    def get_cache_hit_count(self, db_session, event_time):
        query_text = "select count(*) from stl_event_trace " \
                "where message like 'Hit STL S3 key cache%' " \
                "and eventtime > '{}' " \
                "and sliceid <> 20000;".format(event_time)
        return self.get_query_result(db_session, query_text)

    def init_test_config(self, cluster):
        self.message = str(uuid.uuid4())
        self.stl_file_base_time = int(time.time())

        # Modify cluster_id to avoid S3 path conflict when multiple jobs run
        # this test cluster upload STL to S3.
        random_number = random.randint(0, 0x0FFFFFFF)
        self.cluster_id = str(random_number)
        self.immutable_cluster_id = str(uuid.uuid4())

        guc = {
            "enable_monitoring_connector_main_cluster": "true",
            "monitoring_connector_interval_main_cluster_seconds": 10,
            "log_connector_max_batch_size_mb": 3,
            "monitoring_connector_interval_seconds": 3,
            "enable_query_sys_table_log_from_S3": "true",
            "s3_stl_bucket": "padb-monitoring-test",
            "stl_enable_CN_fetching_LN_STL_from_s3": "true",
            "stl_s3_key_cache_ttl_in_hours": 24,
            "cluster_id": self.cluster_id,
            "immutable_cluster_id": self.immutable_cluster_id,
            "slices_per_node": "2",
            'query_ln_stl_log_from_s3_buffer_secs': 0
        }

        guc['gconf_event'] = '{}|{}'.format(str(cluster.get_padb_conf_value(
            "gconf_event")), 'EtMonitoringTracing,level=ElDebug5')

        return guc

    @pytest.mark.session_ctx(user_type=SessionContext.BOOTSTRAP)
    def test_hit_stl_s3_keys_cache_should_return_all_records(
            self, cluster, cluster_session, db_session, s3_client):
        """
        STL query fetches STL data from S3 when local STL data is not enough.
        This test will create 6 days local STL data and upload to S3, then we
        delete Local STL data to trigger PADB scan STL from S3. We will validate
        that the STL query has hit cache and returned correct data.
        """
        guc = self.init_test_config(cluster)
        # We use timestamp to query event from stl_event_trace. In case test
        # host's timestamp has gap with cluster, we look back 3 seconds.
        test_start_time = (datetime.now() + timedelta(seconds=-3)).\
                strftime("%Y-%m-%d %H:%M:%S")
        history_data_duration = 6
        self.clean_stl_files(guc, history_data_duration, cluster, s3_client)

        with cluster_session(
                gucs=guc, clean_db_before=True, clean_db_after=True):
            # Create STL data with 6 days on cluster's local disk.
            total_rows, file_list_ordered_by_date = self.prepare_stl_data(
                    guc, history_data_duration, cluster, s3_client)

            query_runs = 0;
            for f in file_list_ordered_by_date:
                MonitoringTestSuite.delete_local_files_by_path([f])
                log.info('Delete file: {}'.format(f))
                row_count = self.fetch_stl_test_rows_count(db_session)
                assert total_rows == row_count, "stl rows count doesn't match"
                query_runs += 1

            # All queries executed on CN slices should hit cache, except the
            # first query. node_count * slice_count * (query_runs - 1)
            expected_min_cache_hit_count = cluster.node_count * \
                    int(cluster.get_padb_conf_value("slices_per_node")) * \
                    (query_runs - 1)

            actual_cache_hit_count = self.get_cache_hit_count(
                    db_session, test_start_time)

            assert actual_cache_hit_count >= expected_min_cache_hit_count, \
                    'STL S3 key cache hit less than expected'

    @pytest.mark.session_ctx(user_type=SessionContext.BOOTSTRAP)
    def test_hit_stl_cache_with_invalid_s3_files_should_refresh_cache(
            self, cluster, cluster_session, db_session, s3_client):
        """
        This test verifies that PADB should delete cache when PADB cached
        invalided key. This test will create 6 days STL data on local and upload
        to S3, then we delete all local STL so that PADB read STL from S3
        and update cache. Then we delete 1 day's STL file on S3 to make S3 cache
        invalid. Then run query, assume the query should return the not deleted
        records.
        """
        guc = self.init_test_config(cluster)
        # We use timestamp to query event from stl_event_trace. In case test
        # host's timestamp has gap with cluster, we look back 3 seconds.
        test_start_time = (datetime.now() + timedelta(seconds=-3)).\
                strftime("%Y-%m-%d %H:%M:%S")
        history_data_duration = 6
        self.clean_stl_files(guc, history_data_duration, cluster, s3_client)

        with cluster_session(
                gucs=guc, clean_db_before=True, clean_db_after=True):
            # Create 6 days' STL data on cluster's local disk.
            total_rows, file_list_ordered_by_date = self.prepare_stl_data(
                    guc, history_data_duration, cluster, s3_client)

            # Delete local STL, so that STL query have to scan STL from S3.
            MonitoringTestSuite.delete_local_files_by_path(
                    file_list_ordered_by_date)

            # Run STL query, this query should set cache.
            row_count = self.fetch_stl_test_rows_count(db_session)
            assert total_rows == row_count, "stl rows count doesn't match"

            # Delete STL files on S3 for day1 to make cache invalid.
            MonitoringTestSuite.clean_stl_log_files_on_s3_for_dates(
                cluster, s3_client, guc,
                self.test_stl_table_name, ClusterType.Main,
                [self.get_date_str(2)])

            # The query should hit invalid cache and failed, then the query
            # return the not deleted records.
            row_count = self.fetch_stl_test_rows_count(db_session)
            expected_row_count = total_rows - len(NodeType)
            assert expected_row_count == row_count, 'Row count doesnt match'
