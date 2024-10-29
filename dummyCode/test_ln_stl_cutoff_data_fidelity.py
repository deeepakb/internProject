# Copyright 2024 Amazon.com, Inc. or its affiliates All Rights Reserved
import datetime
import logging
import os
import pytest
import time
import uuid

from raff.common.base_test import (run_priviledged_query,
                                   run_privileged_query_no_result)
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.monitoring.monitoring_test import MonitoringTestSuite
from raff.monitoring.monitoring_test import UploadRecordStatus
from raff.monitoring.monitoring_test import NodeType, ClusterType

XEN_ROOT = os.environ["XEN_REL"]
watermark_dir_leader = XEN_ROOT + "/../data/log.leader/externalizer"
CONNECTOR_INTERVAL_SEC = 30

log = logging.getLogger(__name__)


@pytest.mark.localhost_only
@pytest.mark.serial_only
class TestStlLnDataFidelity(MonitoringTestSuite):
    """
    This test ensures records are fetched correctly via the corresponding stl
    from both local and S3 on LN with the recent changes to the LN_cutoff time
    calculation.
    """

    # STL tables to be used for validation
    stl_table_to_verify = "stl_query"
    s3client_file_count_query = """
                SELECT key, SPLIT_PART(stl_s3client.key, '/', 6) as cluster_type
                FROM stl_s3client WHERE query = (SELECT max(query) FROM stl_query
                WHERE querytxt like '%select count(*) from {} where userid={}%')
                GROUP BY key, cluster_type;
                """
    current_time = int(time.time())
    today_midnight = current_time//86400*86400
    today_time_2359 = today_midnight + 82800

    # A dummy userid to identify the records used for this test (Setting it as
    # current time to avoid conflicting results from other tests)
    userid = str(current_time)
    session_ctx = SessionContext(user_type=SessionContext.BOOTSTRAP)

    CONNECTOR_DEFAULT_GUCS = {
        "enable_monitoring_connector_main_cluster": "true",
        "monitoring_connector_interval_main_cluster_seconds": 10,
        "log_connector_max_batch_size_mb": 3,
        "enable_query_sys_table_log_from_S3": "true",
        "s3_stl_bucket": "padb-monitoring-test",
        "stl_enable_CN_fetching_LN_STL_from_s3": "true",
        "enable_stl_immutable_cluster_id_partition": "true",
        "stl_query_range_in_days": 14,
        "enable_bootstrap_user_query_stl_from_s3": "true",
        "query_ln_stl_log_from_s3_buffer_secs": 60,
        "enable_prefer_ln_stl_log_scan_from_s3": "true"
    }

    def get_connector_gucs_dict(self, cluster):
        """
        Retuen dictionary of connector_gucs.
        Since the test also deletes S3 objects, use random
        virtual cluster id to avoid deleting other test's
        S3 objects.
        """
        connector_gucs = dict(self.CONNECTOR_DEFAULT_GUCS)
        immutable_cluster_id = uuid.uuid4()
        gconf_event = self.set_stl_imm_cluster_id_event(
            cluster, immutable_cluster_id)
        gconf_event = '{}|{}'.format(
            gconf_event, 'EtMonitoringTracing,level=ElDebug5')
        cluster_id = cluster.get_padb_conf_value('cluster_id')
        connector_gucs.update(cluster_id=cluster_id, gconf_event=gconf_event)
        return connector_gucs

    def get_date_str(self, number_of_days_ago):
        """
        Returns the date string in the format YYYYMMDD for the date which is
        number_of_days_ago
        """
        return datetime.datetime.fromtimestamp(
            self.today_midnight - number_of_days_ago*86400).strftime("%Y%m%d")

    def is_query_date_same_as_document_create_date(self):
        """
        Check if the day has changed from the time the test started to the time
        the queries are getting executed. Occurs if the test is started around
        23:59 GMT
        """
        query_time = int(time.time())
        query_time_midnight = query_time//86400*86400
        return query_time_midnight == self.today_midnight

    def get_query_results(self, cluster, ctx, query_text):
        """
        Get query results.
        """
        conn_params = cluster.get_conn_params()
        with DbSession(conn_params, session_ctx=ctx) as db_session:
            with db_session.cursor() as cursor:
                log.info('query_text: {}'.format(query_text))
                cursor.execute(query_text)
                query_results = cursor.fetchall()
                log.info(query_results)
                return query_results

    def get_test_query_results_count(self, cluster, table_name):
        """
        Get the count of all the records from table_name
        for the userid
        """
        return self.get_query_results(
            cluster, self.session_ctx,
            "select count(*) from {} where userid={};".format(
                table_name, self.userid))

    def get_timstamp_from_result(self, result):
        # Loop through each tuple in the list
        for tuple in result:
            # Loop through each item in the tuple
            for item in tuple:
                # Check if 'ln_stl_cutoff_time_' is in the item
                if 'ln_stl_cutoff_time_' in item:
                    # Split the item and return the timestamp
                    timestamp = item.split(':')[-1].strip()
                    # Check if the timestamp is greater than 0
                    assert int(timestamp) > 0, "Incorrect timestamp found"
                    return int(timestamp)
        # If no timestamp is found, raise an assertion error
        assert False, "No timestamp found"

    def create_file_contents(self, total_hours_ago, node_identifier):
        """
        Returns the string of dummy records created
        """
        return self.userid + '|' + str(total_hours_ago) + '|1|' + \
            str(node_identifier) + '\n' + \
            self.userid + '|' + str(total_hours_ago) + '|2|' + \
            str(node_identifier) + '\n'

    def create_test_stl_file_on_local(self, table_name, days_old,
                                      node_type, hours_ago=0):
        """
        Format of the dummy records created:
        userId|total_hours_ago|Count|ClusterIdentifier
        ClusterIdentifier: 9:LN
        Eg: 1627061865|144|2|9 => LN record with timestamp 6 days ago
         1627061865|0|2|9 => LN record with timestamp of current day
         1627061865|2|2|0 => LN record with timestamp 2 hours ago
        """
        total_hours_ago = days_old * 24 + hours_ago
        if hours_ago == 0:
            base_time = self.today_midnight
        else:
            base_time = self.today_time_2359
        if node_type == NodeType.Leader:
            MonitoringTestSuite.\
                create_test_file_with_log_time_in_LN(
                    table_name, [total_hours_ago],
                    base_time, self.create_file_contents(
                        total_hours_ago, 9))

    def create_test_stl_file_for_ln_and_cn_local(self, table_name, days_old,
                                                 node_type, hours_ago=0):
        """
        Format of the dummy records created:
        userId|total_hours_ago|Count|ClusterIdentifier
        ClusterIdentifier: 0: CN0, 1: CN1, 2: CN2, 9:LN
        Eg: 1627061865|168|1|2 => CN2 record with timestamp 7 days ago
         1627061865|144|2|9 => LN record with timestamp 6 days ago
         1627061865|0|2|0 => CN0 record with timestamp of current day
         1627061865|2|2|0 => CN0 record with timestamp 2 hours ago
         1627061865|27|2|0 => CN0 record with timestamp 2 days and 3 hours ago
        """
        total_hours_ago = days_old * 24 + hours_ago
        if hours_ago == 0:
            base_time = self.today_midnight
        else:
            base_time = self.today_time_2359
        if node_type == NodeType.Leader:
            MonitoringTestSuite.\
                create_test_file_with_log_time_in_LN(
                    table_name, [total_hours_ago],
                    base_time, self.create_file_contents(
                        total_hours_ago, 9))
        elif node_type == NodeType.Compute_0:
            MonitoringTestSuite.\
                create_test_file_with_log_time_in_CN_0(
                    table_name, [total_hours_ago],
                    base_time, self.create_file_contents(
                        total_hours_ago, 0))
        elif node_type == NodeType.Compute_1:
            MonitoringTestSuite.\
                create_test_file_with_log_time_in_CN_1(
                    table_name, [total_hours_ago],
                    base_time, self.create_file_contents(
                        total_hours_ago, 1))
        elif node_type == NodeType.Compute_2:
            MonitoringTestSuite.\
                create_test_file_with_log_time_in_CN_2(
                    table_name, [total_hours_ago],
                    base_time, self.create_file_contents(
                        total_hours_ago, 2))
        else:
            log.error("Input node type is invalid")

    def create_test_files_on_local_and_s3(self, cluster, s3_client, connector_gucs,
                                          start_day, total_number_of_days,
                                          table_name, node_types, upload_to_s3=True):
        """
        Create dummy test files from current day to total_number_of_days old in Leader
        node on local and ensure they are uploaded to S3.
        """
        # Populate logs on local
        for days_old in range(start_day, start_day + total_number_of_days):
            self.create_test_stl_file_on_local(table_name, days_old, node_types)
            # The following logs helps to find on which day and for
            # which node type files were created.
            log.debug("Created test files for : " + str(days_old) +
                      " days ago and node: " + node_types.name)

        if upload_to_s3:
            # Wait for logs to be uploded onto S3
            MonitoringTestSuite.\
                clean_stl_on_s3_and_wait_for_new_stl_upload_to_s3(
                    cluster, s3_client, connector_gucs,
                    table_name,
                    total_number_of_days,
                    0, ClusterType.Main,
                    [])
            log.info("Created test STL files on local and S3")

    def verify_LN_log_scan_prefer_S3_for_stl(
            self, cluster, cluster_session, s3_client,
            connector_gucs, table_name):
        """
        Verifies that logs for the Leader Node (LN) are scanned preferentially from S3.
        """
        record_count_per_file = 2
        total_number_of_days = 8
        with cluster_session(gucs=connector_gucs, clean_db_before=True,
                             clean_db_after=True):
            try:
                # Create test files on local for 8 days and externalize logs to S3
                self.create_test_files_on_local_and_s3(
                    cluster, s3_client, connector_gucs, 1, total_number_of_days,
                    table_name, NodeType.Leader)
                # Calculate the total number of distinct records expected
                total_distinct_records_created = total_number_of_days * \
                    record_count_per_file
                # We read the ln_cutoff_time from watermark file. During multiple reads
                # to fetch consistent file timestamp from watermark file we always
                # return last_log_file_timestamp uploaded to S3 whose upload time <=
                # (query_start_time - query_ln_stl_log_from_s3_buffer_secs)
                # as the s3_stl_cutoff_time on LN. So here we wait for 90 secs before
                # getting the count of query results from the STL table to make
                # sure the logs are scanned from S3 instead of local.
                time.sleep(90)
                # Get the count of query results from the STL table
                stl_query_result_count = self.get_test_query_results_count(
                    cluster, table_name)
                assert stl_query_result_count[0][0] == total_distinct_records_created, \
                    'Incorrect records found'
                s3client_res = self.get_query_results(
                    cluster, self.session_ctx, self.s3client_file_count_query.format(
                        self.stl_table_to_verify, self.userid))
                log.info("query_res->{}".format(s3client_res))
                assert len(s3client_res) == total_number_of_days, \
                    "Incorrect number of files scanned from S3"
            finally:
                self.clean_stl_log_files_on_s3_for_dates(
                    cluster, s3_client, connector_gucs,
                    table_name, ClusterType.Main,
                    self.get_date_str(total_number_of_days))

    def test_verify_stl_scan_prefers_s3_logs_over_local_for_ln(
            self, cluster, cluster_session, s3_client):
        """
        Test to verify that STL data on the Leader node prefers to scan logs from S3
        and only scans remaining logs from local storage.
        Steps:
        1. Create dummy STL query records from the current day to the previous 7 days.
        2. Externalize the current day to the previous 7 days records to S3.
        3. Get the count of records from stl_query to ensure that we have correct
           number of records as result from STL table.
        4. Get the file count from stl_s3client, and each file contains 2 records
           stl_s3client should retrurn 8 files.
        """
        connector_gucs = self.get_connector_gucs_dict(cluster)
        self.verify_LN_log_scan_prefer_S3_for_stl(
            cluster, cluster_session, s3_client, connector_gucs,
            self.stl_table_to_verify)

    def test_verify_stl_retrieves_all_logs_from_both_local_and_s3(
            self, cluster, cluster_session, s3_client):
        """
        Test to verify that PADB retrieves STL data on the Leader node scanning partial
        logs from both S3 and local storage without log loss.
        Steps:
        1. Create dummy STL query records from the three days to seven days old.
        2. Externalize the records to S3 and clean up the files from local storage.
        3. Create three more STL query records with the most recent timestamp on local.
        4. Get the count of records from stl_query to ensure that we have correct
           number of records as result from STL table without log loss.
        5. Get the file count from stl_s3client, and each file contains 2 records
           stl_s3client should retrurn 4 files.
        """
        connector_gucs = self.get_connector_gucs_dict(cluster)
        connector_gucs.update(
            {"monitoring_connector_interval_main_cluster_seconds": 20})
        immutable_cluster_id = cluster.get_padb_conf_value(
                        "immutable_cluster_id")
        with cluster_session(gucs=connector_gucs, clean_db_before=True,
                             clean_db_after=True):
            try:
                # Create test files from three days to seven days old and externalize
                # them to S3.
                self.create_test_files_on_local_and_s3(
                    cluster, s3_client, connector_gucs, 3, 4,
                    self.stl_table_to_verify, NodeType.Leader)
                # Clean up local stl_query log files.
                self.cleanup_log_files_by_type(self.stl_table_to_verify)
                # We read the ln_cutoff_time from watermark file. During multiple reads
                # to fetch consistent file timestamp from watermark file we always
                # return last_log_file_timestamp uploaded to S3 whose upload time <=
                # (query_start_time - query_ln_stl_log_from_s3_buffer_secs)
                # as the s3_stl_cutoff_time on LN. So here we wait for 90 secs before
                # getting the count of query results from the STL table to make
                # sure the logs are scanned from S3 instead of local.
                time.sleep(120)
                # Create test files from current date to three days old.
                self.create_test_files_on_local_and_s3(
                    cluster, s3_client, connector_gucs, 0, 3,
                    self.stl_table_to_verify, NodeType.Leader, False)
                # Get the count of query results from the STL table
                stl_query_result_count = self.get_test_query_results_count(
                    cluster, self.stl_table_to_verify)
                assert stl_query_result_count[0][0] == 14, \
                    'Incorrect records found'
                s3client_res = self.get_query_results(
                    cluster, self.session_ctx, self.s3client_file_count_query.format(
                        self.stl_table_to_verify, self.userid))
                log.info("query_res->{}".format(s3client_res))
                assert len(s3client_res) >= 4, \
                    "Incorrect number of files scanned from S3"
            finally:
                self.clean_stl_log_files_on_s3_for_dates(
                    cluster, s3_client, connector_gucs,
                    self.stl_table_to_verify, ClusterType.Main,
                    [self.get_date_str(day) for day in range(8, 0, -1)],
                    immutable_cluster_id=immutable_cluster_id)

    def test_ln_cutoff_time_same_as_watermark_cutoff_time(
            self, cluster, cluster_session, db_session):
        """
        First, this test create a fake cut off timestamp two days ago.
        Then writes the timestamp to water mark file. Later we fetch the
        ln_cutoff_time from stl_print and check if the cutoff_time is the most recent
        log file timestamp read from water mark file.
        """
        guc = {'enable_query_sys_table_log_from_S3': 'true',
               'enable_prefer_ln_stl_log_scan_from_s3': 'true'}
        # Choose a stl table, which has no write from PADB during this test
        stl_table_to_verify = "stl_concurrency_scaling_query_mapping"
        test_query = 'SELECT * FROM {};'.format(stl_table_to_verify)
        stl_query = """
        SELECT xid, pid FROM stl_query where query = (SELECT max(query) from
        stl_query where querytxt like '%{}%') and starttime >= '{}';
        """
        stl_print_query = """
        SELECT message FROM stl_print WHERE recordtime >= '{}' and message like
        'xid: {}%' and pid = {} limit 1;
        """
        with cluster_session(gucs=guc, clean_db_before=True):
            with db_session.cursor() as cursor:
                try:
                    cursor.execute("select now();")
                    test_start_time = cursor.fetch_scalar()
                    # Create a fake cutoff time (2 days ago) and persist in watermark
                    # file.
                    today_midnight = int(time.time())//86400*86400
                    cutoff_time = (today_midnight - 946684800 - 48 * 3600)\
                        * (10**6)
                    self.persist_externalizer_watermark_v2(
                        stl_table_to_verify, cutoff_time,
                        UploadRecordStatus.Complete)
                    run_privileged_query_no_result(
                            cluster, self.db.cursor(),
                            test_query)
                    test_query_res = run_priviledged_query(
                            cluster, self.db.cursor(),
                            stl_query.format(test_query, test_start_time))
                    stl_print_res = run_priviledged_query(
                            cluster, self.db.cursor(),
                            stl_print_query.format(
                                test_start_time, test_query_res[0][0],
                                test_query_res[0][1]))
                    expected_ln_cutoff_time = self.get_timstamp_from_result(
                        stl_print_res)
                    # We assign ln_cutoff time as the last_logfile_timestamp + 1 added
                    # to S3 by fetching it from water mark file.
                    self.verify_externalizer_watermark(
                        stl_table_to_verify, expected_ln_cutoff_time - 1,
                        UploadRecordStatus.Complete)
                finally:
                    self.cleanup_directory(watermark_dir_leader)

    @pytest.mark.skip(reason="Redshift-70244")
    def test_cn_scans_ln_stl_logs_from_s3(self, cluster, cluster_session, db_session,
                                          s3_client):
        """
        The test validates two things:
        1. CN always scans local logs.
        2. CN scans LN STL logs first from S3 and scans remaining from local if needed.
        Steps:
        1.Create 8 days logs on both LN and CN with two records in each file.
        2. Externalize all the logs generated on local to S3.
        3. Verify that the query count is the same from stl_scan after logs uploaded
           to S3.
        4. Verify that the number of rows scanned on LN after S3 upload is 0 and LN
           logs are scanned by CNs.
        """
        node_types = [NodeType.Leader, NodeType.Compute_0,
                      NodeType.Compute_1, NodeType.Compute_2]
        connector_gucs = self.get_connector_gucs_dict(cluster)
        immutable_cluster_id = cluster.get_padb_conf_value(
                        "immutable_cluster_id")
        stl_scan_query = """
        SELECT "rows",
                CASE
                    WHEN slice > 1000 THEN 'LN'
                    WHEN slice < 1000 THEN 'CN'
                    ELSE 'Unknown'
                END AS node_type
            FROM stl_scan
            WHERE
                query = (
                    SELECT MAX(query)
                    FROM stl_query
                    WHERE querytxt = 'select count(*) from {} where userid={};'
                )
                AND segment IN (0, 1)
                AND starttime >= '{}';
                """
        record_count_per_file = 2
        total_number_of_days = 8
        with cluster_session(gucs=connector_gucs, clean_db_before=True):
            try:
                test_start_time = self.current_db_time(db_session)
                # Create local log files on LN and CN's.
                for days_old in range(0, total_number_of_days, 1):
                    for node_type in node_types:
                        self.create_test_stl_file_for_ln_and_cn_local(
                            self.stl_table_to_verify, days_old, node_type)
                # Upload all local logs to S3.
                MonitoringTestSuite.\
                    clean_stl_on_s3_and_wait_for_new_stl_upload_to_s3(
                        cluster, s3_client, connector_gucs,
                        self.stl_table_to_verify,
                        total_number_of_days,
                        total_number_of_days, ClusterType.Main,
                        [self.get_date_str(day)
                         for day in range(total_number_of_days, -1, -1)],
                        immutable_cluster_id=immutable_cluster_id)
                log.info("Created test STL files on local and S3")
                # We read the ln_cutoff_time from watermark file. During multiple reads
                # to fetch consistent file timestamp from watermark file we always
                # return last_log_file_timestamp uploaded to S3 whose upload time <=
                # (query_start_time - query_ln_stl_log_from_s3_buffer_secs)
                # as the s3_stl_cutoff_time on LN. So here we wait for 90 secs before
                # getting the count of query results from the STL table to make
                # sure the logs are scanned from S3 instead of local.
                time.sleep(90)
                total_distinct_records_created = total_number_of_days * \
                    len(node_types) * record_count_per_file
                stl_query_result_count = self.get_test_query_results_count(
                    cluster, self.stl_table_to_verify)
                log.info("total_no_of_records->{}".format(stl_query_result_count))
                # Assert query count is same of records created.
                assert stl_query_result_count[0][0] == total_distinct_records_created, \
                    'Incorrect records found'
                # Validate LN after S3 upload should scan 0 rows.
                stl_scan_res = self.get_query_results(
                        cluster, self.session_ctx, stl_scan_query.format(
                            self.stl_table_to_verify, self.userid, test_start_time))
                # Verify total rows returned from stl_query are equal to
                # rows returned from stl_scan also validate no.of rows scanned on
                # LN > 0.
                ln_row_cnt = 0
                stl_scan_row_count = 0
                for row in stl_scan_res:
                    row_count, node_type = row
                    if node_type == 'LN':
                        assert row_count == 0
                        ln_row_cnt += 1
                    elif node_type == 'CN':
                        assert row_count >= 0
                    stl_scan_row_count += row_count
                assert ln_row_cnt == 1,\
                    'No.of LN should be equal to 1'
                assert stl_scan_row_count == total_distinct_records_created,\
                    'Incorrect records found'
                # Verify all LN files are scanned from S3.
                s3client_res = self.get_query_results(
                    cluster, self.session_ctx, self.s3client_file_count_query.format(
                        self.stl_table_to_verify, self.userid))
                log.info("query_res->{}".format(s3client_res))
                assert len(s3client_res) == 8, \
                    "Incorrect number of files scanned from S3"
            finally:
                # Clean stl_query_detail local log files
                self.cleanup_log_files_by_type(self.stl_table_to_verify)
                # Clean up local stl_query log files.
                self.clean_stl_log_files_on_s3_for_dates(
                    cluster, s3_client, connector_gucs,
                    self.stl_table_to_verify, ClusterType.Main,
                    [self.get_date_str(day)
                     for day in range(total_number_of_days, -1, -1)],
                    immutable_cluster_id=immutable_cluster_id)
