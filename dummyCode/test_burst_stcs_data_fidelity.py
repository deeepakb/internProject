# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import os
import time
import pytest
import sqlparse

from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst
from raff.common.aws_clients.redshift_client import RedshiftClient
from raff.common.db.db_exception import Error
from raff.common.db.redshift_db import RedshiftDb
from raff.common.profile import Profiles
from raff.common.region import Regions
from raff.util.utils import run_bootstrap_sql
from plumbum import ProcessExecutionError
from io import open

__all__ = [setup_teardown_burst]
log = logging.getLogger(__name__)

S3_BUCKET = 'redshift-monitoring-qa'
DB_USER = 'stcs_test_user'
DB_USER_PW = 'Testing1234'
XEN_ROOT = os.environ['XEN_ROOT']
PATH_BASE = os.path.join(XEN_ROOT, "test/raff/burst/testfiles")
BURST_QUERIES_FILE = os.path.join(
  PATH_BASE, "test_burst_stcs_data_fidelity_burst_queries.sql")
USER_ID_COL_NAME = 'userid'

CUSTOM_GUCS = {
    'burst_mode': '3',
    's3_stl_bucket': S3_BUCKET,
    'burst_max_idle_time_seconds': '3600',
    'try_burst_first': 'true',
    'enable_short_query_bias': 'false',
    'systable_subscribed_log_rotation_time_in_seconds': '10',
    'monitoring_connector_interval_seconds': '10',
    "enable_monitoring_connector_main_cluster": "false",
    "enable_burst_async_acquire": "false"}

STCS_TABLE_QUERY = '''
SELECT DISTINCT tablename tn
FROM pg_catalog.pg_tables
WHERE schemaname = 'pg_catalog'
AND tablename LIKE 'stcs_%'
AND \'{}\' = SOME(SELECT column_name
    FROM information_schema.columns
    WHERE table_name = tn)'''.format(USER_ID_COL_NAME)

PREPARE_USER_QUERIES = [
    'drop user if exists {};'.format(DB_USER),
    'create user {} with password \'{}\';'.format(DB_USER, DB_USER_PW),
    'grant all on all tables in schema public to {};'.format(DB_USER)
]

VALIDATION_TABLES = ['stcs_query', 'stcs_querytext', 'stcs_scan']

@pytest.mark.load_data
@pytest.mark.cluster_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.serial_only
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_GUCS)
class TestBurstSTCSDataFidelity(BurstTest):
    def _request_cleanup_for_teardown(
            self, cluster):
        """
        send xpx cleanup_for_teardown to cluster
        """
        try:
            cluster.run_xpx("cleanup_for_teardown")
            return True
        except Error as e:
            log.info(e.pgerror)
            return False
        except ProcessExecutionError as pe:
            log.info(pe)
            return False

    def _wait_cleanup_for_teardown_to_complete(self, cluster):
        """
        This method continue to send xpx cleanup_for_teardown command,
        and wait until it is done
        """
        log.info("_wait_cleanup_for_teardown_to_complete starts")
        connector_gucs = dict(CUSTOM_GUCS)
        log_rotation_in_seconds = int(
            connector_gucs["systable_subscribed_log_rotation_time_in_seconds"])
        connector_interval_in_seconds = int(
            connector_gucs["monitoring_connector_interval_seconds"])
        timeout_secs = (log_rotation_in_seconds +
                        connector_interval_in_seconds) * 5
        timeout = time.time() + timeout_secs
        while time.time() < timeout:
            ans = self._request_cleanup_for_teardown(cluster)
            if ans:
                log.info("cleanup_for_teardown is completed successfully")
                return True
            log.info("cleanup_for_teardown is in process, retrying...")
        log.info("cleanup_for_teardown is timed out")
        return False

    def get_only_one_burst_cluster(self, cluster):
        """
        Get only one burst cluster object for a main cluster in burst mode 3.
        Args:
            None
        Returns:
            The burst cluster object
        """
        results = cluster.list_acquired_burst_clusters()
        err_msg = "Failed to get burst cluster."
        assert len(results) > 0, err_msg
        burst_cluster_arn = results[0].split(':cluster:')[-1].strip()
        burst_client = RedshiftClient(
            profile=Profiles.QA_BURST_TEST, region=Regions.QA)
        return burst_client.describe_cluster(burst_cluster_arn)

    def _run_burst_queries(self, cluster):
        """
        Run burst queries to trigger stcs logging.
        Args:
            cluster (object): Cluster object where the query executed.
        Returns:
            None

        """
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
        executed_query = num_query_after - num_query_before
        err_msg = (
          "Expect {} of queries bursted, but only see {} from"
          " stl_burst_query_execution.").format(query_count, executed_query)
        assert executed_query == query_count, err_msg
        return query_count

    def _get_stcs_table_names(self, cluster):
        """
        Get list of STCS table name created in database.
        Args:
            cluster (object): Cluster object where the query executed.
        Returns:
            list of stcs table names.

        """
        results = run_bootstrap_sql(cluster, STCS_TABLE_QUERY)
        if len(results) > 0:
            log.info(results)
            return results
        else:
            log.error("Failed to get stcs table names")
            return []

    def _get_col_list(self, cluster, table_name):
        """
        Get the list of columns of a table.
        Args:
            cluster (object): Cluster object where the query executed.
            table_name (string): The interested table name.
        Returns:
            List of stcs table names.

        """
        col_query = ("SELECT column_name, data_type "
                     "FROM information_schema.columns "
                     "WHERE table_schema=\'pg_catalog\' "
                     "AND table_name=\'{}\' "
                     "ORDER BY ordinal_position;").format(table_name)

        col_list = run_bootstrap_sql(cluster, col_query)
        log.debug(col_query)
        log.debug("%s col set: %s" % (table_name, col_list))
        return col_list

    def _get_key_col_value(self, cluster, stcs_name, burst_cluster_id):
        """
        Get the range used to limit the row scope of a given stcs table and
        the key column name.
        Args:
            cluster (object): Cluster object where the query executed.
            stcs_name (string): The interested stcs table name.
            burst_cluster_id (int): Burst cluster id of current cluster.
        Returns:
            Name of key column.

        """
        query = ("select usesysid from pg_user "
                 "where usename=\'{}\';").format(DB_USER)
        return int(run_bootstrap_sql(cluster, query)[0][0])

    def _select_from_stl_table(self, burst_cluster, stl_name,
                               col_list, key_col_name, key_col_value):
        """
        Select content of a list of columns from a stl table in a burst
        cluster given a key column value.
        For time data type, we need to truncate the ms values. In stcs,
        the time stamp values are stored as long values. In order to compare
        the time column in stl with the long value in stcs, we need to convert
        long value in stcs to time. There might be minor differences
        during the conversion process. So truncate the ms part to avoid the
        minor difference.
        Args:
            burst_cluster (object): Cluster object where the select executed.
            stl_name (string): The interested stl table name.
            col_list (list): The interested column list of the stl table.
            key_col_name (string): The key column name used to limit row scope.
            key_col_value (list): The key column range used to limit row scope.
        Returns:
            Result of select query from the stl table.

        """
        err_msg = "Invalid table name:{}".format(stl_name)
        assert stl_name.startswith("stl"), err_msg
        col_query = ""
        order_query = ""
        for col in col_list:
            col_name = col[0]
            data_type = col[1]
            if data_type == "timestamp without time zone":
                # In stl_schema_quota_violations, there is a col called
                # timestamp, so here we want rename the alias name to avoid
                # ambiguation.
                col_query += "date_trunc(\'second\', {}) as {},".format(
                    col_name, col_name + "_sec")
                order_query += col_name + "_sec,"
            else:
                col_query += col_name + ","
                order_query += col_name + ","
        col_query = col_query[:-1]
        order_query = order_query[:-1]

        query = ("select {} from {} "
                 "where {} = {} "
                 "order by {};").format(col_query, stl_name,
                                        key_col_name, key_col_value,
                                        order_query)
        log.info("STL query: {}".format(query))
        results = run_bootstrap_sql(burst_cluster, query)
        return results

    def _select_from_stcs_table(self, cluster, stl_name, stcs_name, col_list,
                                burst_cluster_id, key_col_name, key_col_value):
        """
        Select content of a list of columns from a stcs table in a main
        cluster given a row scope.
        In stcs, the time stamp values are stored as long values. In order
        to compare the time column in stl with the long value in stcs,
        we need to convert long value in stcs to time.
        Args:
            cluster (object): Cluster object where the select executed.
            stl_name (string): The corresponding stl table name of stcs table.
            stcs_name (string): The interested stcs table name.
            col_list (list): The interested column list of the stcs table.
            key_col_name (string): The key column name used to limit row scope.
            key_col_value (list): The key column range used to limit row scope.
        Returns:
            Result of select query from the stcs table.

        """
        err_msg = "Invalid table name:{}".format(stcs_name)
        assert stcs_name.startswith("stcs"), err_msg
        col_query = ""
        order_query = ""
        for col in col_list:
            col_name = col[0]
            data_type = col[1]
            if data_type == "timestamp without time zone":
                col_query += ("(timestamp \'epoch\' + (({}/("
                              "1000*1000))+946684800) * interval \'1 second\')"
                              " as {},").format(col_name, col_name + "_sec")
                order_query += col_name + "_sec,"
            else:
                col_query += col_name + ","
                order_query += col_name + ","
        col_query = col_query[:-1]
        order_query = order_query[:-1]

        query = ("select {} from {} "
                 "where (split_part(__path, \'/\', 10) = {} or "
                 "split_part(split_part(__path, \'/{}/\', 2),\'_\', 1) = {})"
                 " and {} = {} "
                 "and __cluster_type = \'cs\' order by {};").format(col_query,
                                                     stcs_name,
                                                     burst_cluster_id,
                                                     stl_name,
                                                     burst_cluster_id,
                                                     key_col_name,
                                                     key_col_value,
                                                     order_query)
        log.info("STCS query: {}".format(query))
        results = run_bootstrap_sql(cluster, query)
        return results

    def _compare_table_content(self, stl_name, stcs_name, burst_cluster,
                               cluster, burst_cluster_id):
        """
            The test is used to compare the content of stcs tables in Main
            cluster is consistent with corresponding stl tables in Burst
            cluster.
        """
        # 1. get column list of the stl table
        col_list = self._get_col_list(cluster, stl_name)

        # 2. get the range of rows of the data fidelity test
        key_col_value = self._get_key_col_value(cluster, stcs_name,
                                                burst_cluster_id)

        # 3. get results from stl tables in burst cluster
        stl_result = self._select_from_stl_table(
            burst_cluster, stl_name, col_list, USER_ID_COL_NAME, key_col_value)
        log.info("STL query result: {}".format(str(stl_result)))

        # 4. get results from stcs tables in main cluster
        stcs_result = self._select_from_stcs_table(
            cluster, stl_name, stcs_name, col_list, burst_cluster_id,
            USER_ID_COL_NAME, key_col_value)
        log.info("STCS query result: {}".format(str(stcs_result)))

        err_msg = "Query results from {} and {} are inconsistent.".format(
            stl_name, stcs_name)
        if len(stl_result) != len(stcs_result) or stl_result != stcs_result:
            # for debugging purpose, record the failed table content
            with open("/tmp/stl_%s.txt" % (
                    os.getpid(),), "w") as f:
                for item in stl_result:
                    f.write("{}\n".format(item))
            with open("/tmp/stl_%s.txt" % (
                    os.getpid(),), "w") as f:
                for item in stcs_result:
                    f.write("{}\n".format(item))
            assert (len(stl_result) == len(stcs_result)), err_msg
            assert (stl_result == stcs_result), err_msg
        log.info("{} and {} are consistent.".format(stl_name, stcs_name))

    def compare_all_stl_stcs_table_content(self, cluster, burst_cluster):
        stcs_list_result = self._get_stcs_table_names(cluster)
        for stcs_name_list in stcs_list_result:
            # TODO: https://sim.amazon.com/issues/SysTable-1232
            # Convert test_stcs_data_fidelity to a system test for full
            # coverage.
            stcs_name = stcs_name_list[0]

            if stcs_name not in VALIDATION_TABLES:
                continue

            stl_name = stcs_name.replace("stcs", "stl", 1)
            burst_cluster_id = str(burst_cluster.get_padb_conf_value(
                "cluster_id"))
            self._compare_table_content(stl_name, stcs_name, burst_cluster,
                                        cluster, burst_cluster_id)

    def test_stcs_data_fidelity(self, cluster):
        """
            This test ensures the content of stcs tables in Main cluster is
            consistent with corresponding stl tables in Burst
            cluster.

            The test includes the following steps:
            1. create new user to run burst query.
            2. get only one burst cluster for the main cluster.
            3. run several burst queries and check if the burst queries run
            successfully.
            4. select from stl and stcs. And check for the completeness and
            correctness:
                (1). All rows in stl table are also in stcs tables under the
                same userid.
                (2). The rows in stl tables have same content as stcs tables.
            In order to test both, first I need to get the range of rows we
            want to compare, and then compare the content of rows within the
            range.
            The way we find the range is by choosing one key column in a
            stcs table, and select all rows equal to that key column value.
            The following is the key column names:
                (1). For user facing tables, we use userid as the key column.
                (2). For stcs_concurrency_scaling_query_mapping, we use
                primary_query
            And finally compare the two select result.

            In the table content comparision, there is one kind of columns
            differ in stl and stcs tables: time. In stcs, we don't have
            timestamp column. This is because when spectrum request data
            from s3, we cannot keep the timestamp datatype. Instead, we use
            long in stcs. So I need to convert the long data type back to
            timestamp.
        """
        # 1. create new db user
        for query in PREPARE_USER_QUERIES:
            run_bootstrap_sql(cluster, query)

        # 2. get only one burst cluster object.
        burst_cluster = self.get_only_one_burst_cluster(cluster)

        burst_cluster_id = str(burst_cluster.get_padb_conf_value("cluster_id"))
        log.info("burst cluster id: %s" % burst_cluster_id)

        # 3. run a set of burst queries and check the burst queries run
        # successfully.
        query_count = self._run_burst_queries(cluster)
        # Force all stl log be externalized in burst cluster.
        ans = self._wait_cleanup_for_teardown_to_complete(burst_cluster)
        err_msg = "Failed to externalzied all stl logs."
        assert ans, err_msg
        # Check the burst queries have been run successfully.
        query = "select count(*) from " \
                "stcs_concurrency_scaling_query_mapping where " \
                "concurrency_scaling_cluster = {};".format(burst_cluster_id)
        results = int(run_bootstrap_sql(cluster, query)[0][0])
        err_msg = "Failed to burst all test queries."
        assert results == query_count, err_msg

        # 4. compare stl and stcs tables' content
        self.compare_all_stl_stcs_table_content(cluster, burst_cluster)
