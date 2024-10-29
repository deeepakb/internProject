# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest
import logging

from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.monitoring.monitoring_test import MonitoringTestSuite

log = logging.getLogger(__name__)

cluster_mode = ['saz_provisioned', 'serverless', 'maz_provisioned']

legacy_view_map = {
    'stl_query':
    'SELECT stll_query.userid, stll_query.query, stll_query."label", '
    'stll_query.xid, stll_query.pid, stll_query."database", '
    'stll_query.querytxt, stll_query.starttime, stll_query.endtime, '
    'stll_query.aborted, stll_query.insert_pristine, '
    'stll_query.concurrency_scaling_status FROM stll_query WHERE '
    '(((stll_query.access_control = 0) OR (stll_query.access_control IS NULL))'
    ' OR (NOT is_seclog_downgrade_enabled()));',
    'svl_s3log':
    'SELECT stl_s3log.pid, stl_s3log.query, stl_s3log.segment, '
    'stl_s3log.step, stl_s3log.node, stl_s3log.slice, '
    'stl_s3log.eventtime, btrim((stl_s3log.message)::text) AS message '
    'FROM stl_s3log WHERE (stl_s3log.is_internal = 0);',
    'svv_query_inflight':
    'SELECT b.userid, a.slice, a.query, a.pid, a.starttime, a.suspended, '
    'b.text, b."sequence" FROM stv_inflight a, stl_querytext b '
    'WHERE (a.query = b.query);',
    'svcs_s3query':
    "SELECT stcs.userid, \"map\".primary_query AS query, stcs.segment, "
    "stcs.step, stcs.node, stcs.slice, "
    "('1970-01-01 00:00:00'::timestamp without time zone + "
    "(((((stcs.starttime)::numeric / (1000.0 * 1000.0)) + "
    "946684800.0))::double precision * '00:00:01'::interval)) AS "
    "starttime, ('1970-01-01 00:00:00'::timestamp without time zone + "
    "(((((stcs.endtime)::numeric / (1000.0 * 1000.0)) + "
    "946684800.0))::double precision * '00:00:01'::interval)) AS endtime, "
    "(stcs.endtime - stcs.starttime) AS elapsed, "
    "btrim((stcs.external_table_name)::text) AS external_table_name, "
    "stcs.file_format, stcs.is_partitioned, stcs.is_rrscan, "
    "stcs.is_nested, stcs.s3_scanned_rows, stcs.s3_scanned_bytes, "
    "stcs.s3query_returned_rows, stcs.s3query_returned_bytes, stcs.files, "
    "stcs.splits, stcs.total_split_size, stcs.max_split_size, "
    "stcs.total_retries, stcs.max_retries, stcs.max_request_duration, "
    "stcs.avg_request_duration, stcs.max_request_parallelism, "
    "stcs.avg_request_parallelism, stcs.slowdown_count, "
    "stcs.max_concurrent_slowdown_count, stcs.scan_type FROM "
    "(stcs_s3query stcs JOIN stcs_concurrency_scaling_query_mapping "
    "\"map\" ON ((\"map\".concurrency_scaling_query = stcs.query))) WHERE "
    "((((((stcs.__cluster_type = 'cs'::bpchar) AND "
    "(to_date((stcs.__log_generated_date)::text, 'YYYYMMDD'::text) > "
    "(getdate() - '7 days'::interval))) AND "
    "(to_date((\"map\".__log_generated_date)::text, 'YYYYMMDD'::text) > "
    "(getdate() - '7 days'::interval))) AND "
    "((\"map\".concurrency_scaling_cluster)::text = "
    "split_part((stcs.__path)::text, '/'::text, 10))) AND "
    "(stcs.is_copy = 0)) AND (((EXISTS (SELECT 1 FROM pg_user WHERE "
    "((pg_user.usename = (\"current_user\"())::name) AND "
    "(pg_user.usesuper = true)))) OR (EXISTS (SELECT 1 FROM "
    "pg_shadow_extended WHERE (((pg_shadow_extended.\"sysid\" = "
    "\"current_user_id\"()) AND (pg_shadow_extended.colnum = 2)) AND "
    "(pg_shadow_extended.value = (-1)::text))))) OR (stcs.userid = "
    "\"current_user_id\"())));",
}

new_view_map = {
    'sys_load_history':
    'SELECT slhb.user_id, slhb.query_id, '
    '"max"(slhb.xid) AS transaction_id, "max"(slhb.pid) AS session_id, '
    '"max"((slhb.db)::text) AS database_name, CASE '
    'WHEN ("max"(slhb.status) = 3) THEN \'aborted\'::text '
    'WHEN ("max"(slhb.status) = 4) THEN \'completed\'::text '
    'WHEN (("max"(slhb.status) = 2) AND ("max"(suqs.query_id) IS NOT NULL)) '
    'THEN \'running\'::text '
    'WHEN (("max"(slhb.status) = 2) AND ("max"(suqs.query_id) IS NULL)) '
    'THEN \'aborted\'::text ELSE \'unknown\'::text END AS status, '
    '"max"((slhb.table_name)::text) AS table_name, '
    'min(slhb.event_time) AS start_time, "max"(slhb.event_time) AS end_time, '
    'date_diff(\'microseconds\'::text, min(slhb.event_time), '
    '"max"(slhb.event_time)) AS duration, '
    '"max"((slhb.data_source)::text) AS data_source, '
    '"max"((slhb.file_format)::text) AS file_format, '
    'sum(slhb.rows_copied) AS loaded_rows, '
    'CASE WHEN (("max"(slhb.status) <> 4) OR '
    '(sum(slhb.rows_copied) = 0)) THEN (0)::bigint '
    'ELSE sum(slhb.bytes_copied) END AS loaded_bytes, '
    'CASE WHEN ((count(DISTINCT slhb.filename) - 1) > 0) '
    'THEN (count(DISTINCT slhb.filename) - 1) '
    'ELSE (0)::bigint END AS source_file_count, '
    'sum(slhb.file_bytes) AS source_file_bytes, '
    'sum(slhb.file_count_scanned) AS file_count_scanned, '
    'sum(slhb.file_bytes_scanned) AS file_bytes_scanned, '
    'sum(slhb.error_count) AS error_count, '
    'slhb.copy_job_id '
    'FROM (stl_load_history_base slhb '
    'LEFT JOIN stv_user_query_state suqs ON '
    '((slhb.query_id = suqs.query_id))) '
    'WHERE (slhb.user_id > 1) '
    'GROUP BY slhb.user_id, slhb.query_id, slhb.copy_job_id;'
}

disable_message = 'Error: describing system view <{}> is disabled.'
validation_query_fmt = "select pg_get_viewdef('{view_name}')"
serverless_stl_views = [
    'sys_load_history', 'sys_query_history', 'sys_query_detail',
    'sys_unload_history', 'sys_serverless_usage', 'sys_external_query_detail',
    'sys_load_error_detail']


@pytest.mark.serial_only  # Due to guc changes
@pytest.mark.localhost_only  # Due to bootstrapuser
class TestDisableSystemViewDescribeInServerless(MonitoringTestSuite):
    """
    This test validates the following scenarios:
        - In provisioned mode, all types of db users can describe system views
        - In serverless mode, bootstrap user can describe system views. But
          regular and super users will not be able to describe system views.
    """

    """
    Run describe system view and check result
    """
    def run_system_view_describe(
            self, guc, cluster, cluster_session, view_map, cluster_mode):
        with cluster_session(gucs=guc):
            conn_params = cluster.get_conn_params()
            for user_type in SessionContext.user_types:
                if user_type == SessionContext.MASTER:
                    # master user session does not work with localhost db
                    continue
                with DbSession(
                    conn_params, session_ctx=SessionContext(user_type)) \
                        as db_session:
                    with db_session.cursor() as cursor:
                        self.validate_result(
                            cursor, cluster_mode, user_type, guc, view_map)

    """
    Validate test result
    """
    def validate_result(self, cursor, cluster_mode, user_type, guc, view_map):
        for view in view_map.keys():
            query = validation_query_fmt.format(view_name=view)
            log.info('Executing query: {}'.format(query))
            if user_type == SessionContext.BOOTSTRAP:
                expected = view_map[view]
            elif cluster_mode == 'saz_provisioned' and \
                    view in legacy_view_map:
                expected = view_map[view]
            elif (cluster_mode == 'serverless' or
                  cluster_mode == 'maz_provisioned') and \
                    view in new_view_map:
                expected = disable_message.format(view)
            else:
                expected = disable_message.format(view)
            cursor.execute(query)
            result = cursor.fetchall()
            print(result[0][0])
            print(expected)
            assert expected == result[0][0], \
                'guc:{}, user:{} query:{} expected:{} actual:{}'.format(
                    guc, user_type, query, expected, result[0][0])

    @pytest.mark.session_ctx(user_type=SessionContext.SUPER)
    def test_describe_serverless_stl_views_should_fail(self, cluster_session,
                                                       db_session):
        '''
        Verify that describe serverless STL view will fail in provisioned
        cluster even we allow customer query serverless STL views.
        '''
        guc = {'is_arcadia_cluster': 'false',
               'enable_arcadia': 'false',
               'enable_arcadia_system_views_in_provisioned_mode': 'true',
               'serverless_only_stl_views': 'sys_serverless_usage'}
        # STL name should be case insensitive.
        test_views = serverless_stl_views + \
            [x.upper() for x in serverless_stl_views]
        with cluster_session(gucs=guc):
            for view in test_views:
                query = validation_query_fmt.format(view_name=view)
                with db_session.cursor() as cursor:
                    cursor.execute(query)
                    result = cursor.fetchall()
                    expected_msg = disable_message.format(view.lower())
                    assert expected_msg == result[0][0], 'Describe any ' \
                        'serverless view should be access denied.'

    """
    Legacy system view describe should be disabled for regular and super users
    in MAZ mode. Bootstrap user can describe system views.
    """

    @pytest.mark.precommit
    def test_disable_legacy_sysview_describe_in_maz_mode(
            self, cluster, cluster_session):
        guc = {
            'multi_az_enabled': 'True',
            'is_multi_az_primary': 'True',
            'legacy_stl_disablement_mode': 1
        }
        self.run_system_view_describe(guc, cluster, cluster_session,
                                      legacy_view_map, 'maz_provisioned')

    """
    New system view describe should be disabled for regular and super users
    in MAZ mode to protect sys_ view internal details.
    """

    @pytest.mark.precommit
    def test_disable_new_sysview_describe_in_maz_mode(self, cluster,
                                                      cluster_session):
        guc = {
            'multi_az_enabled': 'True',
            'is_multi_az_primary': 'True',
            'legacy_stl_disablement_mode': 1
        }
        self.run_system_view_describe(guc, cluster, cluster_session,
                                      new_view_map, 'maz_provisioned')

    """
    System view describe should be disabled for regular and super users
    in the serverless mode. Bootstrap user can describe system views.
    """
    @pytest.mark.precommit
    def test_disable_legacy_sysview_describe_in_serverless_mode(
            self, cluster, cluster_session):
        guc = {'is_arcadia_cluster': 'true',
               'enable_arcadia': 'true'}
        self.run_system_view_describe(guc, cluster, cluster_session,
                                      legacy_view_map, 'serverless')

    """
    New system view describe should be disabled for regular and super users
    in Arcadia mode to protect sys_ view internal details.
    """

    @pytest.mark.precommit
    def test_disable_new_sysview_describe_in_serverless_mode(
            self, cluster, cluster_session):
        guc = {
            'multi_az_enabled': 'True',
            'is_multi_az_primary': 'True',
            'legacy_stl_disablement_mode': 1
        }
        self.run_system_view_describe(guc, cluster, cluster_session,
                                      new_view_map, 'serverless')

    """
    System view describe should be enabled for all types of db users
    in the provisioned mode.
    """
    @pytest.mark.precommit
    def test_enable_legacy_sysview_describe_in_provisioned_mode(
            self, cluster, cluster_session, db_session):
        guc = {
            'is_arcadia_cluster': 'false',
            'enable_arcadia': 'false',
            'multi_az_enabled': 'false'
        }
        self.run_system_view_describe(guc, cluster, cluster_session,
                                      legacy_view_map, 'saz_provisioned')

    """
    New system view describe should be disabled for non-bootstrap users
    in the provisioned mode.
    """

    @pytest.mark.precommit
    def test_disable_new_sysview_describe_in_provisioned_mode(
            self, cluster, cluster_session, db_session):
        guc = {
            'is_arcadia_cluster': 'false',
            'enable_arcadia': 'false',
            'multi_az_enabled': 'false'
        }
        self.run_system_view_describe(guc, cluster, cluster_session,
                                      new_view_map, 'saz_provisioned')

    """
    Describe none existed view should return error.
    """
    def test_return_error_message_when_describe_not_existing_view(
            self, cluster, cluster_session, db_session):
        """
        Describe view with oid 99999999, the view should not exist, then we
        verify the return value should be 'Not a view'.
        """
        query_describe_non_existing_view = 'SELECT pg_get_viewdef(99999999);'
        with db_session.cursor() as cursor:
            cursor.execute(query_describe_non_existing_view)
            result = cursor.fetchall()
            assert result[0][0] == 'Not a view'
