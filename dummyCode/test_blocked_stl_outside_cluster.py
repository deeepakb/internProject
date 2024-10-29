# Copyright 2022 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
import uuid

from raff.common.db.session import DbSession, SessionContext
from raff.monitoring.monitoring_test import MonitoringTestSuite

log = logging.getLogger(__name__)


@pytest.mark.serial_only  # Serial because it changes guc values.
@pytest.mark.localhost_only
class TestDisableQueryStlFromS3(MonitoringTestSuite):
    """
    Test even we enabled querying STL data from S3, we still able to do:
    1. Disable bootstrap user retrieve historical STL data from S3.
    2. Disable specific STLs retrieve historical data from S3.
    """

    def run_query_with_gucs(self, cluster, cluster_session, gucs, queries,
                            user_type):
        res = []
        with cluster_session(
                gucs=gucs, clean_db_before=False, clean_db_after=False):
            ctx = SessionContext(user_type)
            conn_params = cluster.get_conn_params()
            with DbSession(conn_params, session_ctx=ctx) as db_session:
                with db_session.cursor() as cursor:
                    for query in queries:
                        cursor.execute(query)
                        res.append(int(cursor.fetch_scalar()))
        return res

    def test_disable_query_stl_data_from_s3(self, cluster, cluster_session,
                                            db_session):
        gucs = {
            'enable_query_sys_table_log_from_S3': 'false',
            'enable_bootstrap_user_query_stl_from_s3': 'true',
            'query_from_s3_blocked_stls': '',
            'enable_stl_immutable_cluster_id_partition': 'true',
            's3_stl_bucket': 'padb-monitoring-test',
            'enable_arcadia': 'false',
            'immutable_cluster_id': str(uuid.uuid4()),
            'query_ln_stl_log_from_s3_buffer_secs': 0
        }
        stls = ['stl_query', 'stl_scan', 'stl_aggr']
        query_id = 0
        for stl in stls:
            self.cleanup_log_files_by_type(stl)
        with cluster_session(
                gucs=gucs, clean_db_before=False, clean_db_after=False):
            sample_query = ("/*{}*/ select count(*) from stl_query;").format(
                uuid.uuid4())
            with db_session.cursor() as cursor:
                cursor.execute(sample_query)
                cursor.execute('select pg_last_query_id()')
                query_id = cursor.fetch_scalar()

            # Upload local STL to S3 and clean local stl_query, stl_scan.
            self.wait_cleanup_for_teardown_to_complete(
                cluster, timeout_secs=180)

        query_template = \
            ("select count(*) from {} where query = {}")
        queries = \
            [query_template.format(stl, query_id) for stl in stls]

        original_stl_cnts = self.run_query_with_gucs(
            cluster, cluster_session, gucs, queries,
            SessionContext.SUPER)
        assert 1 == original_stl_cnts[0], 'stl_query count should be 1'
        assert 0 < original_stl_cnts[1], 'stl_scan count should > 0'
        assert 0 < original_stl_cnts[2], 'stl_aggr count should > 0'

        for stl in stls:
            self.cleanup_log_files_by_type(stl)
        # Reboot PADB to release the deleted STL file handler.
        cluster.reboot_cluster()

        # Verify PADB should be able to query STL from S3.
        log.info('immutable_cluster_id: {}'.format(
            gucs['immutable_cluster_id']))
        gucs['query_from_s3_blocked_stls'] = ''
        gucs['enable_query_sys_table_log_from_S3'] = 'true'
        gucs['enable_bootstrap_user_query_stl_from_s3'] = 'true'
        stl_cnts = self.run_query_with_gucs(cluster, cluster_session,
                                            gucs, queries,
                                            SessionContext.SUPER)
        assert original_stl_cnts == stl_cnts, 'stl count not match'

        # Verify PADB won't return stl from S3 when we blocked
        # stl_query and stl_scan retriving historical data from S3.
        # stl_aggr will return data from s3.
        gucs['query_from_s3_blocked_stls'] = 'stll_query,stll_scan'
        gucs['enable_query_sys_table_log_from_S3'] = 'true'
        gucs['enable_bootstrap_user_query_stl_from_s3'] = 'true'
        stl_cnts = self.run_query_with_gucs(cluster, cluster_session,
                                            gucs, queries,
                                            SessionContext.SUPER)
        assert [0, 0, original_stl_cnts[2]] == stl_cnts, \
            'stl count not match'

        # Verify PADB won't return stl from S3 when we blocked
        # bootstrap user retriving historical data from S3.
        gucs['query_from_s3_blocked_stls'] = ''
        gucs['enable_query_sys_table_log_from_S3'] = 'true'
        gucs['enable_bootstrap_user_query_stl_from_s3'] = 'false'
        stl_cnts = self.run_query_with_gucs(cluster, cluster_session,
                                            gucs, queries,
                                            SessionContext.BOOTSTRAP)
        assert [0, 0, 0] == stl_cnts, 'stl count not match'

        # Verify PADB won't return stl from S3 when we blocked
        # stl_query and stl_scan, and bootstrap user retriving
        # historical data from S3.
        gucs['query_from_s3_blocked_stls'] = 'stll_query,stll_scan'
        gucs['enable_query_sys_table_log_from_S3'] = 'true'
        gucs['enable_bootstrap_user_query_stl_from_s3'] = 'false'
        stl_cnts = self.run_query_with_gucs(cluster, cluster_session,
                                            gucs, queries,
                                            SessionContext.BOOTSTRAP)
        assert [0, 0, 0] == stl_cnts, 'stl count not match'

        # Verify superuser is able to retrieve STL from S3, even we
        # blocked bootstrap user retrieve STL from S3.
        gucs['query_from_s3_blocked_stls'] = ''
        gucs['enable_query_sys_table_log_from_S3'] = 'true'
        gucs['enable_bootstrap_user_query_stl_from_s3'] = 'false'
        stl_cnts = self.run_query_with_gucs(cluster, cluster_session,
                                            gucs, queries,
                                            SessionContext.SUPER)
        assert original_stl_cnts == stl_cnts, 'stl count not match'
