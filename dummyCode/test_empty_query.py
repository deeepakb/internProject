# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest

from raff.common.db.driver.jdbc_exception import Error
from raff.monitoring.monitoring_test import MonitoringTestSuite

log = logging.getLogger(__name__)


class TestEmptyQuery(MonitoringTestSuite):

    '''
    This test verifies that empty query will be executed successfully
    via JDBC driver without crashing PADB, but the query result is empty.
    '''
    @pytest.mark.localhost_only
    @pytest.mark.no_psycopg
    @pytest.mark.serial_only
    def test_empty_query_for_jdbc(self, cluster_session, db_session):
        empty_queries = ["", " ", "--nothing"]
        expected_error_msg = "no results to fetch"
        guc_configs = [
            # serverless
            {'is_arcadia_cluster': 'true', 'enable_arcadia': 'true'},
            # non serverless
            {'is_arcadia_cluster': 'false', 'enable_arcadia': 'false'}
        ]
        for guc in guc_configs:
            with cluster_session(gucs=guc):
                with db_session.cursor() as cursor:
                    for query in empty_queries:
                        actual_error_msg = ""
                        try:
                            cursor.execute(query)
                            cursor.fetchall()
                        except Error as e:
                            actual_error_msg = str(e)
                        assert expected_error_msg == actual_error_msg
