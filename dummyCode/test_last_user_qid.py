# Copyright 2023 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import uuid
import pytest
import logging

from raff.monitoring.monitoring_test import MonitoringTestSuite
from raff.common.db.db_exception import DatabaseError, ProgrammingError

log = logging.getLogger(__name__)


@pytest.mark.session_ctx(user_type='bootstrap')
@pytest.mark.localhost_only
@pytest.mark.serial_only
class TestLastUserQueryId(MonitoringTestSuite):
    """
    This test class contains tests that validate the functionality of the
    last_user_querid().
    """
    def clean_up_queries(self, cursor, username, test_table):
        cursor.execute("reset session AUTHORIZATION")
        try:
            cursor.execute("drop user {};".format(username))
            cursor.execute("DROP TABLE {};".format(test_table))
            cursor.execute("drop table #test_1_reg")
        except tuple(DatabaseError + ProgrammingError):
            pass

    def verify_query_id_helper(self, cursor, query_text, db_session):
        test_start_time = self.current_db_time(db_session)
        cursor.execute(query_text)
        cursor.execute("SELECT last_user_query_id();")
        last_user_query_id = cursor.fetch_scalar()
        SYS_QUERY_HISTORY_QUERY_STMT = """
        SELECT query_id FROM sys_query_history
        WHERE query_text LIKE '%{}%'
        AND status = 'success' AND start_time > '{}' AND user_id > 1;
        """
        cursor.execute(SYS_QUERY_HISTORY_QUERY_STMT.format(query_text, test_start_time))
        user_query_id = cursor.fetch_scalar()
        assert last_user_query_id == user_query_id

    def test_get_last_user_query_id(self, cluster_session,
                                    db_session):
        guc1 = {'enable_arcadia': 'true', 'is_arcadia_cluster': 'true'}
        guc2 = {'enable_arcadia': 'false', 'is_arcadia_cluster': 'false',
                'enable_arcadia_system_views_in_provisioned_mode': 'true'}
        gucs = [guc1, guc2]
        for guc in gucs:
            with cluster_session(gucs=guc):
                with db_session.cursor() as cursor:
                    try:
                        username = "reguser_" + self._generate_random_string()
                        # Create test table.
                        test_table = "public.test_table_{}".format(uuid.uuid4().hex)
                        cursor.execute(
                                "create user {} password disable".format(username))
                        utility_query = "SET SESSION AUTHORIZATION {};".format(username)
                        cursor.execute(utility_query)
                        self.verify_query_id_helper(cursor, utility_query, db_session)
                        ddl_query = "CREATE TABLE {} (col1 int);".format(test_table)
                        self.verify_query_id_helper(cursor, ddl_query, db_session)
                        dml_query = "INSERT INTO {} VALUES(1);".format(test_table)
                        self.verify_query_id_helper(cursor, dml_query, db_session)
                        cursor.execute("CREATE TABLE #test_1 (col1 int);")
                        test_query = """
                        INSERT INTO #test_1 SELECT
                        query_id FROM sys_query_history WHERE
                        query_id = last_user_query_id() AND
                        query_text LIKE '%CREATE TABLE%';
                        """
                        cursor.execute(test_query)
                        cursor.execute("SELECT COUNT(*) FROM #test_1")
                        count = cursor.fetchall()[0][0]
                        assert count == 1
                    finally:
                        self.clean_up_queries(cursor, username, test_table)
