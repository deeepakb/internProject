# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
import logging
import pytest

from raff.storage.storage_test import disable_all_autoworkers
from raff.burst.burst_super_simulated_mode_helper import super_simulated_mode
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext

log = logging.getLogger(__name__)
__all__ = [disable_all_autoworkers, super_simulated_mode]

GUCS = {
    'enable_burst': 'true',
    'mv_enable_refresh_to_burst': 'true',
    'selective_dispatch_level': '0',
    'query_group': 'burst',
    'enable_result_cache_for_session': 'false',
    'burst_enable_write': 'true',
    'burst_enable_write_user_ctas': 'false',
    'superblock_elasticity_type': '0',
    'enable_mirror_to_s3': '2',
    'enable_commits_to_dynamo': '2',
    'burst_allow_dirty_reads': 'true',
    'try_burst_first': 'true',
    'xen_guard_enabled': 'true',
    'vacuum_auto_worker_enable': 'false',
    's3commit_enable_delete_tag_throttling': 'false',
    'enable_burst_async_acquire': 'false',
    'burst_enable_user_temp_table': 'false',
    'burst_enable_write_user_ctas': 'false'
}
MY_USER = "test_user"
MY_SCHEMA = "test_schema"
CREATE_TEMP_TABLE = "CREATE TEMP TABLE test_temp (id INT)"
DROP_TABLE = "DROP TABLE IF EXISTS {} CASCADE"
FAIL_BURST_QUERY_1 = "CREATE TABLE test_system_ctas AS SELECT * FROM stl_query"
FAIL_BURST_QUERY_2 = "SELECT * FROM test_temp, stl_query"
CHECK_LAST_QUERY_FAIL_STATUS = "SELECT concurrency_scaling_status "           \
                            "FROM stl_query_concurrency_scaling_fail_reasons "\
                            "WHERE query = {} "                               \
                            "ORDER BY timestamp DESC"


@pytest.mark.serial_only
@pytest.mark.localhost_only
@pytest.mark.custom_burst_gucs(gucs=GUCS)
@pytest.mark.custom_local_gucs(gucs=GUCS)
@pytest.mark.session_ctx(user_type='bootstrap')
@pytest.mark.super_simulated_mode
@pytest.mark.usefixtures("super_simulated_mode")
class TestBurstFailureReasonsLogging():
    """
        This class tests if all failure reasons are correctly logged in
        stl_query_concurrency_scaling_fail_reasons when a query failed
        to burst.
    """

    def _get_last_query_id(self, db_cursor):
        db_cursor.execute("select pg_last_query_id();")
        qid = db_cursor.fetch_scalar()
        return qid

    # A padb_fetch_sample query automatically comes after a CTAS
    # query. So we need to query text matching to retrieve the
    # query id of CTAS.
    def _get_last_ctas_query_id(self, cursor, ctas_query):
        query = ("select query from stl_query"
                 " where querytxt='{};'"
                 " order by 1 desc limit 1").format(ctas_query)
        cursor.execute(query)
        return cursor.fetch_scalar()

    def _check_fail_status(self, query, expected_status_lst, cursor):
        res_status = [status for row in cursor.fetchall() for status in row]
        for status in expected_status_lst:
            assert status in res_status, \
                    "Burst status code {} is expected to be logged as a " \
                    "burst failure reason of query {} but it didn't show " \
                    "on the log.".format(status, query)

    def test_burst_failure_reasons_logging(self, db_session, cluster):
        user_session = DbSession(
            cluster.get_conn_params(),
            session_ctx=SessionContext(username=MY_USER, schema=MY_SCHEMA))

        # cursor to execute queries checking
        # stl_query_concurrency_scaling_fail_reasons
        super_user_cursor = db_session.cursor()
        # cursor to run burst queries that should be
        # failed due to multiple reasons, which are logged in
        # stl_query_concurrency_scaling_fail_reasons
        user_cursor = user_session.cursor()
        user_cursor.execute("set session authorization '{}'".format(MY_USER))
        user_cursor.execute("set enable_result_cache_for_session to off")
        user_cursor.execute("set query_group to burst")

        # The first burst query should fail because of these status codes:
        # 6 - System table accessed
        # 67 - CTAS with burst-write
        user_cursor.execute(DROP_TABLE.format("test_system_ctas"))
        user_cursor.execute(FAIL_BURST_QUERY_1)
        ctas_query_id = \
            self._get_last_ctas_query_id(user_cursor, FAIL_BURST_QUERY_1)
        super_user_cursor.execute(
            CHECK_LAST_QUERY_FAIL_STATUS.format(ctas_query_id))
        self._check_fail_status(FAIL_BURST_QUERY_1, [6, 67], super_user_cursor)

        # The second burst query should fail because of these status codes:
        # 5 - User temporary table accessed
        # 6 - System table accessed
        user_cursor.execute(DROP_TABLE.format("test_temp"))
        user_cursor.execute(CREATE_TEMP_TABLE)
        user_cursor.execute(FAIL_BURST_QUERY_2)
        super_user_cursor.execute(
            CHECK_LAST_QUERY_FAIL_STATUS.format(
                self._get_last_query_id(user_cursor)))
        self._check_fail_status(FAIL_BURST_QUERY_2, [5, 6], super_user_cursor)
