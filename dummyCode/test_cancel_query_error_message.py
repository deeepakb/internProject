# Copyright 2021 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import logging
import pytest
import threading
import time

from psycopg2.extensions import QueryCanceledError
from raff.common.db.db_exception import ProgrammingError
from raff.common.db.driver.jdbc_exception import Error
from raff.common.db.session import DbSession
from raff.common.db.session_context import SessionContext
from raff.monitoring.monitoring_test import MonitoringTestSuite

log = logging.getLogger(__name__)


@pytest.mark.localhost_only
@pytest.mark.serial_only
class TestErrorMessageOfCanceledQueryInSysQueryHistory(MonitoringTestSuite):
    """
    Test that sys_query_history recorded the error message of canceled query.
    """

    # Long run query.
    long_query_query = (
        "WITH BIG as (SELECT * FROM PG_ATTRIBUTE"
        " LIMIT 100), SMALL as (SELECT * FROM PG_ATTRIBUTE LIMIT 4)"
        " SELECT COUNT(*) FROM BIG as B1, BIG as B2, BIG as B3, BIG as"
        " B4, BIG B5, BIG B6, SMALL as S1;")

    @pytest.mark.session_ctx(user_type=SessionContext.REGULAR)
    def test_sys_query_history_should_record_self_canceled_error_message(
            self, cluster, cluster_session, db_session):
        """
        This test verifies that sys_query_history should record the query
        cancel message in column 'error_mesage' when the query is cancel by
        the user itself. This test simuluates self cancel query by setting
        query timeout to make the query being canceled by itself when query
        reaches timeout.
        """
        guc = {'is_arcadia_cluster': 'true', 'enable_arcadia': 'true'}

        with cluster_session(gucs=guc):
            with db_session.cursor() as cursor:
                cursor.execute("select now();")
                test_start_time = cursor.fetch_scalar()
                query_canceled = False
                try:
                    # Set query timeout 1 second.
                    cursor.execute('SET statement_timeout TO 1000;')
                    cursor.execute(self.long_query_query)
                except (QueryCanceledError, Error) as e:
                    query_canceled = True
                    log.info("error:{}".format(e))
                assert query_canceled

                # Clean timeout.
                cursor.execute('SET statement_timeout TO 0;')
                # Query error message.
                query_error_message_from_sys_query_history = '''
                select error_message from sys_query_history
                where query_text = '{}' and start_time >= '{}'
                '''.format(self.long_query_query, test_start_time)
                cursor.execute(query_error_message_from_sys_query_history)
                error_message = cursor.fetchall()[0][0]
                log.info('errormessage:{}'.format(error_message))

                assert error_message.find('cancel') != -1

    def run_query_async(self, db_session, cluster, query_text):
        thread = threading.Thread(
            target=self.run_query, args=(db_session, cluster, query_text))
        thread.start()
        return thread

    def cancel_query_and_verify_error_message(
            self, cluster, cluster_session, db_session, cancel_cmd):
        """
        This test verifies that sys_query_history should record the query
        cancel message when the query is cancelled by another user. This
        test simulates cancel other user's query by: 1. Regular user issues
        a long run query. 2. Super user cancel the regular user issued query.
        3. Verify the error message in sys_query_history.
        """
        guc = {'is_arcadia_cluster': 'true', 'enable_arcadia': 'true'}

        with cluster_session(gucs=guc):
            try:
                test_start_time = self.current_db_time(db_session)
                query_pid = 'select pg_backend_pid();'
                pid = self.run_query_first_result(db_session, cluster,
                                                  query_pid)
                log.info("pid:{}".format(pid))

                # Regular user issued the long run query.
                thread = self.run_query_async(db_session, cluster,
                                              self.long_query_query)

                # Create session for super user.
                super_session = \
                    SessionContext(user_type=SessionContext.SUPER)
                super_db_session = DbSession(
                    cluster.get_conn_params(), session_ctx=super_session)

                # Make sure the long query is running.
                running_query_cnt = 0
                for retry in range(0, 3, 1):
                    retrieve_long_query = (
                        "select count(*) from sys_query_history"
                        " where status not in ('success', 'failed')"
                        " and query_text='{}' and start_time > '{}'".format(
                            self.long_query_query, test_start_time))
                    running_query_cnt = self.run_query_first_result(
                        super_db_session, cluster, retrieve_long_query)
                    if running_query_cnt > 0:
                        break
                    time.sleep(10)
                assert running_query_cnt == 1

                # Super user cancels the regular user's running query.
                cancel_query = cancel_cmd.format(pid)
                self.run_query_first_result(super_db_session, cluster,
                                            cancel_query)

                retrieve_current_user = ('select usesysid from pg_user where'
                                         ' usename in (select user);')
                super_user_id = self.run_query_first_result(
                    super_db_session, cluster, retrieve_current_user)
                # Check error message in sys_query_history.
                query_error_message = (
                    "select error_message from sys_query_history where"
                    " session_id = {} and start_time > '{}' order by "
                    " start_time desc limit 1;".format(pid, test_start_time))
                error_message = self.run_query_first_result(
                    super_db_session, cluster, query_error_message)
                log.info("error: {}".format(error_message))
                assert ('Query canceled by user_id: {} with calling '
                        'pg_terminate_backend({}).').format(
                                super_user_id, pid) == error_message
            finally:
                # 60 seconds.
                thread.join(timeout=60.0)

    @pytest.mark.session_ctx(user_type=SessionContext.REGULAR)
    @pytest.mark.no_jdbc  # jdbc doesn't support python multi threads.
    def test_sys_query_history_should_record_non_self_canceled_error_message(
            self, cluster, cluster_session, db_session):
        """
        Test that sys_query_history will record the error message when user
        calls pg_terminate_backend() or pg_cancel_backend() to terminate
        running query.
        """
        cancel_cmds = ['select pg_terminate_backend({});',
                       'select pg_cancel_backend({});']
        for cmd in cancel_cmds:
            self.cancel_query_and_verify_error_message(
                    cluster, cluster_session, db_session, cmd)

    @pytest.mark.session_ctx(user_type=SessionContext.SUPER)
    def test_super_user_has_no_access_to_stv_user_query_state(
            self, cluster, db_session):
        """
        Test that super user cannot access stv_user_query_state.
        """
        with db_session.cursor() as cursor:
            permission_error = False
            try:
                cursor.execute('select * from stv_user_query_state')
            except ProgrammingError as e:
                assert "permission denied for relation stv_user_query_state" \
                        in str(e)
                permission_error = True
            assert permission_error
