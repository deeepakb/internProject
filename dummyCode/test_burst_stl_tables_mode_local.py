# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import logging
import pytest
import uuid
import datetime

from contextlib import contextmanager
from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst
from raff.common.base_test import FailedTestException

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.

__all__ = [setup_teardown_burst]

log = logging.getLogger(__name__)


@contextmanager
def set_event(cluster, event):
    cluster.set_event(event)
    yield
    cluster.unset_event(event)


def get_user_id(cursor, username):
    cursor.execute("select usesysid from pg_user "
                   "where usename = '{}'".format(username))
    userid = cursor.fetch_scalar()
    return userid


def get_query_id(session):
    with session.cursor() as cursor:
        cursor.execute("select pg_last_query_id();")
        query_id = cursor.fetch_scalar()
        return query_id


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstManagerSTLTablesLocal(BurstTest):

    @contextmanager
    def auto_release_local_burst(self, cluster):
        yield
        self.release_localmode_burst(cluster)

    def test_burst_query_execution_query_error(self, cluster, db_session):
        '''
        This tests simulates an error on the rest agent during a query
        execution and ensures is logged correctly on the burst query
        exectuion table.
        '''
        with set_event(cluster, 'EtSimulateRestAgentQueryError'), \
                self.auto_release_local_burst(cluster), \
                self.db.cursor() as cursor:
            try:
                self.execute_test_file('burst_query', session=db_session)
            except FailedTestException:
                pass
            cursor.execute("SELECT * FROM stl_burst_query_execution where "
                           "error like '%EtSimulateRestAgentQueryError%' ")
            if cursor.rowcount == 0:
                pytest.fail("Failed test with {}".format(
                    "EtSimulateRestAgentQueryError"))

    def test_burst_manager_pers_backup_name(self, cluster, db_session):
        '''
        This test takes 2 backups (first with backup_name1 and then with
        backup_name2), runs a burst query each time and then verifies that
        the backup name is correctly logged in the personalization table.
        The test uses a regular session to burst queries and a bootstrap
        session for visibility on the personalization table.
        '''
        self.release_localmode_burst(cluster)  # to be safe, release first
        with self.db.cursor() as cursor:
            with self.auto_release_local_burst(cluster):
                backup_name = "test_burst_manager_backup_name1" + str(
                    uuid.uuid4()).replace('-', '')
                cluster.backup_cluster(backup_name)
                self.execute_test_file('burst_query', session=db_session)
                cursor.execute(
                    "SELECT * FROM stl_burst_manager_personalization where "
                    "backup_id like '{}%'".format(backup_name))
                if cursor.rowcount == 0:
                    pytest.fail("Failed test with {}".format(backup_name))

            with self.auto_release_local_burst(cluster):
                backup_name = "test_burst_manager_backup_name2" + str(
                    uuid.uuid4()).replace('-', '')
                cluster.backup_cluster(backup_name)
                self.execute_test_file('burst_query', session=db_session)
                cursor.execute(
                    "SELECT * FROM stl_burst_manager_personalization where "
                    "backup_id like '{}%'".format(backup_name))
                if cursor.rowcount == 0:
                    pytest.fail("Failed test with {}".format(backup_name))

    def test_burst_manager_pers_error(self, cluster, db_session):
        '''
        This test sets an event on the burst side to fail the personalization
        and verifies that the error is correctly logged in the personalization
        table.
        The test uses a regular session to burst queries and a bootstrap
        session for visibility on the personalization table.
        '''
        with self.auto_release_local_burst(cluster), \
                self.db.cursor() as cursor:
            with set_event(cluster, 'EtSimulateRestAgentPersonalizationError'):
                self.execute_test_file('burst_query', session=db_session)
                cursor.execute(
                    "SELECT * FROM stl_burst_manager_personalization where "
                    "error like '%EtSimulateRestAgentPersonalizationError%' ")
                if cursor.rowcount == 0:
                    pytest.fail("Failed test with {}".format(
                        "EtSimulateRestAgentPersonalizationError"))

    def test_burst_manager_cluster_info(self, cluster, db_session):
        '''
        This test burst a query and verify the content of the burst clusters
        table is correct.
        The test uses a regular session to burst queries and a bootstrap
        session for visibility on the clusters table.
        '''
        start_time = datetime.datetime.now().replace(microsecond=0)
        with self.auto_release_local_burst(cluster), \
                self.db.cursor() as cursor:
            self.execute_test_file('burst_query', session=db_session)
            cursor.execute("SELECT * FROM stv_burst_manager_cluster_info")
            results = cursor.result_fetchall()
            if cursor.rowcount == 0:
                pytest.fail(
                    "No rows in {}".format("stv_burst_manager_cluster_info"))

            row = results.rows[0]
            num_sessions = row[1]
            max_sessions = row[2]
            last_used = row[6]
            assert num_sessions == 0
            assert max_sessions == 5
            assert last_used >= start_time

    def test_burst_prepare(self, cluster, db_session):
        '''
        This test burst a query and verify the content of the prepare
        table is correct.
        The test uses a regular session to burst queries and a bootstrap
        session for visibility on the clusters table.
        '''
        test_start_time = datetime.datetime.now().replace(microsecond=0)
        with self.auto_release_local_burst(cluster), \
                self.db.cursor() as cursor:
            self.execute_test_file('burst_query', session=db_session)
            query_id = get_query_id(db_session)
            cursor.execute(
                "SELECT * FROM stl_burst_prepare order by starttime desc")
            results = cursor.result_fetchall()
            if cursor.rowcount == 0:
                pytest.fail("No rows in {}".format("stl_burst_prepare"))

            row = results.rows[0]
            userid = row[0]
            starttime = row[3]
            endtime = row[4]
            queryid = row[6]
            assert userid == get_user_id(cursor,
                                         db_session.session_ctx.username)
            assert queryid == query_id
            assert starttime > test_start_time
            assert endtime > test_start_time

    def test_burst_connection(self, cluster, db_session):
        '''
        This test burst a query and verify the content of the connection
        table is correct.
        The test uses a regular session to burst queries and a bootstrap
        session for visibility on the clusters table.
        '''
        test_start_time = datetime.datetime.now().replace(microsecond=0)
        with self.auto_release_local_burst(cluster), \
                self.db.cursor() as cursor:
            self.execute_test_file('burst_query', session=db_session)
            cursor.execute(
                "SELECT * FROM stl_burst_connection order by eventtime desc")
            results = cursor.result_fetchall()
            if cursor.rowcount == 0:
                pytest.fail("No rows in {}".format("stl_burst_connection"))

            # The first row is the record for closing the connection
            close_row = results.rows[0]
            action = close_row[0].strip()
            method = close_row[1].strip()
            eventtime = close_row[5]
            error = close_row[8].strip()
            assert action == 'Shutdown'
            assert method == 'Prepare'
            assert eventtime > test_start_time
            assert error == ''

            # The second row is the record for opening the connection
            open_row = results.rows[1]
            action = open_row[0].strip()
            method = open_row[1].strip()
            eventtime = open_row[5]
            error = open_row[8].strip()
            assert action == 'Open'
            assert method == 'Prepare'
            assert eventtime > test_start_time
            assert error == ''
