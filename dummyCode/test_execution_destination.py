import pytest
import os.path
import subprocess
import time

from raff.common.db.session_context import SessionContext
from raff.common.db.session import DbSession
from raff.monitoring.monitoring_test import MonitoringTestSuite

from raff.burst.burst_test import (BurstTest, setup_teardown_burst,
                                   verify_query_bursted)
from io import open

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst, verify_query_bursted]


@pytest.mark.localhost_only
class TestExecutionDestination(MonitoringTestSuite, BurstTest):
    """ Check whether the 'ety' field which determines whether the query
        was executed on Burst or Main cluster is logged in the monitoring
        log/qinfo/queryid.info file
    """

    @pytest.mark.serial_only
    @pytest.mark.burst_precommit
    def test_main_execution_destination(self, db_session, cluster):
        """ Run a query on Main cluster and check whether the execution destination
            is correctly set to 'Main'
        """
        conn_params = cluster.get_conn_params()
        query_txt_no_split = "select * from stv_inflight"
        ctx = SessionContext(user_type=SessionContext.SUPER)
        self.cleanup_monitoring_directory()
        with DbSession(conn_params, session_ctx=ctx) as session:
            with session.cursor() as cursor:
                cursor.execute(query_txt_no_split)
                assert self.get_info_file_val(
                    'ety', self.get_info_file_path(cursor)) == 'Main'

    @pytest.mark.load_tpcds_data
    @pytest.mark.serial_only
    @pytest.mark.usefixtures("setup_teardown_burst")
    @pytest.mark.burst
    @pytest.mark.burst_precommit
    @pytest.mark.serial_only
    def test_burst_execution_destination(self, db_session,
                                         verify_query_bursted):
        """ Run a query on Burst cluster and check whether the execution destination
            is correctly set to 'Concurrency Scaling'
        """
        self.cleanup_monitoring_directory()
        with db_session.cursor() as cursor:
            self.execute_test_file('query3_tpcds', session=db_session)
            assert self.get_info_file_val(
                'ety',
                self.get_info_file_path(cursor)) == 'Concurrency Scaling'

    def get_info_file_val(self, key, filename):
        '''
        Given the key and the file to look for it, return the value for the key

        Args:
            key: The key to look for
            filename: The name of the file to find the key in.

        Returns:
            The value corresponding to the key.
        '''
        test_file = "pg_val_file.test"
        total = 0
        while total < 300:
            if os.path.isfile(filename):
                break
            time.sleep(1)
            total = total + 1
        cmd = "bzgrep '{}' {} | cut -d':' -f2- > {}".format(
            key, filename, test_file)
        subprocess.check_output(cmd, shell=True)
        with open(test_file) as fp:
            val = fp.readline().strip()
        os.remove(test_file)
        return val

    def get_info_file_path(self, cursor):
        cursor.execute("select pg_last_query_id();")
        qid = cursor.fetch_scalar()
        info_file = self.mon_dir + "/" + str(qid) + ".info.bz2"
        info_file_path = os.path.join(self.mon_dir, info_file)
        return info_file_path
