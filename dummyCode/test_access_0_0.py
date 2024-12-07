# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import logging
import os
import pytest
from contextlib import contextmanager
from raff.common.cluster.cluster_session import ClusterSession
from raff.qp.qp_test import QPTest
from raff.qp.qp_test import run_sql_file

log = logging.getLogger(__name__)


@pytest.yield_fixture(scope='class')
def enable_mv(request, cluster):
    session = ClusterSession(cluster)
    gucs = {
        'enable_mv': 'true',
        "vacuum_auto_enable": "false",
        "auto_analyze": "false",
        "vacuum_auto_worker_enable": "false",
        "mv_enable_inflight_txn_handling": "false",
        "mv_enable_v0_1_mv_creation": "false",
        "mv_enable_aqmv": "false"
    }
    with session(gucs):
        yield


@pytest.mark.serial_only
@pytest.mark.usefixtures("enable_mv")
@pytest.mark.skip(reason=("MV with version 0_0 is deprecated"))
class TestMVAccess_0_0(QPTest):
    """
    Tests user access to MV's.
    """
    NAME = "mv"

    @property
    def name(self):
        return TestMVAccess_0_0.NAME

    def generate_sql_res_files(self):
        return True

    @contextmanager
    def create_setup_context(self, db_session, cluster):
        """
        Register validation stored procedures.
        """
        try:
            with db_session.cursor() as cursor:
                path = os.path.join(self.testfiles_dir,
                                    'test_validation_setup.sql')
                run_sql_file(path, "noop", "noop", cursor)
                yield
        finally:
            with db_session.cursor() as cursor:
                path = os.path.join(self.testfiles_dir,
                                    'test_validation_cleanup.sql')
                run_sql_file(path, "noop", "noop", cursor)

    @pytest.mark.session_ctx(user_type='super')
    def test_mv_access_0_0(self, cluster, db_session, cluster_session):
        """
        Tests basic MV access.
        """
        with cluster.event('EtStopRefreshUntilDmlXidLargest'):
            with self.create_setup_context(db_session, cluster):
                self.execute_test_file('test_access_0_0', session=db_session)

    @pytest.mark.localhost_only
    @pytest.mark.session_ctx(user_type='bootstrap')
    def test_mv_bootstrap_access(self, cluster, db_session, cluster_session):
        """
        Tests basic MV access from bootsrap user's perspective.
        """
        with cluster.event('EtStopRefreshUntilDmlXidLargest'):
            with self.create_setup_context(db_session, cluster):
                self.execute_test_file('test_bootstrap_access',
                                       session=db_session)

    @pytest.mark.no_jdbc
    def test_mv_corrupt_view(self, cluster, db_session, cluster_session):
        """
        Tests that if an MV's view definition that references its expected
        internal table is corrupted, access to the MV's view to the internal
        table is blocked.
        """
        with db_session.cursor() as cursor:
            cursor.execute('CREATE TABLE base_corrupt (a INT, b INT)')
            cursor.execute("CREATE MATERIALIZED VIEW mv_corrupt AS SELECT a "
                           "FROM base_corrupt")
        # When MV's are not supported, the MV's view is just a normal view so
        # its definition be replaced.
        mv_off = {
            'enable_mv': 'false',
            'mv_deny_access_internal_table': 'false'
        }
        with cluster_session(gucs=mv_off):
            with db_session.cursor() as cursor:
                cursor.execute("CREATE OR REPLACE VIEW mv_corrupt AS SELECT a "
                               "FROM base_corrupt")
        # When MV's are supported, that view becomes an MV. Check that the MV
        # cannot be accessed: the definition refers to some bogus table so an
        # exception is thrown.
        mv_on = {
            'enable_mv': 'true',
            'mv_deny_access_internal_table': 'false'
        }
        with cluster_session(gucs=mv_on):
            with db_session.cursor() as cursor:
                found_error = False
                try:
                    cursor.execute('SELECT * FROM mv_corrupt')
                except Exception as e:
                    found_error = True
                    log.info("Corrupt view error: {}".format(str(e)))
                    exp_err = "Expected internal table named "
                    "mv_tbl__mv_corrupt__0, found base_corrupt"
                    assert any(exp_err in line for line in str(e).splitlines())
                assert found_error
