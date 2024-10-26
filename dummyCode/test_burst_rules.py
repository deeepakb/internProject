# Copyright18 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import uuid

import pytest
import random

from contextlib import contextmanager
from time import sleep
from raff.common.result import SelectResult
from raff.burst.burst_test import (BurstTest, setup_teardown_burst)
from raff.common.base_test import FailedTestException
from raff.common.db.session import DbSession
from raff.common.base_test import run_priviledged_query_scalar_int

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst"]

CUSTOM_SD_GUCS = {'selective_dispatch_level': '3', 'enable_burst': 'true'}
CUSTOM_AUTO_GUCS = {
    "try_burst_first": "false",
    "enable_burst_spectrum": "false",
    "enable_vacuum_transparent_recluster_algorithm": "false",
    # TODO(Redshift-56873): Remove and update expected test output when DSW is
    # enabled by default
    "data_sharing_writes_enabled": "false"

}
CUSTOM_NESTED_SPECTRUM_GUCS = {"enable_burst_spectrum": "true"}

CHECK_BURST_STATUS_SQL = """
select concurrency_scaling_status from svl_query_concurrency_scaling_status
where query = {}
"""
CHECK_BURST_STATUS_TXT_SQL = """
select concurrency_scaling_status_txt from svl_query_concurrency_scaling_status
where query = {}
"""


@contextmanager
def set_event(cluster, event):
    cluster.set_event(event)
    yield
    cluster.unset_event(event)


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_SD_GUCS)
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstRules(BurstTest):
    def generate_sql_res_files(self):
        return True

    def test_burst_rules_log(self, db_session):
        with self.db.cursor() as cursor:
            cursor.execute("grant usage on language plpythonu to PUBLIC;")
        self.execute_test_file('burst_rules', session=db_session)

    def test_burst_config(self, db_session):
        self.execute_test_file('burst_config', session=self.db)

    def test_bootstrap_burst(self, db_session):
        '''
        Burst a query as bootstrap user and verifies the status is correct
        '''
        with self.db.cursor() as cursor:
            self.execute_test_file('burst_query', session=self.db)
            qid = self.last_query_id(cursor)
            cursor.execute(CHECK_BURST_STATUS_TXT_SQL.format(qid))
            txt = cursor.fetchone()[0]
            assert txt.strip() == ("Concurrency Scaling ineligible query "
                                   "- Bootstrap user query")

    def test_non_burst_queue(self, db_session):
        '''
        Run a query in a non burst queue and verifies the status is correct
        '''
        with db_session.cursor() as cursor:
            cursor.execute("set query_group to noburst;")
            cursor.execute("select count(*) from catalog_sales A;")
            qid = self.last_query_id(cursor)
        with self.db.cursor() as cursor:
            cursor.execute(CHECK_BURST_STATUS_SQL.format(qid))
            status = cursor.fetch_scalar()
            assert status == 0

@pytest.mark.load_data
@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_AUTO_GUCS)
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestNoBurstCases(BurstTest):
    def generate_sql_res_files(self):
        return True

    @property
    def format_sql_on_write(self):
        return False

    def test_no_burst_cases(self, db_session):
        self.execute_test_file('no_burst_cases', session=db_session)


@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=CUSTOM_NESTED_SPECTRUM_GUCS)
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestNestedSpectrumNoBurst(BurstTest):
    """
    Tests that run Spectrum queries that select from nested Spectrum tables
    and check that such queries do not burst.
    """

    def generate_sql_res_files(self):
        return True

    @property
    def format_sql_on_write(self):
        return False

    def test_nested_spectrum_no_burst(self, db_session):
        self.execute_test_file('nested_spectrum_no_burst', session=db_session)


INCOMPATIBLE_SERIALIZER_THRESHOLD = 3
RELEASE_ERROR_THRESHOLD = 3
INCOMPATIBLE_SERIALIZER_TRESHOLD_GUCS = {
    'burst_serializer_errors_threshold': INCOMPATIBLE_SERIALIZER_THRESHOLD,
    'burst_release_errors_threshold' : RELEASE_ERROR_THRESHOLD,
    'burst_errors_disabling_period_minutes': 1
}


@pytest.mark.load_data
@pytest.mark.load_tpcds_data
@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=INCOMPATIBLE_SERIALIZER_TRESHOLD_GUCS)
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstTempDisabled(BurstTest):
    @contextmanager
    def auto_reenable(self, cluster):
        cluster.run_xpx('burst_reenable')
        yield
        cluster.run_xpx('burst_reenable')

    def test_burst_temp_disabled_incompatible(self, cluster, db_session):
        '''
        Test that fails a query 3 times due to incompatible serializer error
        (simulated with and event), and checks that burst is disabled after
        that.
        '''
        with self.auto_reenable(cluster):
            with set_event(cluster, "EtBurstFailSerDeVersion"):
                for i in range(INCOMPATIBLE_SERIALIZER_THRESHOLD):
                    try:
                        self.execute_test_file('burst_query', session=db_session)
                        pytest.fail("Burst query was supposed to fail but passed")
                    except FailedTestException:
                        pass

            with self.db.cursor() as bootcursor, db_session.cursor() as cursor:
                for i in range(INCOMPATIBLE_SERIALIZER_THRESHOLD):
                    # Try again to run the same queries.
                    # This time they should not burst.
                    self.execute_test_file('burst_query', session=db_session)
                    qid = self.last_query_id(cursor)
                    bootcursor.execute(CHECK_BURST_STATUS_SQL.format(qid))
                    status = bootcursor.fetch_scalar()
                    assert status == 20

    def test_burst_temp_disabled_release(self, cluster, db_session):
        '''
        Test that simulates releases due to gateway errors, and checks that
        burst is disabled after that.
        '''
        with self.auto_reenable(cluster):
            for i in range(RELEASE_ERROR_THRESHOLD):
                reason = random.choice(["FailedPersonalization", "GatewayError"])
                cluster.run_xpx("burst_release fake_arn_{} {}".format(
                    i, reason))

            with self.db.cursor() as bootcursor, db_session.cursor() as cursor:
                for i in range(RELEASE_ERROR_THRESHOLD):
                    # Try again to run the same queries.
                    # This time they should not burst.
                    self.execute_test_file('burst_query', session=db_session)
                    qid = self.last_query_id(cursor)
                    bootcursor.execute(CHECK_BURST_STATUS_SQL.format(qid))
                    status = bootcursor.fetch_scalar()
                    assert status == 20


DISABLE_BURST_GUCS = {
    'enable_burst': 'false'
}

@pytest.mark.load_tpcds_data
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=DISABLE_BURST_GUCS)
@pytest.mark.burst_precommit
@pytest.mark.serial_only
@pytest.mark.localhost_only
class TestBurstDisabled(BurstTest):
    def test_burst_disabled(self, cluster, db_session):
        '''
        Test that query status is assigned correctly when burst is disabled.
        '''
        self.execute_test_file('burst_query', session=db_session)
        with self.db.cursor() as bootcursor, db_session.cursor() as cursor:
            qid = self.last_query_id(cursor)
            query = CHECK_BURST_STATUS_SQL.format(qid)
            status = run_priviledged_query_scalar_int(cluster, bootcursor,
                                                      query)
            assert status == 2, "Status of query {} is {}".format(qid, status)
