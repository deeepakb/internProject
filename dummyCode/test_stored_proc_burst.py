# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
from contextlib import contextmanager
from raff.burst.burst_test import (BurstTest, setup_teardown_burst)
from copy import deepcopy

__all__ = ["setup_teardown_burst"]

GUCS = {
    'enable_result_cache': 'true',
    'leave_burst_fetch_early_for_SPI': 'true',
    'skip_spi_dest_startup_from_burst_fetch': 'true'
}
GUCS_WITH_DISABLEMENT = {
    'enable_result_cache': 'true',
    'leave_burst_fetch_early_for_SPI': 'false',
    'skip_spi_dest_startup_from_burst_fetch': 'false'
}
BUCKET = "cookie-monster-s3-ingestion"
PREFIX = "raff-dp24640"


@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=GUCS)
@pytest.mark.load_tpcds_data
@pytest.mark.serial_only
class TestStoredProcBurst(BurstTest):
    @property
    def generate_sql_res_files(self):
        return True

    def test_stored_proc_burst(self, db_session, cluster):
        cluster.run_xpx('clear_result_cache')
        self.execute_test_file('stored_proc_burst', session=db_session)

    # [DP-31233] We add no_jdbc mark here because this test is failing on rc
    # with JDBC like this:
    # E   [Amazon](500310) Invalid operation: Could not execute query:
    # E   Details: -----------------------------------------------
    # E     error:  Could not execute query:
    # E     code:      19002
    # E     context:   cursor "<unnamed portal 2>" already exists
    # E     query:     599
    # E     location:  burst_client.cpp:800
    # E     process:   padbmaster [pid=111339]
    # E     -----------------------------------------------;
    #
    # TODO(sunqi): remove the mark once the issue is fixed.
    @pytest.mark.no_jdbc
    def test_stored_proc_burst_cursor(self, db_session, cluster):
        cluster.run_xpx('clear_result_cache')
        self.execute_test_file('stored_proc_burst_cursor', session=db_session)

    @pytest.mark.no_jdbc
    def test_stored_proc_burst_cursor_dp41213(self, db_session, cluster):
        cluster.run_xpx('clear_result_cache')
        self.execute_test_file(
            'stored_proc_burst_cursor_dp41213', session=db_session)


@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=GUCS_WITH_DISABLEMENT)
@pytest.mark.load_tpcds_data
@pytest.mark.serial_only
class TestStoredProcBurstCursorWithDisablement(BurstTest):
    @property
    def generate_sql_res_files(self):
        return True

    @pytest.mark.no_jdbc
    def test_stored_proc_burst_cursor_dp41213_guc_off(self, db_session,
                                                      cluster):
        cluster.run_xpx('clear_result_cache')
        self.execute_test_file(
            'stored_proc_burst_cursor_dp41213_guc_off', session=db_session)


@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=GUCS)
@pytest.mark.load_tpcds_data
@pytest.mark.serial_only
class TestStoredProcBurstUnload(BurstTest):
    @property
    def generate_sql_res_files(self):
        return True

    def _transform_test(self, test, vector):
        result = deepcopy(test)
        result.query.sql = test.query.sql.format(unload_path=self.unload_path)
        return result

    def _merge_tests(self, merge_from, merge_to):
        merge_to.query = merge_from.query
        return merge_to

    def test_stored_proc_burst_unload(self, db_session, cluster, s3_client):
        cluster.run_xpx('clear_result_cache')
        self.unload_path = "s3://{}/{}/{}/".format(
            BUCKET, PREFIX, self._generate_random_string())
        with self.unload_session(self.unload_path, s3_client):
            self.execute_test_file(
                'test_stored_proc_burst_unload', session=db_session)
