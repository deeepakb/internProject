# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
from raff.burst.burst_test import (
    setup_teardown_burst,
    BurstTest
)

__all__ = [setup_teardown_burst]


@pytest.mark.load_ssb_10g_data
@pytest.mark.cluster_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestRedhawkFailedQueries(BurstTest):
    def test_redhawk_failed_queries(self, cluster, db_session):
        """
        Run queries that have failed in burst mode to check for
        regressions from the Redhawk suite
        """
        self.execute_test_file('burst_failed_queries',
                               session=db_session)
