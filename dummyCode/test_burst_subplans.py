# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest

from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst"]


@pytest.mark.load_tpcds_data
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.no_jdbc
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstSubplans(BurstTest):
    def test_burst_subplans(self, db_session):
        """
        Tests queries that use the SubPlan plan node.
        """
        self.execute_test_file('burst_subplans', session=db_session)
