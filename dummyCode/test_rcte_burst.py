# Copyright 2018 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
from raff.burst.burst_test import (BurstTest, setup_teardown_burst)

__all__ = ["setup_teardown_burst"]


@pytest.mark.localhost_only
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.serial_only
class TestRCteBurst(BurstTest):
    @property
    def generate_sql_res_files(self):
        return True

    def test_rcte_burst(self, db_session, cluster):
        cluster.run_xpx('clear_result_cache')
        self.execute_test_file('rcte_burst', session=db_session)
