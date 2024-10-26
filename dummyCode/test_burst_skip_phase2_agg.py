# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest

from raff.burst.burst_test import BurstTest
from raff.burst.burst_test import setup_teardown_burst

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst"]

GUCS = {'broadcast_megabytes_ceiling': '0',
        'skip_phase2_explain ': 'true',
        'explain_pretty_print': 'false'}


@pytest.mark.load_tpcds_data
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.custom_burst_gucs(gucs=GUCS)
@pytest.mark.serial_only
@pytest.mark.localhost_only
class TestBurstSkipPhase2Agg(BurstTest):
    @property
    def generate_sql_res_files(self):
        return True

    def test_burst_skip_phase2_agg(self, db_session):
        """
        Tests queries that use skip phase 2 aggregation.
        """
        self.execute_test_file('burst_skip_phase2_agg',
                               session=db_session)
