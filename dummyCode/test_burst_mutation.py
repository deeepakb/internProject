# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest

from raff.burst.all_shapes_burst_base import TestAllShapesBase
from raff.burst.burst_test import setup_teardown_burst


# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = ["setup_teardown_burst"]


@pytest.mark.all_shapes
@pytest.mark.load_min_all_shapes_data
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstMutation(TestAllShapesBase):
    def test_burst_scan_mutation(self, db_session, vector):
        """
        Tests basic scan mutation on various table characteristics.
        """
        self.execute_test_file(
            'burst_scan_mutation', session=db_session, vector=vector)

    def test_burst_agg_mutation(self, db_session, vector):
        """
        Tests basic scan and aggregation mutation on various table
        characteristics.
        """
        self.execute_test_file(
            'burst_agg_mutation', session=db_session, vector=vector)
