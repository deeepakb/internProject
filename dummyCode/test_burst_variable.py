# Copyright 2019 Amazon.com, Inc. or its affiliates
# All Rights Reserved

import pytest

from raff.burst.burst_test import BurstTest
from raff.dory.dory_test import DoryTest
from raff.burst.burst_test import setup_teardown_burst

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst]


@pytest.mark.load_tpcds_data
@pytest.mark.load_data
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstVariableSlices(BurstTest):
    """
    Basic testing coverage for various join types queries on Burst
    queries with variable slices.
    """
    @property
    def generate_sql_res_files(self):
        return True

    def test_burst_variable_basic_joins_tpch(self, db_session):
        self.execute_test_file('test_burst_variable_basic_joins_tpch',
                               session=db_session)

    def test_burst_variable_basic_joins_tpcds(self, db_session):
        self.execute_test_file('test_burst_variable_basic_joins_tpcds',
                               session=db_session)

    def test_burst1596_variable_leader(self, db_session):
        self.execute_test_file('test_burst1596_variable_leader',
                               session=db_session)


@pytest.mark.skip(reason="Need to be able to run EXPLAIN in Burst.")
@pytest.mark.burst_precommit
@pytest.mark.serial_only
class TestBurstVariableSlicesExplain(BurstTest):
    """
    Explains of the used queries.
    """
    @property
    def generate_sql_res_files(self):
        return True

    def test_burst_variable_basic_joins_tpch_explain(self, db_session):
        self.execute_test_file('test_burst_variable_basic_joins_tpch_explain',
                               session=db_session)

    def test_burst_variable_basic_joins_tpcds_explain(self, db_session):
        self.execute_test_file('test_burst_variable_basic_joins_tpcds_explain',
                               session=db_session)
