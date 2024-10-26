# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
import pytest
from raff.rls.rls_test import RLSTest
from raff.data_loaders.functional import (load_data as tpch_data,
                                          load_tpcds_data as tpcds_data)


@pytest.mark.skip(reason="DP-35469")
class TestIsolateTestData(RLSTest):
    '''
    Test suite to verify test data is loaded onto isolated test db correctly.
    '''

    @pytest.mark.isolate_db(test_data=[tpch_data])
    def test_isolate_test_data(self, cursor):
        assert 0 < cursor.execute_scalar(
            'select count(*) from tpch_nation_redshift')

    @pytest.mark.isolate_db(test_data=[tpch_data, tpcds_data])
    def test_isolate_multiple_loaders(self, cursor):
        assert 0 < cursor.execute_scalar('select count(*) from store_returns')
        assert 0 < cursor.execute_scalar(
            'select count(*) from tpch_nation_redshift')

    @pytest.mark.serial_only  # dbname is fixed.
    @pytest.mark.isolate_db(dbname='testdb', test_data=[tpch_data])
    def test_known_test_db(self, cursor):
        assert 0 < cursor.execute_scalar(
            'select count(*) from tpch_nation_redshift')
