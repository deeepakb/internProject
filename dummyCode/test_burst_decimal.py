# Copyright 2020 Amazon.com, Inc. or its affiliates
# All Rights Reserved
import pytest
from copy import deepcopy

from raff.burst.burst_test import (BurstTest, is_burst_hydration_on,
                                   setup_teardown_burst, verify_query_bursted)
from raff.common.db.redshift_db import RedshiftDb

# Trick to get around claim of unused import even though it is used in the
# usefixtures marker.
__all__ = [setup_teardown_burst, verify_query_bursted]

"""
This test file covers behavior consistency between Redshift and Burst
regarding decimal operations. The same Yaml file is used to compare results
between the following 3 situations:
 - Redshift only
 - Burst based on Spectrum
 - Burst based on Hydration

The decimal_types test suite covers type conversion and rounding/overflow
behavior.

The decimal_expr test suite covers expression applied on decimals. It is
generated from the expr-test unit test suite of Spectrum.
See Spectrum: be/src/exprs/expr-test.cc
For the decimal_expr test suite, queries are based on the following template:

  SELECT {expression}
  FROM dummy_table
  GROUP BY 1;

The group by and the dummy_table ensure that in the Spectrum case the
expression gets effectively pushed down to Spectrum and is evaluated
there.

The decimal_parse test suite covers parsing decimals from a string.

Issues covered (not an exhaustive list):
  Burst-988
    burst_decimal_expr_28  burst_decimal_expr_98 burst_decimal_expr_461
    burst_decimal_expr_462 burst_decimal_expr_485 burst_decimal_expr_486
    burst_decimal_expr_493 burst_decimal_expr_494
  Burst-1598
    burst_decimal_expr_328 burst_decimal_expr_331 burst_decimal_expr_334
    burst_decimal_expr_337 burst_decimal_types_43
  Burst-1639
    burst_decimal_expr_394 burst_decimal_expr_395
    burst_decimal_expr_405 burst_decimal_expr_406
    burst_decimal_expr_429 burst_decimal_expr_430
    burst_decimal_expr_446 burst_decimal_expr_447
  Burst-2065
    burst_decimal_expr_340 burst_decimal_expr_343 burst_decimal_types_42

Disabled tests:
  Burst-1298 - Sum+cast behaviour mismatch between Redshift and Spectrum
    burst_decimal_types_60

  Burst-1530 - Decimal(19,0) overflow
    burst_decimal_expr_92  burst_decimal_expr_93

  Burst-2974 - Modulo behaviour mismatch between Redshift and Spectrum
    burst_decimal_expr_259 to burst_decimal_expr_269
    burst_decimal_expr_285
"""

DROP_TYPE_STMT = "DROP TABLE IF EXISTS decimal_types;"
LOAD_TYPE_STMT =\
    "CREATE TABLE decimal_types AS (SELECT * FROM s3.decimal_types_csv);"

DROP_EXPR_STMT = "DROP TABLE IF EXISTS dummy_table;"
LOAD_EXPR_STMT = "CREATE TABLE dummy_table AS (SELECT 0::int zero);"

DROP_PARSE_STMT = "DROP TABLE IF EXISTS decimal_parse;"
LOAD_PARSE_STMT =\
    "CREATE TABLE decimal_parse AS (SELECT * FROM s3.decimal_parse_csv);"


@pytest.fixture(scope='class')
def setup_and_cleanup_data_decimals(cluster):
    """
    This fixture is used to load and cleanup two decimal test dataset.
    """
    conn_params = cluster.get_conn_params()
    try:
        with RedshiftDb(conn_params) as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(DROP_TYPE_STMT)
            cursor.execute(DROP_EXPR_STMT)
            cursor.execute(DROP_PARSE_STMT)
            cursor.execute(LOAD_TYPE_STMT)
            cursor.execute(LOAD_EXPR_STMT)
            cursor.execute(LOAD_PARSE_STMT)
            yield
    finally:
        with RedshiftDb(conn_params) as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(DROP_TYPE_STMT)
            cursor.execute(DROP_EXPR_STMT)
            cursor.execute(DROP_PARSE_STMT)


# Disable query level codegen because some queries will otherwise fail early
# on main, resulting in an unexpected error code.
BURST_HYDRATION_GUCS = dict(
    enable_query_level_code_generation='false',
    burst_use_local_scans='true',
    rr_dist_num_rows=1)


@pytest.mark.qp_bucket_1
@pytest.mark.no_jdbc
@pytest.mark.serial_only
@pytest.mark.session_ctx(user_type='super')
@pytest.mark.create_external_schema
@pytest.mark.dory_exhaustive
@pytest.mark.usefixtures("setup_and_cleanup_data_decimals")
class TestBurstDecimalRedshift(BurstTest):
    """
    Run the decimal_types, decimal_expr and decimal_parse test suites against
    Redshift.
    """

    def _transform_test(self, test, vector):
        """
        In some cases (e.g. divide by zero), Redshift and Burst return
        different error code. The test result for these cases is updated
        here to fit the expected error code on Redshift.
        """
        if test.error.err_code == "{err_code}":
            transformed_test = deepcopy(test)
            transformed_test.error.err_code = "57014"
            return transformed_test
        return test

    def test_burst_decimal_types_redshift(self, db_session):
        self.execute_test_file('burst_decimal_types', session=db_session)

    def test_burst_decimal_expr_redshift(self, db_session):
        self.execute_test_file('burst_decimal_expr', session=db_session)

    def test_burst_decimal_parse_redshift(self, db_session):
        self.execute_test_file('burst_decimal_parse', session=db_session)


@pytest.mark.no_jdbc
@pytest.mark.localhost_only
@pytest.mark.serial_only
@pytest.mark.session_ctx(user_type='super')
@pytest.mark.usefixtures("setup_teardown_burst")
@pytest.mark.create_external_schema
@pytest.mark.custom_burst_gucs(gucs=BURST_HYDRATION_GUCS)
@pytest.mark.usefixtures("setup_and_cleanup_data_decimals")
class TestBurstDecimalBurstHydration(BurstTest):
    """
    Run the decimal_types, decimal_expr and decimal_parse test suites against
    Burst using hydration.
    """

    def _transform_test(self, test, vector):
        """
        In some cases (e.g. divide by zero), Redshift and Burst return
        different error code. The test result for these cases is updated
        here to fit the expected error code on Burst.
        """
        if test.error.err_code == "{err_code}":
            transformed_test = deepcopy(test)
            transformed_test.error.err_code = "XX000"
            return transformed_test
        return test

    def test_burst_decimal_types_burst_hydration(self, cluster, db_session,
                                                 verify_query_bursted):
        assert is_burst_hydration_on(cluster)
        self.execute_test_file('burst_decimal_types', session=db_session)

    def test_burst_decimal_expr_burst_hydration(self, cluster, db_session,
                                                verify_query_bursted):
        assert is_burst_hydration_on(cluster)
        self.execute_test_file('burst_decimal_expr', session=db_session)

    def test_burst_decimal_parse_burst_hydration(self, cluster, db_session,
                                                 verify_query_bursted):
        assert is_burst_hydration_on(cluster)
        self.execute_test_file('burst_decimal_parse', session=db_session)
