#include <cppunit/BriefTestProgressListener.h>
#include <cppunit/CompilerOutputter.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/TestRunner.h>
#include <cppunit/config/SourcePrefix.h>
#include <cppunit/extensions/TestFactoryRegistry.h>

// clang-format off
#include "test_percentile_parser.hpp"
#include "catalog/pg_aggregate.h"
#include "postgres_ext.h"
#include "parser/parse_func.h"
// clang-format on

CPPUNIT_TEST_SUITE_REGISTRATION(Test_Percentile_Parser);

void Test_Percentile_Parser::test_is_median_function() {
  CPPUNIT_ASSERT(is_median_function(AGG_INT8_MEDIAN));
  CPPUNIT_ASSERT(is_median_function(AGG_INT4_MEDIAN));
  CPPUNIT_ASSERT(is_median_function(AGG_INT2_MEDIAN));
  CPPUNIT_ASSERT(is_median_function(AGG_NUMERIC_MEDIAN));
  CPPUNIT_ASSERT(is_median_function(AGG_FLOAT4_MEDIAN));
  CPPUNIT_ASSERT(is_median_function(AGG_FLOAT8_MEDIAN));
  CPPUNIT_ASSERT(is_median_function(AGG_DATE_MEDIAN));
  CPPUNIT_ASSERT(is_median_function(AGG_TIMESTAMP_MEDIAN));
  CPPUNIT_ASSERT(is_median_function(AGG_TIMESTAMPTZ_MEDIAN));
  CPPUNIT_ASSERT(is_median_function(AGG_INTERVALY2M_MEDIAN));
  CPPUNIT_ASSERT(is_median_function(AGG_INTERVALD2S_MEDIAN));
  CPPUNIT_ASSERT(is_median_function(AGG_TIME_MEDIAN));
  CPPUNIT_ASSERT(is_median_function(AGG_TIMETZ_MEDIAN));

  CPPUNIT_ASSERT(!is_median_function(AGG_INT8_MIN));
  CPPUNIT_ASSERT(!is_median_function(AGG_DATE_FIRST_VALUE));
  CPPUNIT_ASSERT(!is_median_function(AGG_QSUMMARY_TIMESTAMP));
  CPPUNIT_ASSERT(!is_median_function(AGG_INT8_RATIO_TO_REPORT));
}

void Test_Percentile_Parser::test_convert_median_oid_to_percentile_oid() {
  CPPUNIT_ASSERT_EQUAL((Oid)AGG_INT8_PERCENTILE_CONT,
                       convert_median_oid_to_percentile_oid(AGG_INT8_MEDIAN));
  CPPUNIT_ASSERT_EQUAL((Oid)AGG_INT4_PERCENTILE_CONT,
                       convert_median_oid_to_percentile_oid(AGG_INT4_MEDIAN));
  CPPUNIT_ASSERT_EQUAL((Oid)AGG_INT2_PERCENTILE_CONT,
                       convert_median_oid_to_percentile_oid(AGG_INT2_MEDIAN));
  CPPUNIT_ASSERT_EQUAL((Oid)AGG_NUMERIC_PERCENTILE_CONT,
                      convert_median_oid_to_percentile_oid(AGG_NUMERIC_MEDIAN));
  CPPUNIT_ASSERT_EQUAL((Oid)AGG_FLOAT4_PERCENTILE_CONT,
                       convert_median_oid_to_percentile_oid(AGG_FLOAT4_MEDIAN));
  CPPUNIT_ASSERT_EQUAL((Oid)AGG_FLOAT8_PERCENTILE_CONT,
                       convert_median_oid_to_percentile_oid(AGG_FLOAT8_MEDIAN));
  CPPUNIT_ASSERT_EQUAL((Oid)AGG_DATE_PERCENTILE_CONT,
                       convert_median_oid_to_percentile_oid(AGG_DATE_MEDIAN));
  CPPUNIT_ASSERT_EQUAL(
      (Oid)AGG_TIMESTAMP_PERCENTILE_CONT,
      convert_median_oid_to_percentile_oid(AGG_TIMESTAMP_MEDIAN));
  CPPUNIT_ASSERT_EQUAL(
      (Oid)AGG_TIMESTAMPTZ_PERCENTILE_CONT,
      convert_median_oid_to_percentile_oid(AGG_TIMESTAMPTZ_MEDIAN));
  CPPUNIT_ASSERT_EQUAL(
      (Oid)AGG_INTERVALY2M_PERCENTILE_CONT,
      convert_median_oid_to_percentile_oid(AGG_INTERVALY2M_MEDIAN));
  CPPUNIT_ASSERT_EQUAL(
      (Oid)AGG_INTERVALD2S_PERCENTILE_CONT,
      convert_median_oid_to_percentile_oid(AGG_INTERVALD2S_MEDIAN));
  CPPUNIT_ASSERT_EQUAL(
      (Oid)AGG_TIME_PERCENTILE_CONT,
      convert_median_oid_to_percentile_oid(AGG_TIME_MEDIAN));
  CPPUNIT_ASSERT_EQUAL(
      (Oid)AGG_TIMETZ_PERCENTILE_CONT,
      convert_median_oid_to_percentile_oid(AGG_TIMETZ_MEDIAN));

  CPPUNIT_ASSERT_EQUAL((Oid)0,
                       convert_median_oid_to_percentile_oid(AGG_INT8_MIN));
  CPPUNIT_ASSERT_EQUAL(
      (Oid)0, convert_median_oid_to_percentile_oid(AGG_DATE_FIRST_VALUE));
  CPPUNIT_ASSERT_EQUAL(
      (Oid)0, convert_median_oid_to_percentile_oid(AGG_QSUMMARY_TIMESTAMP));
  CPPUNIT_ASSERT_EQUAL(
      (Oid)0, convert_median_oid_to_percentile_oid(AGG_INT8_RATIO_TO_REPORT));
}
