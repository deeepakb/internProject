#pragma once

#include <cppunit/extensions/HelperMacros.h>

class Test_Percentile_Util_Functions : public CPPUNIT_NS::TestFixture {
 private:
  CPPUNIT_TEST_SUITE(Test_Percentile_Util_Functions);
  CPPUNIT_TEST(test_is_percentile_agg_function);
  CPPUNIT_TEST(test_is_percentile_cont_agg_function);
  CPPUNIT_TEST(test_is_percentile_agg_dist_function);

  CPPUNIT_TEST(test_get_percentile_agg_function_size);
  CPPUNIT_TEST(test_get_const_decimal_value);
  CPPUNIT_TEST_SUITE_END();

  /***
  planner.c is_percentile_agg_function, get_percentile_function_size,
  get_percentile_agg_function_size, is_percentile_cont_function
  parse_aggr.c transformPercentileCont, transformListagg,
  loadOrderbyIntoAggregate
  parse_func.c transform_median_func_to_percentile,
  */

 protected:
  void test_is_percentile_agg_function();
  void test_is_percentile_cont_agg_function();
  void test_is_percentile_agg_dist_function();

  void test_get_percentile_agg_function_size();
  void test_get_const_decimal_value();
};

