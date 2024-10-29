#pragma once

#include <cppunit/extensions/HelperMacros.h>

class Test_Percentile_Parser : public CPPUNIT_NS::TestFixture {
 private:
  CPPUNIT_TEST_SUITE(Test_Percentile_Parser);
  CPPUNIT_TEST(test_is_median_function);
  CPPUNIT_TEST(test_convert_median_oid_to_percentile_oid);
  CPPUNIT_TEST_SUITE_END();

 protected:
  void test_is_median_function();
  void test_convert_median_oid_to_percentile_oid();
};

