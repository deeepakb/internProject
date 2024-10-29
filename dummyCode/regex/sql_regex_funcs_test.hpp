#pragma once

#include <cppunit/extensions/HelperMacros.h>

class SqlRegexFuncsTest : public CPPUNIT_NS::TestFixture {
 public:
  CPPUNIT_TEST_SUITE(SqlRegexFuncsTest);
  CPPUNIT_TEST(test_non_utf8);
  CPPUNIT_TEST_SUITE_END();

 private:
  void test_non_utf8();
};

