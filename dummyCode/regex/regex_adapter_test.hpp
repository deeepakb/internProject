#pragma once

#include <cppunit/extensions/HelperMacros.h>

class RegexAdapterTest : public CPPUNIT_NS::TestFixture {
 public:
  CPPUNIT_TEST_SUITE(RegexAdapterTest);
  CPPUNIT_TEST(test_regex_iterator);
  CPPUNIT_TEST(test_re2_unicode);
  CPPUNIT_TEST_SUITE_END();

 private:
  void test_regex_iterator();
  void test_re2_unicode();
};

