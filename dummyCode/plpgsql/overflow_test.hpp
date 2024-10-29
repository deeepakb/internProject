#pragma once

#include <cppunit/extensions/HelperMacros.h>


class Overflow_test : public CPPUNIT_NS::TestFixture
{

private:

  CPPUNIT_TEST_SUITE( Overflow_test  );
  CPPUNIT_TEST( dstring_append_test );
  CPPUNIT_TEST_SUITE_END();

protected:
  void dstring_append_test();

};



