#include "overflow_test.hpp"
#include "xen_utils/qid.hpp"

#include "pl/plpgsql/src/plpgsql.h"

#include <memory>

const int LEN = INT_MAX >> 8;

CPPUNIT_TEST_SUITE_REGISTRATION( Overflow_test );

void Overflow_test::dstring_append_test()
{
  PLpgSQL_dstring ds;
  std::unique_ptr<char[]> str(new char[LEN]);
  bool append_failed = false;

  memset(str.get(), 'A', LEN - 1);
  str[LEN - 1] = '\0';

  plpgsql_dstring_init(&ds);

  PG_TRY();
  {
    for (size_t i = 0; i < ULONG_MAX; ++i)
      plpgsql_dstring_append(&ds, str.get());
  }
  PG_CATCH();
  {
    append_failed = true;
  }
  PG_END_TRY();
  plpgsql_dstring_free(&ds);

  CPPUNIT_ASSERT(append_failed);
}
