#include "regex_adapter_test.hpp"

#include <cppunit/config/SourcePrefix.h>

#include "re2/cppunit_interface.h"
#include "regex/regex_dispatch.hpp"
#include "xen_utils/unicode_ops.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION(RegexAdapterTest);

constexpr re2::Rune UNICODE_UPPER_LIMIT = 0x10ffff;

template <typename MatchType>
static void CheckMatch(const MatchType& match, size_t start) {
  char out_buf[2];

  CPPUNIT_ASSERT(match.Matched(0));
  CPPUNIT_ASSERT_EQUAL(2UL, match.Length(0));
  CPPUNIT_ASSERT_EQUAL(start, match.Position(0));
  CPPUNIT_ASSERT_EQUAL(2UL, match.Write(0, out_buf, 2));
  CPPUNIT_ASSERT_EQUAL('a', out_buf[0]);
  CPPUNIT_ASSERT_EQUAL('b', out_buf[1]);

  CPPUNIT_ASSERT(match.Matched(1));
  CPPUNIT_ASSERT_EQUAL(1UL, match.Length(1));
  CPPUNIT_ASSERT_EQUAL(start, match.Position(1));
  CPPUNIT_ASSERT_EQUAL(1UL, match.Write(1, out_buf, 2));
  CPPUNIT_ASSERT_EQUAL('a', out_buf[0]);

  CPPUNIT_ASSERT(match.Matched(2));
  CPPUNIT_ASSERT_EQUAL(1UL, match.Length(2));
  CPPUNIT_ASSERT_EQUAL(start + 1UL, match.Position(2));
  CPPUNIT_ASSERT_EQUAL(1UL, match.Write(2, out_buf, 2));
  CPPUNIT_ASSERT_EQUAL('b', out_buf[0]);
}

template <typename MatchPtr>
static void CheckMatchPtr(const MatchPtr& match, size_t start) {
  char out_buf[2];

  CPPUNIT_ASSERT(match->Matched(0));
  CPPUNIT_ASSERT_EQUAL(2UL, match->Length(0));
  CPPUNIT_ASSERT_EQUAL(start, match->Position(0));
  CPPUNIT_ASSERT_EQUAL(2UL, match->Write(0, out_buf, 2));
  CPPUNIT_ASSERT_EQUAL('a', out_buf[0]);
  CPPUNIT_ASSERT_EQUAL('b', out_buf[1]);

  CPPUNIT_ASSERT(match->Matched(1));
  CPPUNIT_ASSERT_EQUAL(1UL, match->Length(1));
  CPPUNIT_ASSERT_EQUAL(start, match->Position(1));
  CPPUNIT_ASSERT_EQUAL(1UL, match->Write(1, out_buf, 2));
  CPPUNIT_ASSERT_EQUAL('a', out_buf[0]);

  CPPUNIT_ASSERT(match->Matched(2));
  CPPUNIT_ASSERT_EQUAL(1UL, match->Length(2));
  CPPUNIT_ASSERT_EQUAL(start + 1UL, match->Position(2));
  CPPUNIT_ASSERT_EQUAL(1UL, match->Write(2, out_buf, 2));
  CPPUNIT_ASSERT_EQUAL('b', out_buf[0]);
}

template <typename RegexType>
static void test_regex_iterator_impl() {
  RegexParameters params;
  params.submatch = true;
  RegexType regex("(a)(b)", 6, params, REGEX_ALLOC_STD);
  typename RegexType::StateOptional state_1, state_2, state_3, state_4;
  typename RegexType::IteratorType gen_1 =
      regex.RegexIterator("abab", 4, &state_1);
  typename RegexType::IteratorType gen_2 =
      regex.RegexIterator("abab", 4, &state_2);
  typename RegexType::IteratorType gen_3 =
      regex.RegexIterator("abab", 4, &state_3);
  typename RegexType::IteratorType gen_4 =
      regex.RegexIterator("abab", 4, &state_4);
  typename RegexType::IteratorType end = regex.RegexIteratorEnd();

  std::vector<RegexMatchResults> stored_results;
  stored_results.assign(gen_3, end);
  CPPUNIT_ASSERT_EQUAL(2UL, stored_results.size());

  {
    typename RegexType::MatchRef val_a = *gen_1;
    RegexMatchResults val_b = RegexMatchResults(*gen_2);
    const RegexMatchResults& val_c = stored_results[0];
    typename RegexType::IteratorType val_d = gen_1;
    typename RegexType::CopyPtr val_e = gen_2++;
    CheckMatch(val_a, 0);
    CheckMatch(val_b, 0);
    CheckMatch(val_c, 0);
    CheckMatchPtr(val_d, 0);
    CheckMatchPtr(val_e, 0);
    ++gen_1;
  }

  {
    typename RegexType::MatchRef val_a = *gen_2;
    const RegexMatchResults& val_b = stored_results[1];
    CheckMatch(val_a, 2);
    CheckMatch(val_b, 2);
    typename RegexType::IteratorType val_c = gen_2;
    typename RegexType::CopyPtr val_d = gen_1++;
    CheckMatchPtr(val_c, 2);
    CheckMatchPtr(val_d, 2);
    RegexMatchResults val_e = RegexMatchResults(*gen_2++);
    CheckMatch(val_e, 2);
  }

  CPPUNIT_ASSERT(gen_1 == end);
  CPPUNIT_ASSERT(gen_2 == end);

  CPPUNIT_ASSERT_EQUAL(2L, std::distance(gen_4, end));
}

void RegexAdapterTest::test_regex_iterator() {
  test_regex_iterator_impl<BoostRegexAdapter>();
  test_regex_iterator_impl<RE2RegexAdapter>();
}

void RegexAdapterTest::test_re2_unicode() {
  for (re2::Rune r = 0; r < UNICODE_UPPER_LIMIT; ++r) {
    re2::Rune lower = unicode_to_lower(r);
    re2::Rune upper = unicode_to_upper(r);
    if (UNLIKELY(r != lower || r != upper)) {
      std::stringstream buf;
      buf << std::hex << "0x" << r;
      std::string r_str = buf.str();
      CPPUNIT_ASSERT_MESSAGE(r_str, re2::EquivalentUpToCase(r, lower));
      CPPUNIT_ASSERT_MESSAGE(r_str, re2::EquivalentUpToCase(r, upper));
    }
  }
}
