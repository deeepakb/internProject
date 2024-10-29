#include "sql_regex_funcs_test.hpp"

#include <cppunit/config/SourcePrefix.h>

#include <iostream>
#include <vector>
#include <regex/boost_regex.hpp>
#include <regex/re2_regex.hpp>
#include "regex/sql_regex_funcs.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION(SqlRegexFuncsTest);

static constexpr size_t kNumSequences = 52;
static constexpr const char* kSequences[kNumSequences] = {
    // State 0-1, unexpected continuation byte
    "\x80",
    // State 0-1, two byte overlong encoding
    "\xc0",
    // State 0-1-1, two byte overlong encoding
    "\xc0\xbf",
    // State 0-2-1, ascii byte used as continuation byte
    "\xc2\x7f",
    // State 0-2-1, leading byte used as continuation byte
    "\xc2\xc0",
    // State 0-3-1, ascii byte used as continuation byte
    "\xe1\x7f",
    // State 0-3-1, leading byte used as continuation byte
    "\xe1\xc0",
    // State 0-3-2-1, ascii byte used as continuation byte
    "\xe1\x80\x7f",
    // State 0-3-2-1, leading byte used as continuation byte
    "\xe1\x80\xc0",
    // State 0-3-1, ascii byte used as continuation byte
    "\xee\x7f",
    // State 0-3-1, leading byte used as continuation byte
    "\xee\xc0",
    // State 0-3-2-1, ascii byte used as continuation byte
    "\xee\x80\x7f",
    // State 0-3-2-1, leading byte used as continuation byte
    "\xe1\x80\xc0",
    // State 0-4-1, ascii byte used as continuation byte
    "\xe0\x7f",
    // State 0-4-1, leading byte used as continuation byte
    "\xe0\xc0",
    // State 0-4-1, three byte overlong encoding
    "\xe0\x9f",
    // State 0-4-1-1, three byte overlong encoding
    "\xe0\x9f\xbf",
    // State 0-4-2-1, ascii byte used as continuation byte
    "\xe0\xa0\x7f",
    // State 0-4-2-1, leading byte used as continuation byte
    "\xe0\xa0\xc0",
    // State 0-5-1, ascii byte used as continuation byte
    "\xed\x7f",
    // State 0-5-1, leading byte used as continuation byte
    "\xed\xc0",
    // State 0-5-1, surrogate codepoint
    "\xed\xa0",
    // State 0-5-1, surrogate codepoint
    "\xed\xa0\x80",
    // State 0-5-2-1, ascii byte used as continuation byte
    "\xed\x80\x7f",
    // State 0-5-2-1, leading byte used as continuation byte
    "\xed\x80\x7f",
    // State 0-6-1, ascii byte used as continuation byte
    "\xf0\x7f",
    // State 0-6-1, leading byte used as continuation byte
    "\xf0\xc0",
    // State 0-6-1, four byte overlong encoding
    "\xf0\x8f",
    // State 0-6-1-1-1, four byte overlong encoding
    "\xf0\x8f\xbf\xbf",
    // State 0-6-3-1, ascii byte used as continuation byte
    "\xf0\x90\x7f",
    // State 0-6-3-1, leading byte used as continuation byte
    "\xf0\x90\xc0",
    // State 0-6-3-2-1, ascii byte used as continuation byte
    "\xf0\x90\x80\x7f",
    // State 0-6-3-2-1, ascii byte used as continuation byte
    "\xf0\x90\x80\xc0",
    // State 0-7-1, ascii byte used as continuation byte
    "\xf1\x7f",
    // State 0-7-1, leading byte used as continuation byte
    "\xf1\xc0",
    // State 0-7-3-1, ascii byte used as continuation byte
    "\xf1\x80\x7f",
    // State 0-7-3-1, leading byte used as continuation byte
    "\xf1\x80\xc0",
    // State 0-7-3-2-1, ascii byte used as continuation byte
    "\xf1\x80\x80\x7f",
    // State 0-7-3-2-1, ascii byte used as continuation byte
    "\xf1\x80\x80\xc0",
    // State 0-8-1, ascii byte used as continuation byte
    "\xf4\x7f",
    // State 0-8-1, leading byte used as continuation byte
    "\xf4\xc0",
    // State 0-8-1, four byte too large codepoint
    "\xf4\x90",
    // State 0-8-1-1-1, four byte too large codepoint
    "\xf4\x90\x80\x80",
    // State 0-8-3-1, ascii byte used as continuation byte
    "\xf4\x80\x7f",
    // State 0-8-3-1, leading byte used as continuation byte
    "\xf4\x80\xc0",
    // State 0-8-3-2-1, ascii byte used as continuation byte
    "\xf4\x80\x80\x7f",
    // State 0-8-3-2-1, ascii byte used as continuation byte
    "\xf4\x80\x80\xc0",
    // State 0-1, too large codepoint
    "\xf5",
    // State 0-1, too large codepoint
    "\xf5\x80\x80\x80",
    // State 0-1, too many bytes
    "\xf8",
    // State 0-1-1-1-1-1, too many bytes
    "\xf8\x88\x80\x80\x80",
    // Special case: null byte
    "\x00"
};

static constexpr size_t kNumTails = 9;
static constexpr const char* kTails[kNumTails] = {
    "", "a", "aa", "aaa", "aaaa", "α", "αα", "ａ", "🅰"
};

static void AssertLessEqualWithInfo(
    size_t part, size_t i, size_t j, x_len_t lhs, x_len_t rhs) {
  std::ostringstream msg_stream;
  msg_stream << "Part " << part << " sequence " << i;
  msg_stream << " tail " << j << ": " << lhs << " is not <= " << rhs;
  CPPUNIT_ASSERT_MESSAGE(msg_stream.str(), lhs <= rhs);
}

void SqlRegexFuncsTest::test_non_utf8() {
  /***
   * The test cases are inspired by the DFA in
   * https://bjoern.hoehrmann.de/utf-8/decoder/dfa/. Each string explores
   * illegal state transitions in that DFA, followed by between 0 and 4 other
   * bytes of various styles. This should give good coverage of the types of
   * invalid UTF-8 scenarios that exist in arbitrary binary data.
   *
   * We only need to check that the regex functions don't crash, cause
   * out of bound accesses, or return strings with invalid lengths. We have no
   * guarantees about the actual returned results.
   */

  std::vector<char> xenstr_bytes(32);
  xenstr* xenstr_ptr = reinterpret_cast<xenstr*>(xenstr_bytes.data());
  x_len_t xenstr_rep_size = 0;
  xenstr* xenstr_rep_ptr = reinterpret_cast<xenstr*>(&xenstr_rep_size);
  std::vector<char> xenstr_dest_bytes(32);
  xenstr* xenstr_dest_ptr = reinterpret_cast<xenstr*>(xenstr_dest_bytes.data());

  BoostRegexAdapter::RegexAdapterPtr boost_ptr;
  RE2RegexAdapter::RegexAdapterPtr re2_ptr;
  RegexReplaceData<BoostRegexAdapter> boost_replace_data;
  RegexReplaceData<RE2RegexAdapter> re2_replace_data;

  const char* pattern = ".*";
  x_len_t pattern_len = strlen(pattern);
  xenstr_ptr->len = pattern_len;
  memcpy(xenstr_ptr->str, pattern, pattern_len);
  RegexParameters params = {};
  params.case_ascii_only = true;
  f_xenstr_regex_init(xenstr_ptr, &boost_ptr, params);
  f_xenstr_regex_init(xenstr_ptr, &re2_ptr, params);

  for (size_t i = 0; i < kNumSequences; i++) {
    const char* sequence = kSequences[i];
    for (size_t j = 0; j < kNumTails; j++) {
      const char* tail = kTails[j];
      x_len_t sequence_len = strlen(sequence);
      x_len_t tail_len = strlen(tail);
      size_t max_allowed_len = sequence_len + tail_len;
      xenstr_ptr->len = sequence_len + tail_len;
      memcpy(xenstr_ptr->str, sequence, sequence_len);
      memcpy(xenstr_ptr->str + sequence_len, tail, tail_len);
      f_xenstr_regex_match(xenstr_ptr, boost_ptr.get());
      f_xenstr_regex_match(xenstr_ptr, re2_ptr.get());
      f_xenstr_regexp_count(xenstr_ptr, boost_ptr.get(), 1);
      f_xenstr_regexp_count(xenstr_ptr, re2_ptr.get(), 1);
      f_xenstr_regexp_instr(xenstr_ptr, boost_ptr.get(), 1, 1, 0);
      f_xenstr_regexp_instr(xenstr_ptr, re2_ptr.get(), 1, 1, 0);
      f_xenstr_regex_substr(xenstr_ptr, boost_ptr.get(), xenstr_dest_ptr, 1, 1);
      AssertLessEqualWithInfo(0, i, j, xenstr_dest_ptr->len, max_allowed_len);
      f_xenstr_regex_substr(xenstr_ptr, re2_ptr.get(), xenstr_dest_ptr, 1, 1);
      AssertLessEqualWithInfo(1, i, j, xenstr_dest_ptr->len, max_allowed_len);
    }
  }

  pattern = "(unseen string)?";
  pattern_len = strlen(pattern);
  xenstr_ptr->len = pattern_len;
  memcpy(xenstr_ptr->str, pattern, pattern_len);
  f_xenstr_regex_init(xenstr_ptr, &boost_ptr, params);
  f_xenstr_regex_init(xenstr_ptr, &re2_ptr, params);
  boost_replace_data.Setup(boost_ptr.get(), xenstr_rep_ptr);
  re2_replace_data.Setup(re2_ptr.get(), xenstr_rep_ptr);

  for (size_t i = 0; i < kNumSequences; i++) {
    const char* sequence = kSequences[i];
    for (size_t j = 0; j < kNumTails; j++) {
      const char* tail = kTails[j];
      x_len_t sequence_len = strlen(sequence);
      x_len_t tail_len = strlen(tail);
      x_len_t max_allowed_len = sequence_len + tail_len + 1;
      xenstr_ptr->len = sequence_len + tail_len;
      memcpy(xenstr_ptr->str, sequence, sequence_len);
      memcpy(xenstr_ptr->str + sequence_len, tail, tail_len);
      f_xenstr_regexp_replace(xenstr_ptr, xenstr_rep_ptr, boost_ptr.get(),
                              xenstr_dest_ptr, 1);
      AssertLessEqualWithInfo(2, i, j, xenstr_dest_ptr->len, max_allowed_len);
      f_xenstr_regexp_replace(xenstr_ptr, xenstr_rep_ptr, re2_ptr.get(),
                              xenstr_dest_ptr, 1);
      AssertLessEqualWithInfo(3, i, j, xenstr_dest_ptr->len, max_allowed_len);
      f_xenstr_regexp_replace(xenstr_ptr, xenstr_rep_ptr, boost_ptr.get(),
                              xenstr_dest_ptr, 1, &boost_replace_data);
      AssertLessEqualWithInfo(4, i, j, xenstr_dest_ptr->len, max_allowed_len);
      f_xenstr_regexp_replace(xenstr_ptr, xenstr_rep_ptr, re2_ptr.get(),
                              xenstr_dest_ptr, 1, &re2_replace_data);
      AssertLessEqualWithInfo(5, i, j, xenstr_dest_ptr->len, max_allowed_len);
    }
  }
}
