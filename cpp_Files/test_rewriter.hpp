/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#pragma once

// gtest must be included before pgtest
#include "ddm/ddm_policy.hpp"
#include "sys/result_cache_api.h"

#include "pg/test/pg_test.hpp"

#include <boost/format.hpp>
#include <gtest/gtest.h>

#include <string>

namespace pgtest {
class TestDDMRewriter : public pg::PgTest {
 public:
  static void SetUpTestCase();
  static void TearDownTestCase();
};

/// Utility method for validating that
/// 1. The input query has non-Zero requiredPErms
/// 2. DDM is applied on a query containing masks.
/// 3. requiredPerms of the input query rte are the same as the ddm subquery.
/// 4. requiredPerms of the output query from query rewrite all are 0.
void ValidateDDMPropagationForMaskedTable(std::string input_q_str,
                                          ResultCacheState rcs);

/// Utility method to get formatted message for mismatch between expected
/// rewritten query and actual rewritten query.
/// @param expected_query the expected rewritten query.
/// @param rewritten_query the actual rewritten query.
/// @return formatted error message.
boost::format GetQueryNotMatchedErrorMessage(
    std::string const& expected_query, std::string const& rewritten_query);
};  // namespace pgtest
