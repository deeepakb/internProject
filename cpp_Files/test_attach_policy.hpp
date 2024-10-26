/// Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#pragma once

#include <gtest/gtest.h>

#include "ddm/ddm_policy.hpp"

#include "pg/test/pg_test.hpp"

#include <string>
#include <vector>

namespace pgtest {
/// Container for parameters of masking policy attachment test cases.
struct PolicyAttachmentParam {
  /// Query to be executed to create and attach the policy.
  std::vector<std::string> queries;
  /// Name of the policy to be created and attached.
  std::string pol_name;
  /// Policy attrs.
  std::string pol_attrs;
  /// Policy expression.
  std::string pol_expr;
  /// Input attributes for the masking policy expression in JSON.
  std::string pol_attrsin;
  /// Output attributes for the masking policy expression in JSON.
  std::string pol_attrsout;
  /// Expected total number of the parsed policies
  std::size_t expected_pol_size;
  /// Expected parsed policy's total number of input attributes.
  std::size_t expected_polattrsin_size;
  /// Expected parsed policy's total number of output attributes.
  std::size_t expected_polattrsout_size;
  /// Expected column name of the parsed policy's attrsin[0].
  std::string expected_polattrsin_0_colname;
  /// Expected column name of the parsed policy's attrsout[0].
  std::string expected_polattrsout_0_colname;
};
class TestDDMPolicyAttachment
    : public pg::PgTest,
      public ::testing::WithParamInterface<PolicyAttachmentParam> {
 public:
  static void SetUpTestCase();
  static void TearDownTestCase();
};
}  // namespace pgtest
