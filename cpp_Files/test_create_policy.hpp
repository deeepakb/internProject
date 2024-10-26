/// Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#pragma once

#include <gtest/gtest.h>

#include "ddm/ddm_policy.hpp"

#include "pg/test/pg_test.hpp"

#include <string>

namespace pgtest {
/// Container for parameters of the test case of policy creation.
struct PolicyCreationParam {
  /// Query to be executed to create the masking policy.
  std::string query;
  /// Name of the masking policy to be created.
  std::string name;
};
class TestDDMPolicyCreation
    : public pg::PgTest,
      public ::testing::WithParamInterface<PolicyCreationParam> {
 public:
  static void SetUpTestCase();
  static void TearDownTestCase();
};
}  // namespace pgtest
