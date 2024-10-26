/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#pragma once

// These two includes must go first and in this order.
// clang-format off
#include <gtest/gtest.h>
#include "pg/test/pg_test.hpp"
// clang-format on

#include "ddm/ddm_policy.hpp"

#include "pg/test/pg_test.hpp"

#include <string>

namespace pgtest {

class TestDDMPolicy : public pg::PgTest {
 public:
  static void SetUpTestCase();
  static void TearDownTestCase();
};
};  // namespace pgtest
