/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#pragma once

#include <gtest/gtest.h>

#include <string>

#include "pg/src/include/parser/parse_node.h"
#include "pg/test/pg_test.hpp"

namespace pgtest {

class TestDDMMutator : public pg::PgTest {
 public:
  static void SetUpTestCase();
  static void TearDownTestCase();
};

/// no-op mutator to verify copy flags
Node* NoOpMutator(Node* node, void* context);

/// Utility method which tests that given an input query, and flags that
/// the the NoOp query_tree_mutator visits an equal number of nodes and produces
/// an equivalent query tree when the no copy and copy flags are set. This
/// method has appropriate assertions for QTW_DONT_COPY_QUERY
/// QTW_DONT_COPY_SUBQUERY
/// QTW_DONT_COPY_NODE
void ValidateNoOpEquality(Query* input_q, int flags);

};  // namespace pgtest
