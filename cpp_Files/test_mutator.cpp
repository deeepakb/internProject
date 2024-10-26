/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#include "pg/ddm/test_mutator.hpp"

#include "omnisql/omnisql_translate_query.hpp"
#include "rewrite/rewriteHandler.h"

#include "pg/src/include/optimizer/clauses.h"
#include "pg/test/pg_test.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <set>
#include <string>

namespace pgtest {

struct VisitorContext {
  // Counter of visits the mutator has made across the Query Tree.
  int visits = 0;
};

Node* NoOpMutator(Node* node, void* context) {
  auto ctx = reinterpret_cast<VisitorContext*>(context);
  ++ctx->visits;
  return node;
}

void TestDDMMutator::SetUpTestCase() {
  pg::RunQuery("CREATE TABLE mutator_t (a int);");
  pg::RunQuery("CREATE TABLE mutator_rls_t (a int);");
  pg::RunQuery("ALTER TABLE mutator_rls_t ROW LEVEL SECURITY ON;");
  pg::RunQuery("CREATE USER u1_mutator_test PASSWORD DISABLE;");
  pg::RunQuery("GRANT SELECT ON mutator_rls_t TO u1_mutator_test;");
  pg::RunQuery("GRANT SELECT ON mutator_t TO u1_mutator_test;");
  pg::RunQuery("ALTER USER u1_mutator_test with CREATEUSER;");
}

void TestDDMMutator::TearDownTestCase() {
  pg::RunQuery("DROP TABLE mutator_t;");
  pg::RunQuery("DROP TABLE mutator_rls_t;");
  pg::RunQuery("DROP USER u1_mutator_test;");
}

void ValidateNoOpEquality(Query* input_q, int flags) {

  Query* input_q_copy = copyObject(input_q);
  auto ctx = VisitorContext();
  Query* result = query_tree_mutator(input_q_copy, NoOpMutator, &ctx, flags);
  auto copy_visits = ctx.visits;
  EXPECT_TRUE(copy_visits > 0) << "Expected to visit multiple nodes.";

  EXPECT_TRUE(input_q_copy != result)
      << "Expected result to not be pointer equal with input_q.";
  EXPECT_TRUE(equal(input_q, result)) << "Failed deep equality check";

  input_q_copy = copyObject(input_q);
  ctx = VisitorContext();
  result = query_tree_mutator(input_q_copy, NoOpMutator, &ctx,
                              flags ^ QTW_DONT_COPY_NODE);
  auto no_copy_visits = ctx.visits;
  EXPECT_TRUE(input_q_copy->targetList == result->targetList)
      << "Mutator replaced targetList pointer.";
  EXPECT_TRUE(linitial(input_q_copy->targetList) ==
              linitial(result->targetList))
      << "Mutator replaced targetEntry pointer";
  EXPECT_TRUE(no_copy_visits > 0) << "Expected to visit multiple nodes.";
  EXPECT_TRUE(no_copy_visits == copy_visits)
      << "Visited a different number of nodes";
  EXPECT_TRUE(input_q_copy == result)
      << "Expected result to be pointer equal with input_q.";
  EXPECT_TRUE(equal(input_q, result)) << "Failed deep equality check";


  auto old_rte =
      reinterpret_cast<RangeTblEntry*>(linitial(input_q_copy->rtable));
  auto new_rte = reinterpret_cast<RangeTblEntry*>(linitial(result->rtable));
  if (result->rtable != nullptr && old_rte->rtekind == RTE_SUBQUERY) {
    EXPECT_TRUE(old_rte->rtekind == RTE_SUBQUERY) << "expected a subquery.";
    EXPECT_TRUE(old_rte->subquery != nullptr) << "expected non-null subquery";
    EXPECT_TRUE(old_rte->subquery == new_rte->subquery)
        << "expected subquery not to be modified.";
  }

  EXPECT_TRUE(equal(input_q, result)) << "Failed deep equality check";

  input_q_copy = copyObject(input_q);
  ctx = VisitorContext();
  result = query_tree_mutator(input_q_copy, NoOpMutator, &ctx,
                              flags ^ QTW_DONT_COPY_QUERY);
  no_copy_visits = ctx.visits;
  EXPECT_TRUE(no_copy_visits > 0) << "Expected to visit multiple nodes.";
  EXPECT_TRUE(no_copy_visits == copy_visits)
      << "Visited a different number of nodes";
  EXPECT_TRUE(input_q_copy == result)
      << "Expected result to be pointer equal with input_q.";
  EXPECT_TRUE(equal(input_q, result)) << "Failed deep equality check";
}

// Performs an A/B test that a complex query is correctly processed across
// different combinations of flags.
PG_TEST_F(TestDDMMutator, TestNoOpCopy) {
  auto input_q_str =
      "WITH cte1 AS (SELECT * FROM mutator_t AS d where a > 5),"
      " rlscte1 AS (SELECT avg(distinct(a)), a FROM mutator_rls_t WHERE a < 5 "
      "GROUP BY a HAVING a <3 ORDER BY 1 desc)"
      " SELECT e.a, d.a FROM (SELECT * FROM rlscte1) as d JOIN cte1 "
      "as e ON (e.a = "
      "d.a);";
  auto input_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(input_q_str))));
  ValidateNoOpEquality(input_q, 0x0);
  ValidateNoOpEquality(input_q, QTW_WALK_RELATIONS);

  ValidateNoOpEquality(input_q, QTW_WALK_RELATIONS ^ QTW_WALK_RLS_SUBQUERIES);
  ValidateNoOpEquality(input_q, QTW_WALK_RELATIONS ^ QTW_WALK_RLS_SUBQUERIES);
  ValidateNoOpEquality(input_q, QTW_WALK_RELATIONS ^ QTW_IGNORE_RT_SUBQUERIES);
}

// Performs an A/B test of a sub query containing an RLS reference.
PG_TEST_F(TestDDMMutator, TestNoOpCopySimpleSubQuery) {
  auto input_q_str =
      "SELECT * from (SELECT * from mutator_rls_t) as mutator_rls_t;";

  auto input_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(input_q_str))));
  auto subquery_rte =
      reinterpret_cast<RangeTblEntry*>(linitial(input_q->rtable));
  EXPECT_TRUE(subquery_rte->rtekind == RTE_SUBQUERY) << "Expected subquery";
  auto nested_rls_rte = reinterpret_cast<RangeTblEntry*>(
      linitial(subquery_rte->subquery->rtable));

  EXPECT_TRUE(nested_rls_rte->rtekind == RTE_RLS_RELATION) << "Expected RLS";
  nested_rls_rte->subquery = copyObject(subquery_rte->subquery);

  ValidateNoOpEquality(input_q, 0x0);
  ValidateNoOpEquality(input_q, QTW_WALK_RELATIONS);

  ValidateNoOpEquality(input_q, QTW_WALK_RELATIONS ^ QTW_WALK_RLS_SUBQUERIES);
  ValidateNoOpEquality(input_q, QTW_WALK_RELATIONS ^ QTW_WALK_RLS_SUBQUERIES);
  ValidateNoOpEquality(input_q, QTW_WALK_RELATIONS ^ QTW_IGNORE_RT_SUBQUERIES);
}

// A/B test that unnest queries have identical visits with and without copy
// flags.
PG_TEST_F(TestDDMMutator, TestUnnestQuery) {
  auto input_q_str =
      "WITH data AS (SELECT array(1,2,3,4) AS num UNION ALL (SELECT array(5,6) "
      "as num)) SELECT nums FROM data as d, d.num AS nums;";
  auto input_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::ParseWithNestedSupport(input_q_str))));

  ValidateNoOpEquality(input_q, 0x0);
  ValidateNoOpEquality(input_q, QTW_WALK_RELATIONS);

  ValidateNoOpEquality(input_q, QTW_WALK_RELATIONS ^ QTW_WALK_RLS_SUBQUERIES);
  ValidateNoOpEquality(input_q, QTW_WALK_RELATIONS ^ QTW_WALK_RLS_SUBQUERIES);
  ValidateNoOpEquality(input_q, QTW_WALK_RELATIONS ^ QTW_IGNORE_RT_SUBQUERIES);
}
};  // namespace pgtest
