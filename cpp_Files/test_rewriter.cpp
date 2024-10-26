/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#include "pg/ddm/test_rewriter.hpp"

#include "ddm/rewriter.h"
#include "omnisql/omnisql_print_sql.hpp"
#include "omnisql/omnisql_translate_query.hpp"
#include "rewrite/rewriteHandler.h"

#include "pg/src/include/utils/lsyscache.h"
#include "pg/test/pg_test.hpp"

#include <boost/format.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <set>
#include <string>

extern bool g_pgtest_ddm;
extern bool gconf_enable_ddm;
extern bool gconf_enable_ddm_on_views;
extern bool gconf_ddm_enable_over_super_paths;
extern bool gconf_enable_qpc_for_session;

auto TEST1_MASK =
    "(SELECT 2 * foo_col AS foo_col, 10 * bar_col AS bar_col, 2 * car_col "
    "as car_col, 5 * dar_col AS dar_col, 15 * ear_col AS ear_col, "
    "CASE WHEN far_col IN (SELECT b FROM lookup) THEN far_col + 5 "
    "ELSE NULL END AS far_col FROM test1)";

auto VIEW1_MASK = R"SQL(
    foo_col, bar_col, car_col, dar_col, ear_col,
    CASE WHEN far_col IN (SELECT b FROM lookup) THEN far_col + 5 ELSE NULL
    END AS far_col
    )SQL";
namespace pgtest {

void TestDDMRewriter::SetUpTestCase() {
  g_pgtest_ddm = true;
  gconf_enable_ddm = true;
  gconf_enable_ddm_on_views = true;
  gconf_enable_qpc_for_session = false;
  pg::RunQuery("CREATE TABLE t (a int);");
  pg::RunQuery("CREATE TABLE lookup (b int);");
  pg::RunQuery(
      "CREATE TABLE test1 (foo_col int, bar_col int, car_col int, dar_col int, "
      "ear_col int, far_col int);");
  pg::RunQuery(
      "CREATE TABLE test2 (foo_col int, bar_col int, car_col int, dar_col int, "
      "ear_col int, far_col int);");
  pg::RunQuery("CREATE TABLE test4 (foo_col int, bar_col int);");
  pg::RunQuery("CREATE TABLE test5 (a int);");
  pg::RunQuery(
      "CREATE MASKING POLICY mp1re with (a int, b int) USING (2 * a, 5 * b);");
  pg::RunQuery(
      "CREATE MASKING POLICY mp2re with (a int, b int) USING (10 * a, 15 * "
      "b);");
  pg::RunQuery(
      "CREATE MASKING POLICY mp3re with (a int, b int) USING (10 * a * b);");
  pg::RunQuery(
      "CREATE MASKING POLICY mp4re WITH (a int) USING "
      "(CASE WHEN a IN (SELECT b FROM lookup) THEN a + 5 ELSE NULL END);");
  pg::RunQuery("CREATE MASKING POLICY mp5re WITH (a int) USING(a+10);");
  pg::RunQuery("CREATE USER u1re PASSWORD DISABLE;");
  pg::RunQuery("CREATE ROLE r1re;");
  pg::RunQuery("CREATE ROLE r2re;");
  pg::RunQuery("GRANT ROLE r1re TO u1re;");
  pg::RunQuery("GRANT ROLE r2re TO u1re;");
  pg::RunQuery("GRANT ALL ON TABLE test1 to u1re;");
  pg::RunQuery("GRANT ALL ON TABLE test2 to u1re;");
  pg::RunQuery("GRANT ALL ON TABLE lookup to u1re;");

  pg::RunQuery("GRANT ALL ON SCHEMA public to u1re;");

  /// Attach mp1 to u1 with priority 0 on foo_col, bar_col
  pg::RunQuery(
      "ATTACH MASKING POLICY mp1re ON test1 (foo_col, bar_col) TO u1re;");
  /// Attach mp2 to u1 with priority 1 on bar_col, car_col <- mp2 wins bar
  pg::RunQuery(
      "ATTACH MASKING POLICY mp2re ON test1 (bar_col, car_col)"
      " TO u1re PRIORITY 1;");
  /// Attach mp1 to r1 with priority 2 on car_col, dar_col <- mp1 wins dar, mp2
  /// wins car
  pg::RunQuery(
      "ATTACH MASKING POLICY mp1re ON test1 (car_col, dar_col)"
      " TO ROLE r1re PRIORITY 3;");
  /// Attach mp2 to r2 with priority 1 on dar_col, ear_col <- mp1 wins dar_col,
  /// mp2 wins ear_col
  pg::RunQuery(
      "ATTACH MASKING POLICY mp2re ON test1 (dar_col, ear_col)"
      " TO ROLE r2re PRIORITY 2;");
  /// Attach mp3 to u3re with priority 10 on foo_col, bar_col
  pg::RunQuery(
      "ATTACH MASKING POLICY mp3re ON test4 (foo_col) USING (foo_col, bar_col)"
      " TO u1re PRIORITY 10;");
  /// Attach mp4 to u1re with priority 1 on far_col.
  pg::RunQuery("ATTACH MASKING POLICY mp4re ON test1 (far_col) TO u1re;");
  /// Attach mp5 to u1re with priority on a
  pg::RunQuery("ATTACH MASKING POLICY mp5re ON test5 (a) TO u1re;");
  pg::RunQuery("CREATE VIEW re_view AS (SELECT * FROM test1);");
  pg::RunQuery("CREATE VIEW view1 AS (SELECT * FROM test1);");
  pg::RunQuery("CREATE VIEW view2 AS (SELECT * FROM view1);");
  pg::RunQuery(
      "CREATE VIEW lbv1 AS (SELECT * FROM pgtest_testcase_schema.test5) WITH "
      "NO SCHEMA BINDING;");
  pg::RunQuery(
      "CREATE VIEW lbv2 AS (SELECT * FROM pgtest_testcase_schema.lbv1) WITH NO "
      "SCHEMA BINDING;");

  pg::RunQuery("ATTACH MASKING POLICY mp4re ON view1(far_col) TO PUBLIC;");
  pg::RunQuery("ATTACH MASKING POLICY mp4re ON view2(far_col) TO PUBLIC;");
  pg::RunQuery("ATTACH MASKING POLICY mp5re ON lbv1(a) TO PUBLIC;");
  pg::RunQuery("ATTACH MASKING POLICY mp5re ON lbv2(a) TO PUBLIC;");
  /// TODO(jamoorem) - This is required to make the test tables visible to the
  ///                  user u1re. We should improve the pgtest infrastructure to
  //                   avoid this requirement.
  pg::RunQuery("ALTER USER u1re with CREATEUSER;");

  pg::RunQuery("CREATE TABLE target_table_dp53981 (id int, val varchar(256));");
  pg::RunQuery("CREATE TABLE source_table_dp53981 (id int);");
  pg::RunQuery("CREATE MASKING POLICY mp_dp53981 WITH (a int) USING (a+10);");
  pg::RunQuery(
      "ATTACH MASKING POLICY mp_dp53981 ON target_table_dp53981(id) TO "
      "PUBLIC;");
}

void TestDDMRewriter::TearDownTestCase() {
  g_pgtest_ddm = false;

  pg::RunQuery("DROP TABLE t;");
  // Need to drop masking policy which references lookup table because the
  // lookup table can not be dropped as the policy depends on it.
  pg::RunQuery("DROP VIEW IF EXISTS view2;");
  pg::RunQuery("DROP VIEW IF EXISTS view1;");
  pg::RunQuery("DROP VIEW IF EXISTS lbv1;");
  pg::RunQuery("DROP VIEW IF EXISTS lbv2;");
  pg::RunQuery("DROP MASKING POLICY mp4re CASCADE;");
  pg::RunQuery("DROP MASKING POLICY mp5re CASCADE;");
  pg::RunQuery("DROP MASKING POLICY mp_dp53981 CASCADE;");
  pg::RunQuery("DROP TABLE lookup;");
  pg::RunQuery("DROP VIEW IF EXISTS re_view_u1;");
  pg::RunQuery("DROP VIEW re_view;");
  pg::RunQuery("DROP TABLE test1;");
  pg::RunQuery("DROP TABLE test2;");
  pg::RunQuery("DROP TABLE IF EXISTS test3;");
  pg::RunQuery("DROP TABLE test4;");
  pg::RunQuery("DROP TABLE test5;");
  pg::RunQuery("DROP TABLE IF EXISTS target_table_dp53981;");
  pg::RunQuery("DROP TABLE IF EXISTS source_table_dp53981;");
  pg::RunQuery("DROP ROLE r1re FORCE;");
  pg::RunQuery("DROP ROLE r2re FORCE;");
}

void ValidateDDMPropagationForMaskedTable(std::string input_q_str,
                                          ResultCacheState rcs) {
  auto input_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(input_q_str))));
  auto input_rte = reinterpret_cast<RangeTblEntry*>(linitial(input_q->rtable));
  EXPECT_TRUE(input_rte->requiredPerms != 0)
      << "requiredPerms check not present pre-ddm";
  auto rewrite_result =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */);
  EXPECT_TRUE(rewrite_result.ddm_applied) << "DDM should have been applied.";

  ListCell* rewrite_rte_cell = NULL;
  // validate that RTE perms are pushed down for the first level of RTEs.
  EXPECT_TRUE(list_length(rewrite_result.rewrite->rtable) >= 1)
      << "DDM should have been applied.";
  foreach(rewrite_rte_cell, rewrite_result.rewrite->rtable) {
    auto ddm_rte = reinterpret_cast<RangeTblEntry*>(
        linitial(rewrite_result.rewrite->rtable));
    // skip rtables which are not ddm protected and do not have a nested q.
    // This test is not responsible for verifying that DDM policies are applied.
    if (ddm_rte->has_ddm_rewrite) {
      auto ddm_nested_q = reinterpret_cast<RangeTblEntry*>(ddm_rte)->subquery;
      EXPECT_TRUE(ddm_nested_q != nullptr);
      auto ddm_nested_rte =
          reinterpret_cast<RangeTblEntry*>(linitial(ddm_nested_q->rtable));
      EXPECT_TRUE(input_rte->requiredPerms == ddm_nested_rte->requiredPerms)
          << "DDM and input rte should have identical perms.";
      EXPECT_TRUE(
          bms_equal(input_rte->selectedCols, ddm_nested_rte->selectedCols))
          << "DDM and input rte should have identical selectedCols.";
      EXPECT_TRUE(
          bms_equal(input_rte->updatedCols, ddm_nested_rte->updatedCols))
          << "DDM and input rte should have identical updatedCols.";
      EXPECT_TRUE(input_rte->checkAsUser == ddm_nested_rte->checkAsUser)
          << "DDM and input rte should have identical checkAsUser.";
    }
  }

  auto qra_result = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));
  auto qra_rte = reinterpret_cast<RangeTblEntry*>(linitial(qra_result->rtable));
  if (qra_rte->rtekind != RTE_SUBQUERY) {
    EXPECT_TRUE(qra_rte->requiredPerms == 0)
        << "requiredPerms check not disabled for RTE";
  } else if (qra_rte->rtekind == RTE_SUBQUERY && qra_rte->has_ddm_rewrite) {
    auto qra_rte_inner = reinterpret_cast<RangeTblEntry*>(
        linitial(reinterpret_cast<RangeTblEntry*>(linitial(qra_result->rtable))
                     ->subquery->rtable));
    EXPECT_TRUE(qra_rte_inner->requiredPerms == 0)
        << "requiredPerms check not disabled for RTE";
  }
}

boost::format GetQueryNotMatchedErrorMessage(
    std::string const& expected_query, std::string const& rewritten_query) {
  return boost::format(
             "\n====\nExpected and rewritten queries do not match, Expected\n "
             "%s\nActual\n====\n%s\n====\n") %
         expected_query % rewritten_query;
}

/// Test's that a basic SQL SELECT statement will be rewritten correctly.
/// Verifies
/// - DDM Rewriter
/// - QueryRewriteAll rewriter
/// Query expectations are
/// Input Query:
///   "SELECT * from test1;"
/// Rewritten Query:
///   "SELECT * FROM (test1_mask) AS test1;"
PG_TEST_F(TestDDMRewriter, TestRewriteSimpleSelect) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);
  auto input_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse("SELECT * from test1;"))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;

  auto expected_q_fmt =
      boost::format("SELECT * FROM %s AS test1;") % TEST1_MASK;
  auto expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_fmt.str()))));
  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));
  EXPECT_TRUE(rewritten_q_omni == expected_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);

  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));

  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  SetUserId(current_id);
}

/// Test's that a basic SQL SELECT statement on a masked view
/// will be rewritten correctly.
/// Verifies
/// - DDM Rewriter
/// - QueryRewriteAll rewriter
/// Query expectations are
/// Input Query:
///   "SELECT * from view1;"
/// Rewritten Query:
///   "SELECT * FROM (SELECT view_mask FROM (masked_t1) AS test1;"
PG_TEST_F(TestDDMRewriter, TestRewriteMaskedView) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);
  auto input_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse("SELECT * FROM view1;"))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;

  auto expected_q_fmt =
      boost::format("SELECT * FROM (SELECT %s FROM view1) AS view1;") %
      VIEW1_MASK;
  auto expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_fmt.str()))));
  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));
  EXPECT_TRUE(rewritten_q_omni == expected_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);

  // rewrite displays as
  // SELECT * FROM
  //  SELECT MASKED FROM (
  // SELECT * FROM -- Original View definition
  // SELECT test1MASK
  expected_q_fmt = boost::format(
                       "SELECT * FROM (SELECT %s FROM (SELECT * FROM %s AS "
                       "test1) as view1) AS view1;") %
                   VIEW1_MASK % TEST1_MASK;
  expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_fmt.str()))));
  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));

  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));

  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  SetUserId(current_id);
}

/// Test's that a basic SQL SELECT statement on a nested masked view
/// will be rewritten correctly. The view view2 is a select * statement on
/// view1.
/// Verifies
/// - DDM Rewriter
/// - QueryRewriteAll rewriter
/// Query expectations are
/// Input Query:
///   "SELECT * from view2;"
PG_TEST_F(TestDDMRewriter, TestRewriteMaskedNestedView) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);
  auto input_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse("SELECT * FROM view2;"))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;

  auto expected_q_fmt =
      boost::format("SELECT * FROM (SELECT %s FROM view2) AS view2;") %
      VIEW1_MASK;
  auto expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_fmt.str()))));
  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));
  EXPECT_TRUE(rewritten_q_omni == expected_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);

  // rewrite displays as
  // SELECT * FROM
  //  SELECT MASKED FROM (
  // SELECT * FROM -- Original View definition
  // SELECT test1MASK
  expected_q_fmt = boost::format(
                       "SELECT * FROM (SELECT %s FROM (SELECT * FROM (SELECT "
                       "%s FROM (SELECT * FROM %s AS test1) as view1) AS "
                       "view1) AS view2) as view2;") %
                   VIEW1_MASK % VIEW1_MASK % TEST1_MASK;
  expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_fmt.str()))));
  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));

  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));

  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  SetUserId(current_id);
}

PG_TEST_F(TestDDMRewriter, TestRewriteMaskedLBV) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);

  auto input_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse("SELECT * FROM lbv1;"))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;

  auto expected_sql = R"SQL(
    SELECT * FROM 
        (SELECT a+10 AS a FROM
            (SELECT * FROM
                (SELECT a+10 AS a FROM test5)
            AS test5)
        AS lbv1
        )
    AS lbv1;
  )SQL";
  auto expected_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(expected_sql))));

  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));
  EXPECT_TRUE(rewritten_q_omni == expected_q_omni)
      << boost::format(
             "\n====\nExpected and rewritten queries do not match, Expected\n "
             "%s\nActual\n====\n%s\n====\n") %
             expected_q_omni % rewritten_q_omni;

  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);
  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));
  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << boost::format(
             "\n====\nExpected and rewritten queries do not match, Expected\n "
             "%s\nActual\n====\n%s\n====\n") %
             expected_q_omni % rewritten_q_omni;
  SetUserId(current_id);
}

PG_TEST_F(TestDDMRewriter, TestRewriteMaskedNestedLBV) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);

  auto input_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse("SELECT * FROM lbv2;"))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;

  auto expected_sql = R"SQL(
    SELECT * FROM 
        (SELECT a+10 AS a FROM
            (SELECT * FROM
                (SELECT a+10 AS a FROM
                    (SELECT * FROM
                        (SELECT a+10 AS a FROM test5)
                    AS test5)
                AS lbv1)
            AS lbv1)
        AS lbv2)
    AS lbv2;
  )SQL";
  auto expected_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(expected_sql))));

  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));
  EXPECT_TRUE(rewritten_q_omni == expected_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);

  expected_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(expected_sql))));
  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));

  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));

  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  SetUserId(current_id);
}

/// Tests that Views are rewritten with Executor context.
/// Verifies
/// - DDM Rewriter
/// - QueryRewriteAll rewriter
/// Query expectations are
/// For View:
///   "CREATE VIEW re_view_u1 AS (SELECT * FROM test1);"
///
/// Input Query:
///   "SELECT * from re_view_u1;"
/// Rewritten Query:
///   QueryRewriteAll:
///     As u1re:
///       "SELECT * FROM (select * from %s as test1) AS re_view_u1;"
///     As bootstrap:
///        "SELECT * FROM (select * from test1) AS re_view_u1;"
///   DDM Rewriter:
///     "SELECT * from re_view_u1;"
PG_TEST_F(TestDDMRewriter, TestRewriteSimpleSelectFromView) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");

  SetUserId(u1_id);
  // define a view with u1re as owner
  pg::RunQuery("CREATE VIEW re_view_u1 AS (SELECT * FROM test1);");

  auto input_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse("SELECT * from re_view_u1;"))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;

  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(input_q));
  EXPECT_TRUE(rewritten_q_omni == expected_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);

  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);
  auto expected_q_fmt =
      boost::format(
          "SELECT * FROM (select * from %s as test1) AS re_view_u1;") %
      TEST1_MASK;
  auto expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_fmt.str()))));

  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(copyObject(input_q), NULL, rcs, false)));

  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));

  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);

  SetUserId(current_id);
  input_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse("SELECT * from re_view;"))));
  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(copyObject(input_q), NULL, rcs, false)));

  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));

  std::string expected_q_str =
      "SELECT * FROM (select * from test1) AS re_view;";
  expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_str))));

  expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));

  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
}

/// Rewrites a simple select statement when DDM is disabled.
/// Expects that the query "SELECT * from test1;" is not rewritten.
PG_TEST_F(TestDDMRewriter, TestRewriteSimpleSelectDDMDisable) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);
  gconf_enable_ddm = false;
  auto input_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse("SELECT * from test1;"))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;

  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(input_q));
  EXPECT_TRUE(rewritten_q_omni == expected_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);

  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));

  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  SetUserId(current_id);
  gconf_enable_ddm = true;
}

/// Tests that a complex query with joins, predicates, and unmasked tables is
/// Is correctly rewritten.
/// Verifies
/// - DDM Rewriter
/// - QueryRewriteAll rewriter
/// Query expectations are
///   Input Query:
///     SELECT * FROM test1 AS a JOIN test1 AS b ON a.foo_col = b.foo_col
///     JOIN test2 AS c ON a.foo_col = c.foo_col;"
///   Rewritten Query:
///     SELECT * FROM (test1_mask) AS a JOIN (test1_mask) AS b on a.foo_col =
///     b.foo_col JOIN"
//          " test2 AS c ON a.foo_col = c.foo_col;
PG_TEST_F(TestDDMRewriter, TestRewriteQuery) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");

  SetUserId(u1_id);

  auto input_q = reinterpret_cast<Query*>(linitial(pg::Analyze(
      pg::Parse("SELECT * FROM test1 AS a JOIN test1 AS b ON a.foo_col = "
                "b.foo_col JOIN test2 AS c ON a.foo_col = c.foo_col;"))));

  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;

  auto expected_q_str =
      boost::format(
          "SELECT * FROM %s AS a JOIN %s AS b on a.foo_col = b.foo_col JOIN"
          " test2 AS c ON a.foo_col = c.foo_col;") %
      TEST1_MASK % TEST1_MASK;

  auto expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_str.str()))));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));
  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);

  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));

  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  SetUserId(current_id);
}

/// Tests that a query with column alias is correctly rewritten.
PG_TEST_F(TestDDMRewriter, TestRewriteColumnAlias) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");

  SetUserId(u1_id);

  auto input_q = reinterpret_cast<Query*>(linitial(
      pg::Analyze(pg::Parse("SELECT * FROM test1 fake (a, b, c, d, e, f);"))));

  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;

  auto expected_q_str =
      boost::format("SELECT * FROM %s fake (a, b, c, d, e, f);") % TEST1_MASK;

  auto expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_str.str()))));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));
  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);

  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));

  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  SetUserId(current_id);
}

/// Tests that an insert statement is not rewritten.
PG_TEST_F(TestDDMRewriter, TestRewriteInsert) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);
  auto input_q_str = "insert into test1 values (1,1,1,1,1,1);";
  auto input_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(input_q_str))));

  auto expected_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(input_q_str))));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));

  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;
  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);

  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));

  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  pg::RunQuery(input_q_str);
  SetUserId(current_id);
}

/// Tests that a Delete statement is rewritten.
/// Verifies
///  - DDM Rewriter
///  - QueryRewriteAll rewriter
///  - Statement is runnable via PG::RunQuery
/// Query expectations are
///   Input Query:
///     delete from test1 where foo_col = 1;
///   Output Query:
///     delete from test1 where (2 * foo_col) = 1;
PG_TEST_F(TestDDMRewriter, TestRewriteDelete) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);
  auto input_q_str = "delete from test1 where foo_col = 1;";
  auto expected_q_str = "delete from test1 where (2 * foo_col) = 1;";
  auto input_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(input_q_str))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;
  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));

  auto expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_str))));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);

  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));

  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  pg::RunQuery(input_q_str);
  SetUserId(current_id);
}

/// Tests that a DELETE statement with a DDM protected column which includes
/// lookup table is rewritten.
/// Verifies
///  - DDM Rewriter
///  - QueryRewriteAll rewriter
/// Query expectations are
///   Input Query:
///     DELETE FROM test1 where far_col = 1;
///   Output Query:
///     DELETE FROM test1 WHERE
///            (CASE WHEN far_col "IN (SELECT b FROM lookup) THEN far_col + 5
///             ELSE NULL
///             END) = 1;
PG_TEST_F(TestDDMRewriter, TestRewriteLookupDelete) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);
  auto input_q_str = "DELETE FROM test1 WHERE far_col = 1;";
  auto expected_q_str =
      "DELETE FROM test1 WHERE (CASE WHEN far_col IN "
      "(SELECT b FROM lookup) THEN far_col + 5 ELSE NULL END) = 1;";
  auto input_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(input_q_str))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;
  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));

  auto expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_str))));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);

  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));

  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  /// Statement is not runnable via PG::RunQuery because it hits:
  /// "error:  cannot handle unplanned sub-select"
  SetUserId(current_id);
}

/// Tests that a UPDATE statement is rewritten.
/// Verifies
///  - DDM Rewriter
///  - QueryRewriteAll rewriter
/// Query expectations are
///   Input Query:
///     update test1 set foo_col=3 where foo_col = 1;
///   Output Query:
///     update test1 set foo_col = 3 where 2 * foo_col = 1;
PG_TEST_F(TestDDMRewriter, TestRewriteUpdate) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);
  auto input_q_str = "update test1 set foo_col=3 where foo_col = 1;";
  auto expected_q_str = "update test1 set foo_col = 3 where 2 * foo_col = 1;";
  auto input_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(input_q_str))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;
  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  auto expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_str))));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);

  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));

  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);

  SetUserId(current_id);
}

/// Tests that an UPDATE statement with a DDM protected column which includes
/// lookup table is rewritten.
/// Verifies
///  - DDM Rewriter
///  - QueryRewriteAll rewriter
/// Query expectations are
///   Input Query:
///     update test1 set far_col=3 where far_col = 1;
///   Output Query:
///     update test1 set far_col = 3 where
///            (CASE WHEN far_col "IN (SELECT b FROM lookup) THEN far_col + 5
///             ELSE NULL
///             END) = 1;
PG_TEST_F(TestDDMRewriter, TestRewriteLookupUpdate) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);
  auto input_q_str = "update test1 set far_col=3 where far_col = 1;";
  auto expected_q_str =
      "update test1 set far_col = 3 where (CASE WHEN far_col "
      "IN (SELECT b FROM lookup) THEN far_col + 5 ELSE NULL END) = 1;";
  auto input_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(input_q_str))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;
  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  auto expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_str))));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);

  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));

  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);

  SetUserId(current_id);
}

/// Tests that a UPDATE statement with DDM relation in JOIN condition is
/// rewritten Verifies
///  - DDM Rewriter
///  - QueryRewriteAll rewriter
/// Query expectations are
///   Input Query:
///     update test1 set foo_col=3 where foo_col = 1;
///   Output Query:
///     update test1 set foo_col = 3 where 2 * foo_col = 1;
PG_TEST_F(TestDDMRewriter, TestRewriteUpdateWithJOIN) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);

  auto input_q_str = R"SQL(
    UPDATE target_table_dp53981 SET val = 'XXXX' FROM source_table_dp53981 s
    JOIN target_table_dp53981 t ON s.id = t.id;
    )SQL";

  auto expected_q_str = R"SQL(
    UPDATE target_table_dp53981 SET val = 'XXXX' FROM source_table_dp53981 s
    JOIN target_table_dp53981 t ON s.id = t.id + 10;
    )SQL";

  auto input_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(input_q_str))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;
  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));

  auto expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_str))));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));

  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);

  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);
  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));
  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));

  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);

  SetUserId(current_id);
}

/// Tests that a UPDATE statement is rewritten. When there are complex OpExpr
/// In the target.
/// Verifies
///  - DDM Rewriter
///  - QueryRewriteAll rewriter
/// Query expectations are
///   Input Query:
///     update test1 set foo_col= bar_col where foo_col != bar_col;
///   Output Query:
///     update test1 set foo_col = 10 * bar_col
///       where 2 * foo_col != 10 * bar_col;
PG_TEST_F(TestDDMRewriter, TestRewriteUpdateOpExprTarget) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);
  auto input_q_str =
      "update test1 set foo_col= bar_col where foo_col != bar_col;";
  auto expected_q_str =
      "update test1 set foo_col = 10 * bar_col where 2 * foo_col != 10 * "
      "bar_col;";
  auto input_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(input_q_str))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;
  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  auto expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_str))));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);

  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));

  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);

  SetUserId(current_id);
}

/// Tests that a UPDATE statement is rewritten. When there are nested
/// references.
/// Verifies
///  - DDM Rewriter
/// Query expectations are
///   Input Query:
///     update test1 set foo_col=1 where foo_col = ( select max(foo_col) from
///      test1);
///   Output Query:
///     update test1 set foo_col = 1 where 2 * foo_col = (select
///                     max(foo_col) from (test1_mask) as test1);
PG_TEST_F(TestDDMRewriter, TestRewriteUpdateNestedReference) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);
  auto input_q_str =
      "update test1 set foo_col=1 where foo_col = ( select max(foo_col) from "
      "test1);";

  auto expected_q_str =
      (boost::format("update test1 set foo_col = 1 where 2 * foo_col = (select "
                     "max(foo_col) from (%s) as test1);") %
       TEST1_MASK)
          .str();

  auto input_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(input_q_str))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;
  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  auto expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_str))));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);

  // create_entire_apg_pushdown_query_tree
  // does not work with PG Test, resulting in errors not present
  // with external RAFF testing.  Skip QueryRewriteAll for this case.

  SetUserId(current_id);
}

/// Tests that a UPDATE statement is rewritten. When the mask is multi-input.
/// Verifies
///  - DDM Rewriter
///  - QueryRewriteAll rewriter
/// Query expectations are
///   Input Query:
///     update test4 set foo_col=3 where foo_col = 1;
///   Output Query:
///     update test4 set foo_col = 3 where 10 * foo_col * bar_col = 1;
PG_TEST_F(TestDDMRewriter, TestRewriteUpdateMultiInputMask) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);
  auto input_q_str = "update test4 set foo_col=3 where foo_col = 1;";
  auto expected_q_str =
      "update test4 set foo_col = 3 where 10 * foo_col * bar_col = 1;";
  auto input_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(input_q_str))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;
  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  auto expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_str))));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);

  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));

  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);

  SetUserId(current_id);
}

/// Tests that a UPDATE statement is rewritten. When the mask is Nested.
/// Verifies
///  - DDM Rewriter
///  - QueryRewriteAll rewriter
/// Query expectations are
///   Input Query:
///     update test1 set foo_col=3 where exists (select test2.foo_col from
///      test2 where test2.bar_col = test1.foo_col limit 1);
///   Output Query:
///     update test1 set foo_col=3 where exists (select test2.foo_col from
///      test2 where test2.bar_col = (2 * test1.foo_col) limit 1);
PG_TEST_F(TestDDMRewriter, TestRewriteUpdateNestedMask) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);
  auto input_q_str =
      "update test1 set foo_col=3 where exists (select test2.foo_col from "
      "test2 where test2.bar_col = test1.foo_col limit 1);";
  auto expected_q_str =
      "update test1 set foo_col=3 where exists (select test2.foo_col from "
      "test2 where test2.bar_col = (2 * test1.foo_col) limit 1);";
  auto input_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(input_q_str))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;
  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  auto expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_str))));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);

  // create_entire_apg_pushdown_query_tree
  // does not work with PG Test, resulting in errors not present
  // with external RAFF testing.  Skip QueryRewriteAll for this case.
  SetUserId(current_id);
}

/// Tests that a UPDATE statement is rewritten. When the mask is Nested.
/// Verifies
///  - DDM Rewriter
/// Query expectations are
///   Input Query:
///     update test4 set foo_col=3 where exists (select test2.foo_col from
///      test2 where test2.bar_col = test4.foo_col limit 1);
///   Output Query:
///     update test4 set foo_col=3 where exists (select test2.foo_col from
///      test2 where test2.bar_col = (10 * test4.foo_col * test4.bar_col)
///      limit 1);
PG_TEST_F(TestDDMRewriter, TestRewriteUpdateNestedMultiMask) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);
  auto input_q_str =
      "update test4 set foo_col=3 where exists (select test2.foo_col from "
      "test2 where test2.bar_col = test4.foo_col limit 1);";
  auto expected_q_str =
      "update test4 set foo_col=3 where exists (select test2.foo_col from "
      "test2 where test2.bar_col = (10 * test4.foo_col * test4.bar_col) limit "
      "1);";
  auto input_q_list = pg::Analyze(pg::Parse(input_q_str));
  auto input_q = reinterpret_cast<Query*>(linitial(input_q_list));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;
  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  auto expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_str))));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  // create_entire_apg_pushdown_query_tree
  // does not work with PG Test, resulting in errors not present
  // with external RAFF testing.  Skip QueryRewriteAll for this case.

  SetUserId(current_id);
}

/// Tests that a CREATE Table AS statement is rewritten correctly.
/// Verifies
///  - DDM Rewriter
///  - QueryRewriteAll Rewriter.
/// Query expectations are
///   Input Query:
///     create table test3 as (select foo_col from test1);
///   Output Query:
///     create table test3 as (select foo_col from (test1_mask) as test1);
PG_TEST_F(TestDDMRewriter, TestRewriteCreateTableAs) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);
  auto input_q_str = "create table test3 as (select foo_col from test1);";
  auto expected_q_str =
      boost::format(
          "create table test3 as (select foo_col from %s as test1);") %
      TEST1_MASK;
  auto input_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(input_q_str))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;
  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  auto expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_str.str()))));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);

  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));

  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);

  pg::RunQuery(input_q_str);
  SetUserId(current_id);
}

/// Tests that a  join *to* a table which has DDM policies attached is rewritten
/// appropriately.
/// Verifies
///  - DDM Rewriter
///  - QueryRewriteAll Rewriter.
/// Query expectations are
///   Input Query:
///     select test1.foo_col from test2 join test1 on test1.foo_col =
///      test2.foo_col;
///   Output Query:
///     select test1.foo_col from test2 join test1_mask
///       as test1 on test1.foo_col = test2.foo_col;
PG_TEST_F(TestDDMRewriter, TestRewriteReverseJoin) {
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);
  auto input_q_str =
      "select test1.foo_col from test2 join test1 on test1.foo_col = "
      "test2.foo_col;";
  auto expected_q_str = boost::format(
                            "select test1.foo_col from test2 join %s as "
                            "test1 on test1.foo_col = test2.foo_col;") %
                        TEST1_MASK;
  auto input_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(input_q_str))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;
  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));

  auto expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_str.str()))));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);

  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));

  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);

  SetUserId(current_id);
}

/// Tests that DDM Does not modify subquery expressions which are used by
/// Omni. This test should be considered an invariant of DDM.
PG_TEST_F(TestDDMRewriter, TestDDMDoesNotModifySubQueries) {
  auto input_q_str =
      "SELECT * "
      "FROM ("
      "  SELECT * FROM ("
      "    SELECT t2.a"
      "    FROM t AS t2"
      "    WHERE  (t2.a = 5)"
      "  )"
      ");";
  auto input_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(input_q_str))));
  auto rewritten_input_q =
      ddm::RewriteQuery(input_q, false /* log_applied_policy */).rewrite;
  EXPECT_TRUE(input_q == rewritten_input_q) << "ddm rewriter replaced query"
                                               "pointer.";
  auto old_rte = reinterpret_cast<RangeTblEntry*>(linitial(input_q->rtable));
  auto new_rte =
      reinterpret_cast<RangeTblEntry*>(linitial(rewritten_input_q->rtable));
  EXPECT_TRUE(old_rte->rtekind == RTE_SUBQUERY) << "expected a subquery.";
  EXPECT_TRUE(old_rte->subquery != nullptr) << "expected non-null subquery";
  EXPECT_TRUE(old_rte->subquery == new_rte->subquery)
      << "expected subquery not to be modified.";
}

/// Tests that a basic SQL SELECT statement with a mask over SUPER column will
/// be rewritten correctly. Verifies
/// - DDM Rewriter
/// - QueryRewriteAll rewriter
/// - SwapColRefMutator
/// Query expectations are
/// Input Query:
///   "SELECT * from table_super;"
/// Rewritten Query:
///   "SELECT * FROM (mask_super) AS table_super;"
PG_TEST_F(TestDDMRewriter, TestRewriteSimpleSelectSuper) {
  // Expected rewritten super masked expression as subquery.
  auto MASK_SUPER =
      "(SELECT s, (s.version + 1.0)::FLOAT AS out FROM table_super)";
  pg::RunQuery("CREATE TABLE table_super (s SUPER, out FLOAT);");
  pg::RunQuery(
      "CREATE MASKING POLICY mp_super WITH (s SUPER) "
      "USING ( (s.version + 1.0)::FLOAT );");
  pg::RunQuery("GRANT ALL ON TABLE table_super to u1re;");
  // Attach mp_super to PUBLIC with default priority on out column using s
  // column.
  pg::RunQuery(
      "ATTACH MASKING POLICY mp_super ON table_super (out) "
      "USING (s) TO PUBLIC;");

  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);
  auto input_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse("SELECT * from table_super;"))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;

  auto expected_q_fmt =
      boost::format("SELECT * FROM %s AS table_super;") % MASK_SUPER;
  auto expected_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(expected_q_fmt.str()))));
  auto rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));
  EXPECT_TRUE(rewritten_q_omni == expected_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);

  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));

  rewritten_q_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  EXPECT_TRUE(expected_q_omni == rewritten_q_omni)
      << GetQueryNotMatchedErrorMessage(expected_q_omni, rewritten_q_omni);
  SetUserId(current_id);
  pg::RunQuery("DROP TABLE table_super;");
  pg::RunQuery("DROP MASKING POLICY mp_super;");
}

PG_TEST_F(TestDDMRewriter, TestDDMPermissionsPropagation) {
  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);

  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);

  //  Negative case: Validate that an unmasked table has
  //  0. requiredPerms != 0
  //  1. ddm was not applied.
  //  2. that the query has requiredPerms != 0 post QueryRewriteAll
  auto input_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse("SELECT * from t;"))));
  auto rte = reinterpret_cast<RangeTblEntry*>(linitial(input_q->rtable));
  EXPECT_TRUE(rte->requiredPerms != 0)
      << "requiredPerms check not present pre-ddm";
  auto rewrite_result =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */);
  EXPECT_FALSE(rewrite_result.ddm_applied)
      << "DDM should not have been applied on unmasked table.";
  auto qra_result = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(input_q, NULL, rcs, false)));
  rte = reinterpret_cast<RangeTblEntry*>(linitial(qra_result->rtable));
  EXPECT_TRUE(rte->requiredPerms == ACL_SELECT)
      << "requiredPerms should contain select.";

  // Validate that a basic select from a masked table propagates permissions.
  ValidateDDMPropagationForMaskedTable("SELECT * from test1;", rcs);

  // Validate propagation when both masked and unmasked tables are selected.
  ValidateDDMPropagationForMaskedTable(
      "SELECT (SELECT count(1) FROM test1), a from t;", rcs);

  // Same validation for insert queries.
  ValidateDDMPropagationForMaskedTable(
      "INSERT INTO test1 VALUES (1,2,3,4,5,6);", rcs);

  // Same for update Queries
  ValidateDDMPropagationForMaskedTable("UPDATE test1 SET foo_col = bar_col",
                                       rcs);

  // same for Delete Queries.
  ValidateDDMPropagationForMaskedTable("DELETE test1 WHERE foo_col = bar_col",
                                       rcs);
  SetUserId(current_id);
}

/// Tests that DDM Does not modify nodes which are not modified.
/// This test should be considered an invariant of DDM.
PG_TEST_F(TestDDMRewriter, TestDDMDoesNotModifyNodes) {
  auto input_q_str =
      "SELECT * "
      "FROM ("
      "  SELECT * FROM ("
      "    SELECT t2.a"
      "    FROM t AS t2"
      "    WHERE  (t2.a = 5)"
      "  )"
      ");";
  auto input_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(input_q_str))));
  auto rewritten_input_q =
      ddm::RewriteQuery(input_q, true /* log_applied_policy */).rewrite;
  EXPECT_TRUE(input_q->targetList == rewritten_input_q->targetList)
      << "ddm rewriter replaced targetList pointer.";
  EXPECT_TRUE(linitial(input_q->targetList) ==
              linitial(rewritten_input_q->targetList))
      << "ddm rewriter replaced targetEntry pointer";
}

// Simple test that checks that if an attachment to SUPER paths is read during
// the query execution, then the query is aborted with an error. See more tests
// cases in RAFF.
PG_TEST_F(TestDDMRewriter, TestSUPERPathsDowngradeProtection) {
  bool guc_old = gconf_ddm_enable_over_super_paths;
  gconf_ddm_enable_over_super_paths = false;

  pg::RunQuery("CREATE TABLE table_dp49817 (a int, b float, c text)");
  pg::RunQuery(
      "CREATE MASKING POLICY mp_dp49817 "
      "WITH(a int, b float, c text) USING (a, b, c);");
  pg::RunQuery("GRANT ALL ON TABLE table_dp49817 to u1re;");
  pg::RunQuery(
      "ATTACH MASKING POLICY mp_dp49817 "
      "ON table_dp49817(a, b, c) TO PUBLIC;");

  pg::RunQuery(
      R"(UPDATE pg_permission_mask                                        )"
      R"(SET                                                              )"
      R"(  polattrsout =                                                  )"
      R"(    '[                                                           )"
      R"(      { "t": 0, "column": 1, "path": "nested.path" },            )"
      R"(      { "t": 0, "column": 2, "path": "another.nested.path" },    )"
      R"(      3                                                          )"
      R"(    ]'                                                           )"
      R"(WHERE relid = (                                                  )"
      R"(  SELECT C.oid FROM pg_class C WHERE C.relname = 'table_dp49817' )"
      R"()                                                                )");

  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1re");
  SetUserId(u1_id);
  bool query_failed = false;

  PG_TRY();
  { pg::RunQuery("SELECT * from table_dp49817"); }
  PG_CATCH();
  {
    query_failed = true;
    // Note, the framework seems to only copy first 256 characters of the error!
    ErrorData* errData = CopyErrorData();
    std::string error_message = std::string(errData->message);
    std::transform(error_message.begin(), error_message.end(),
                   error_message.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    EXPECT_TRUE(error_message.find("super path") != std::string::npos);
    EXPECT_TRUE(error_message.find("unavailable") != std::string::npos);
    EXPECT_TRUE(error_message.find("upgrade") != std::string::npos);

    FreeErrorData(errData);
    FlushErrorState();
  }
  PG_END_TRY();

  EXPECT_TRUE(query_failed);

  SetUserId(current_id);
  pg::RunQuery("DROP TABLE table_dp49817");
  pg::RunQuery("DROP MASKING POLICY mp_dp49817 CASCADE");

  gconf_ddm_enable_over_super_paths = guc_old;
}

};  // namespace pgtest
