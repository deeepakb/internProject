/// Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.

// These two includes must go first and in this order.
// clang-format off
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
#include "pg/test/pg_test.hpp"
#include <gmock/gmock.h>
// clang-format on

#include "ddm/ddm_lf_alp.hpp"
#include "ddm/ddm_policy.hpp"
#include "ddm/policy_access.h"
#include "ddm/rewriter.h"
#include "external_catalog/model/constants.hpp"
#include "federation/FederatedSnapshotManager.hpp"
#include "nodes/nodes_print.h"
#include "omnisql/omnisql_print_sql.hpp"
#include "omnisql/omnisql_translate_query.hpp"
#include "optimizer/clauses.h"
#include "rewrite/rewriteHandler.h"
#include "sys/event_list.hpp"
#include "utils/lsyscache.h"

#include <boost/algorithm/string/case_conv.hpp>

#include <sstream>
#include <unordered_map>

extern bool gconf_enable_lf_alp;
extern bool gconf_enable_lf_alp_var_optimization;
extern bool gconf_enable_object_transform;
extern bool gconf_enable_qpc_for_session;
extern bool g_pgtest_ddm;
extern bool gconf_spectrum_enable_lf_data_filters;

namespace ddm_test {

class TestDDMLakeFormationALP : public pg::PgTest {
 public:
  TestDDMLakeFormationALP() {
    {
      auto select_q = reinterpret_cast<Query*>(
          linitial(pg::Analyze(pg::Parse("SELECT * FROM t_dp49994_mutator"))));
      auto table_rte =
          reinterpret_cast<RangeTblEntry*>(linitial(select_q->rtable));
      table_oid_map.insert({"t_dp49994_mutator", table_rte->relid});
    }
    {
      auto select_q = reinterpret_cast<Query*>(linitial(pg::Analyze(
          pg::Parse("SELECT * FROM t_dp49994_double_quotes_mutator"))));
      auto table_rte =
          reinterpret_cast<RangeTblEntry*>(linitial(select_q->rtable));
      table_oid_map.insert(
          {"t_dp49994_double_quotes_mutator", table_rte->relid});
    }
  }

 protected:
  std::unordered_map<std::string, Oid> table_oid_map;

  void RewriteTest(
      std::string user_query, std::string lf_policy_json,
      std::string query_expected_str, bool has_ddm_rewrite,
      std::string table_name = "t_dp49994_mutator" /* main table */);

  static void SetUpTestCase() {
    gconf_enable_lf_alp = true;
    // Enable LF data filters, in order to retrieve ALP policies.
    gconf_spectrum_enable_lf_data_filters = true;
    // This test suite assumes no var-level optimization.
    gconf_enable_lf_alp_var_optimization = false;
    gconf_enable_object_transform = true;
    gconf_enable_qpc_for_session = false;
    g_pgtest_ddm = true;

    pg::RunQuery(R"(
      CREATE TABLE t_dp49994_mutator (
        col_int INT,
        col_text TEXT,
        col_bool BOOLEAN,
        col_super_1 SUPER,
        col_super_2 SUPER
      )
    )");

    // Table with double quoted identifiers
    pg::RunQuery(R"(
      CREATE TABLE t_dp49994_double_quotes_mutator (
        "col_int" INT,
        "col_super""double_quotes" SUPER,
        "col_super""""two_double_quotes" SUPER
      )
    )");
    if (get_user_id_by_name("u_dp49994") == InvalidAclId) {
      // We have another test suite that inherits from this one. This user is
      // never DROPed as it is a master user, so we need to ensure we don't
      // CREATE it twice.
      pg::RunQuery("CREATE USER u_dp49994 WITH PASSWORD 'Test1234!'");
      pg::RunQuery("ALTER USER u_dp49994 with CREATEUSER");
    }
  }

  static void TearDownTestCase() {
    pg::RunQuery("DROP TABLE t_dp49994_mutator");
    pg::RunQuery("DROP TABLE t_dp49994_double_quotes_mutator");
    // Do not DROP u_dp49994 since it's a master user.
  }
};

PG_TEST_F(TestDDMLakeFormationALP, ParsingNestedPaths) {
  Event_info mock_lf_event(EtLakeFormationMockTableResponse);
  mock_lf_event.m_args["remote_db.remote_schema.remote_relation"] =
      R"(text://{
        "AuthorizedColumns": [
            "\"d\"",
            "\"b\".\"level_1_a\".\"level_2_a\"",
            "\"b\".\"level_1_a\"",
            "\"b\".\"level_1_a\".\"level_2_b\"",
            "\"b\".\"level_1_c\".\"level_2_a\"",
            "\"b\".\"level_1_b\"",
            "\"c\".\"something\""
        ]
      })";
  xen_populate_single_event(&mock_lf_event);

  Xen->events->set_event(&mock_lf_event);

  {
    // Make sure GetAuthorizedColumns still works as expected.
    auto [found, columns] = fsmutils::GetAuthorizedColumns(
        "remote_db", "remote_schema", "remote_relation");
    EXPECT_TRUE(found);

    std::vector<std::string> expected_columns = {"d", "b", "c"};

    EXPECT_EQ(expected_columns.size(), columns.size());
    for (auto&& column : expected_columns) {
      EXPECT_TRUE(std::find(columns.begin(), columns.end(), column) !=
                  columns.end());
    }
  }

  {
    // Test GetAuthorizedNestedPaths.
    auto&& [found, paths_map] = fsmutils::GetAuthorizedNestedPaths(
        "remote_db", "remote_schema", "remote_relation");

    EXPECT_TRUE(found);
    EXPECT_EQ(2, paths_map.size());
    EXPECT_TRUE(paths_map.find(R"("b")") != paths_map.end());
    EXPECT_TRUE(paths_map.find(R"("c")") != paths_map.end());

    std::vector<std::string> expected_b = {R"("level_1_a")", R"("level_1_b")",
                                           R"("level_1_c"."level_2_a")"};
    std::vector<std::string> expected_c = {R"("something")"};
    std::sort(expected_b.begin(), expected_b.end());

    std::vector<std::string> actual_b, actual_c;
    std::vector<std::string_view> stack;
    PathSetView(paths_map[R"("b")"]).Export(&actual_b, &stack);
    PathSetView(paths_map[R"("c")"]).Export(&actual_c, &stack);
    std::sort(actual_b.begin(), actual_b.end());

    EXPECT_EQ(expected_b, actual_b);
    EXPECT_EQ(expected_c, actual_c);
  }

  Xen->events->unset_event(EtLakeFormationMockTableResponse);
}

PG_TEST_F(TestDDMLakeFormationALP, ParsingNestedPathsError) {
  Event_info mock_lf_event(EtLakeFormationMockTableResponse);
  mock_lf_event.m_args["remote_db.remote_schema.remote_relation"] =
      R"(text://{
        "AuthorizedColumns": [
            "malformed_entry_without_quotes"
        ]
      })";
  xen_populate_single_event(&mock_lf_event);

  Xen->events->set_event(&mock_lf_event);

  try {
    fsmutils::GetAuthorizedColumns("remote_db", "remote_schema",
                                   "remote_relation");
    FAIL() << "Expected ParseError";
  } catch (ParseError const& err) {
    EXPECT_THAT(
        err.what(),
        ::testing::HasSubstr(::awsapi::datacatalog::keys::AUTHORIZED_COLUMNS));
  } catch (...) {
    FAIL() << "Wring exception";
  }

  Xen->events->unset_event(EtLakeFormationMockTableResponse);
}

void TestDDMLakeFormationALP::RewriteTest(std::string user_query,
                                          std::string lf_policy_json,
                                          std::string query_expected_str,
                                          bool has_ddm_rewrite,
                                          std::string table_name) {
  Event_info mock_lf_event(EtLakeFormationMockTableResponse);
  const std::string db_name = "dev";
  const std::string ns_name = "pgtest_testcase_schema";
  auto relation_fmt =
      boost::format("%1%.%2%.%3%") % db_name % ns_name % table_name;
  mock_lf_event.m_args[relation_fmt.str().c_str()] = lf_policy_json.c_str();
  xen_populate_single_event(&mock_lf_event);
  Xen->events->set_event(&mock_lf_event);

  Event_info mock_lf_is_localized_event(EtLakeFormationMockRTELocalViaLF);
  mock_lf_is_localized_event.m_args["oid"] =
      std::to_string(table_oid_map[table_name]).c_str();
  mock_lf_is_localized_event.m_args["db_name"] = db_name;
  mock_lf_is_localized_event.m_args["ns_name"] = ns_name;
  mock_lf_is_localized_event.m_args["rel_name"] = table_name;
  xen_populate_single_event(&mock_lf_is_localized_event);
  Xen->events->set_event(&mock_lf_is_localized_event);

  auto current_id = GetUserId();
  auto target_id = get_user_id_by_name("u_dp49994");
  SetUserId(target_id);

  auto query_fmt = boost::format("SELECT * FROM %1%") % table_name;

  RangeTblEntry* main_table_rte = reinterpret_cast<RangeTblEntry*>(
      linitial(reinterpret_cast<Query*>(
                   linitial(pg::Analyze(pg::Parse(query_fmt.str()))))
                   ->rtable));

  ASSERT_EQ(has_ddm_rewrite, HasDDMRewrite(main_table_rte));
  if (has_ddm_rewrite) {
    Query* query =
        reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(user_query))));
    auto query_rewritten =
        ddm::RewriteQuery(copyObject(query), false /* log_applied_policy */)
            .rewrite;
    auto query_rewritten_omni =
        omnisql::PrintSQL(omnisql::TranslateQuery(query_rewritten));

    Query* query_expected = reinterpret_cast<Query*>(
        linitial(pg::Analyze(pg::Parse(query_expected_str))));
    std::string query_expected_omni =
        omnisql::PrintSQL(omnisql::TranslateQuery(query_expected));

    ASSERT_EQ(query_expected_omni, query_rewritten_omni);
  }

  SetUserId(current_id);

  Xen->events->unset_event(EtLakeFormationMockTableResponse);
  Xen->events->unset_event(EtLakeFormationMockRTELocalViaLF);
}

PG_TEST_F(TestDDMLakeFormationALP, SimpleRewrite) {
  RewriteTest(
      /* user_query */
      std::string(R"(
        SELECT
          *
        FROM
          t_dp49994_mutator)"),
      /* lf_policy_json */
      std::string(R"(text://{
        "AuthorizedColumns": [
            "\"col_super_1\".\"level_1_a\".\"level_2_a\"",
            "\"col_super_1\".\"level_1_a\"",
            "\"col_super_1\".\"level_1_a\".\"level_2_b\"",
            "\"col_super_1\".\"level_1_c\".\"level_2_a\"",
            "\"col_super_1\".\"level_1_b\"",
            "\"col_super_2\".\"something\""
        ],
        "CombinedRowFilter": "TRUE"
      })"),
      /* query_expected_str */
      std::string(R"(
        SELECT
          "col_super_1",
          "col_super_2"
        FROM
          (
            SELECT
              "col_int",
              "col_text",
              "col_bool",
              OBJECT_TRANSFORM(
                "col_super_1"
                KEEP
                  '"level_1_a"',
                  '"level_1_b"',
                  '"level_1_c"."level_2_a"'
              ) AS "col_super_1",
              OBJECT_TRANSFORM(
                "col_super_2"
                KEEP
                  '"something"'
              ) AS "col_super_2"
            FROM
              "t_dp49994_mutator"
          ) AS "t_dp49994_mutator"
      )"),
      true /* has_ddm_rewrite */);
}

PG_TEST_F(TestDDMLakeFormationALP, NoRewrite) {
  RewriteTest(
      /* user_query */
      std::string(R"(
        SELECT
          *
        FROM
          t_dp49994_mutator)"),
      /* lf_policy_json */
      std::string(R"(text://{
        "AuthorizedColumns": [
            "\"col_super_1\"",
            "\"col_super_2\""
        ],
        "CombinedRowFilter": "TRUE"
      })"),
      /* query_expected_str */
      std::string(), false /* has_ddm_rewrite */);
}

PG_TEST_F(TestDDMLakeFormationALP, InjectionAttackAttempt) {
  RewriteTest(
      /* user_query */
      std::string(R"(
        SELECT
          *
        FROM
          t_dp49994_mutator)"),
      /* lf_policy_json */
      std::string(R"(text://{
        "AuthorizedColumns": [
            "\"col_super_1\".\"current_user: ' || CURRENT_USER || ' end\""
        ],
        "CombinedRowFilter": "TRUE"
      })"),
      /* query_expected_str */
      std::string(R"(
        SELECT
          "col_super_1"
        FROM
          (
            SELECT
              "col_int",
              "col_text",
              "col_bool",
              OBJECT_TRANSFORM(
                "col_super_1"
                KEEP
                  '"current_user: \' || CURRENT_USER || \' end"'
              ) AS "col_super_1",
              "col_super_2"
            FROM
              "t_dp49994_mutator"
          ) AS "t_dp49994_mutator"
      )"),
      true /* has_ddm_rewrite */);
}

PG_TEST_F(TestDDMLakeFormationALP, DoubleQuotesInPaths) {
  RewriteTest(
      /* user_query */
      std::string(R"(
        SELECT
          *
        FROM
          t_dp49994_double_quotes_mutator)"),
      /* lf_policy_json */
      std::string(R"(text://{
        "AuthorizedColumns": [
            "\"col_super\"\"double_quotes\".\"level_1\"\"double_quotes\"",
            "\"col_super\"\"\"\"two_double_quotes\".\"level_1\"\"\"\"two_double_quotes\""
        ],
        "CombinedRowFilter": "TRUE"
      })"),
      /* query_expected_str */
      std::string(R"(
        SELECT
          "col_super""double_quotes",
          "col_super""""two_double_quotes"
        FROM
          (
            SELECT
              "col_int",
              OBJECT_TRANSFORM(
                "col_super""double_quotes"
                KEEP
                  '"level_1"double_quotes"'
              ) AS "col_super""double_quotes",
              OBJECT_TRANSFORM(
                "col_super""""two_double_quotes"
                KEEP
                  '"level_1""two_double_quotes"'
              ) AS "col_super""""two_double_quotes"
            FROM
              "t_dp49994_double_quotes_mutator"
          ) AS "t_dp49994_double_quotes_mutator"
      )"),
      true /* has_ddm_rewrite */,
      "t_dp49994_double_quotes_mutator" /* table_name */);
}

PG_TEST_F(TestDDMLakeFormationALP, VariableLevelALPSelect) {
  bool old_guc = gconf_enable_lf_alp_var_optimization;
  gconf_enable_lf_alp_var_optimization = true;
  SCOPE_EXIT_GUARD({ gconf_enable_lf_alp_var_optimization = old_guc; });

  RewriteTest(
      /* user_query */
      std::string(R"(
        SELECT
          "col_super_1"."level_1_a"."authorized",
          "col_super_2"
        FROM
          t_dp49994_mutator)"),
      /* lf_policy_json */
      std::string(R"(text://{
        "AuthorizedColumns": [
            "\"col_super_1\".\"level_1_a\"",
            "\"col_super_2\".\"level_1_b\""
        ],
        "CombinedRowFilter": "TRUE"
      })"),
      /* query_expected_str */
      std::string(R"(
        SELECT
          "col_super_1"."level_1_a"."authorized",
          "col_super_2"
        FROM
          (
            SELECT
              "col_int",
              "col_text",
              "col_bool",
              "col_super_1",
              OBJECT_TRANSFORM(
                "col_super_2"
                KEEP
                  '"level_1_b"'
              ) AS "col_super_2"
            FROM
              "t_dp49994_mutator"
          ) AS "t_dp49994_mutator"
      )"),
      true /* has_ddm_rewrite */);
}

PG_TEST_F(TestDDMLakeFormationALP, VariableLevelALPWhere) {
  bool old_guc = gconf_enable_lf_alp_var_optimization;
  gconf_enable_lf_alp_var_optimization = true;
  SCOPE_EXIT_GUARD({ gconf_enable_lf_alp_var_optimization = old_guc; });

  RewriteTest(
      /* user_query */
      std::string(R"(
        SELECT
          1 AS result
        FROM
          t_dp49994_mutator
        WHERE
          "col_super_1"."level_1_a" = json_parse('{ "a": 5 }')
      )"),
      /* lf_policy_json */
      std::string(R"(text://{
        "AuthorizedColumns": [
            "\"col_super_1\".\"level_1_a\".\"level_2_b\""
        ],
        "CombinedRowFilter": "TRUE"
      })"),
      /* query_expected_str */
      std::string(R"(
        SELECT
          1::INT AS result
        FROM
          (
            SELECT
              "col_int",
              "col_text",
              "col_bool",
              OBJECT_TRANSFORM(
                "col_super_1"
                KEEP
                  '"level_1_a"."level_2_b"'
              ) AS "col_super_1",
              "col_super_2"
            FROM
              "t_dp49994_mutator"
          ) AS "t_dp49994_mutator"
        WHERE
          "col_super_1"."level_1_a" = json_parse('{ "a": 5 }')
      )"),
      true /* has_ddm_rewrite */);
}

class TestDDMLakeFormationPreALP : public pg::PgTest {
 protected:
  static void SetUpTestCase() {
    gconf_enable_lf_alp = false;
    gconf_enable_object_transform = true;
  }
};

/// Make sure GetAuthorizedColumns still works as expected when the LF-ALP GUC
/// is OFF.
PG_TEST_F(TestDDMLakeFormationPreALP, ColumnsParsing) {
  Event_info mock_lf_event(EtLakeFormationMockTableResponse);
  mock_lf_event.m_args["remote_db.remote_schema.remote_relation"] =
      R"(text://{
        "AuthorizedColumns": [
            "column_1",
            "column_2.something",
            "\"column_3\"",
            "\"column_5\".\"other\""
        ]
      })";
  xen_populate_single_event(&mock_lf_event);

  Xen->events->set_event(&mock_lf_event);

  // Make sure GetAuthorizedColumns still works as expected.
  auto [found, columns] = fsmutils::GetAuthorizedColumns(
      "remote_db", "remote_schema", "remote_relation");
  EXPECT_TRUE(found);
  std::vector<std::string> expected_columns = {"column_1", "column_2.something",
                                               "\"column_3\"",
                                               "\"column_5\".\"other\""};

  EXPECT_EQ(expected_columns.size(), columns.size());
  for (auto&& column : expected_columns) {
    EXPECT_TRUE(std::find(columns.begin(), columns.end(), column) !=
                columns.end());
  }

  Xen->events->unset_event(EtLakeFormationMockTableResponse);
}

class TestDDMLakeFormationPostALP : public pg::PgTest {
 protected:
  static void SetUpTestCase() {
    gconf_enable_lf_alp = true;
    gconf_enable_object_transform = true;
  }
};

/// Make sure GetAuthorizedColumns still works as expected when the LF-ALP GUC
/// is ON.
PG_TEST_F(TestDDMLakeFormationPostALP, ColumnsParsing) {
  Event_info mock_lf_event(EtLakeFormationMockTableResponse);
  mock_lf_event.m_args["remote_db.remote_schema.remote_relation"] =
      R"(text://{
        "AuthorizedColumns": [
            "\"column_1\"",
            "\"column_2\".\"something\"",
            "\"column_2\".\"something_else\"",
            "\"column_3\"",
            "\"column_5\".\"other\"",
            "\"column_5.other\""
        ]
      })";
  xen_populate_single_event(&mock_lf_event);

  Xen->events->set_event(&mock_lf_event);

  // Make sure GetAuthorizedColumns still works as expected.
  auto [found, columns] = fsmutils::GetAuthorizedColumns(
      "remote_db", "remote_schema", "remote_relation");
  EXPECT_TRUE(found);
  std::vector<std::string> expected_columns = {
      "column_1", "column_2", "column_3", "column_5", "column_5.other",
  };

  EXPECT_EQ(expected_columns.size(), columns.size());
  for (auto&& column : expected_columns) {
    EXPECT_TRUE(std::find(columns.begin(), columns.end(), column) !=
                columns.end());
  }

  Xen->events->unset_event(EtLakeFormationMockTableResponse);
}

using alp_decision = PathSet::PathMembership;

struct VarLevelALPParam {
  std::string case_name;
  std::string user_query_fmt;
  // Following are overrides for how to map pathset membership to ALP action for
  // the particular test case. For most cases, these are default.
  alp_decision override_authorized = alp_decision::kContainedFully;
  alp_decision override_error = alp_decision::kNotContained;
  alp_decision override_obj_transform = alp_decision::kContainedPartially;
};

class TestDDMVarLevelLakeFormationALP
    : public TestDDMLakeFormationALP,
      public ::testing::WithParamInterface<VarLevelALPParam> {
 public:
  static void SetUpTestCase() {
    TestDDMLakeFormationALP::SetUpTestCase();
    pg::RunQuery(R"(
      CREATE TABLE t_dp54610_join (
        col_int INT
      )
    )");
    pg::RunQuery("CREATE USER u_dp54610 WITH PASSWORD 'Test1234!'");
    pg::RunQuery("ALTER USER u_dp54610 with CREATEUSER");
    gconf_enable_lf_alp_var_optimization = true;
  }
  static void TearDownTestCase() {
    pg::RunQuery("DROP TABLE t_dp54610_join");
    // Do not DROP u_dp54610 since it's a master user.
    TestDDMLakeFormationALP::TearDownTestCase();
  }

  static void ValidateALPPolicyCorrectness(
      std::optional<ddm::LFALPPolicy> lf_alp_policy,
      const std::string& nested_path) {
    EXPECT_TRUE(lf_alp_policy);
    EXPECT_THAT(*lf_alp_policy, ::testing::SizeIs(1)) << nested_path;
    const std::string top_level_name{R"("col_super_1")"};
    EXPECT_THAT(
        *lf_alp_policy,
        ::testing::Contains(::testing::Key(::testing::Eq(top_level_name))))
        << nested_path;
    std::vector<std::string> components;
    std::vector<std::string_view> stack;
    PathSetView((*lf_alp_policy)[top_level_name]).Export(&components, &stack);
    EXPECT_THAT(components, ::testing::ElementsAre(R"("transform"."nested")",
                                                   R"("authorized")"))
        << nested_path;
  }
};

PG_TEST_P(TestDDMVarLevelLakeFormationALP, VarLevelALP) {
  Event_info mock_lf_event(EtLakeFormationMockTableResponse);
  mock_lf_event.m_args["dev.pgtest_testcase_schema.t_dp49994_mutator"] =
      R"(text://{
        "AuthorizedColumns": [
            "\"col_super_1\".\"authorized\"",
            "\"col_super_1\".\"transform\".\"nested\""
        ],
        "CombinedRowFilter": "TRUE"
      })";
  xen_populate_single_event(&mock_lf_event);
  Xen->events->set_event(&mock_lf_event);

  Event_info mock_lf_is_localized_event(EtLakeFormationMockRTELocalViaLF);
  mock_lf_is_localized_event.m_args["oid"] =
      std::to_string(table_oid_map["t_dp49994_mutator"]).c_str();
  mock_lf_is_localized_event.m_args["db_name"] = "dev";
  mock_lf_is_localized_event.m_args["ns_name"] = "pgtest_testcase_schema";
  mock_lf_is_localized_event.m_args["rel_name"] = "t_dp49994_mutator";
  xen_populate_single_event(&mock_lf_is_localized_event);
  Xen->events->set_event(&mock_lf_is_localized_event);

  auto current_id = GetUserId();
  auto target_id = get_user_id_by_name("u_dp54610");
  SetUserId(target_id);

  for (alp_decision testcase :
       {alp_decision::kContainedFully, alp_decision::kContainedPartially,
        alp_decision::kNotContained}) {
    std::string nested_path;
    switch (testcase) {
      case alp_decision::kContainedFully:
        nested_path = R"("authorized")";
        break;
      case alp_decision::kContainedPartially:
        nested_path = R"("transform")";
        break;
      case alp_decision::kNotContained:
        nested_path = R"("denied")";
        break;
    }
    std::string user_query =
        boost::str(boost::format(GetParam().user_query_fmt) %
                   std::string(R"("col_super_1")") % nested_path);

    Query* query =
        reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(user_query))));

    bool error = false;
    ddm::ObjTransformAlpColumnsRelationMap obj_transform_alp_columns_rel_map;
    try {
      obj_transform_alp_columns_rel_map = ddm::WalkVarLevelALP(query);
    } catch (std::exception& e) {
      std::string error_message = e.what();
      FlushErrorState();
      boost::algorithm::to_lower(error_message);
      if (error_message.find("permission")) error = true;
    }

    // Respect overrides.
    switch (testcase) {
      case alp_decision::kContainedFully:
        testcase = GetParam().override_authorized;
        break;
      case alp_decision::kContainedPartially:
        testcase = GetParam().override_obj_transform;
        break;
      case alp_decision::kNotContained:
        testcase = GetParam().override_error;
        break;
    }
    ddm::ObjTransformAlpColumnsCtx alp_ctx =
        obj_transform_alp_columns_rel_map[table_oid_map["t_dp49994_mutator"]];
    switch (testcase) {
      case alp_decision::kContainedFully:
        EXPECT_THAT(error, ::testing::IsFalse()) << nested_path;
        ValidateALPPolicyCorrectness(alp_ctx.lf_alp_policy, nested_path);
        EXPECT_THAT(alp_ctx.obj_transform_alp_columns, ::testing::IsEmpty())
            << nested_path;
        break;
      case alp_decision::kContainedPartially:
        EXPECT_THAT(error, ::testing::IsFalse()) << nested_path;
        ValidateALPPolicyCorrectness(alp_ctx.lf_alp_policy, nested_path);
        EXPECT_THAT(alp_ctx.obj_transform_alp_columns, ::testing::SizeIs(1))
            << nested_path;
        EXPECT_THAT(alp_ctx.obj_transform_alp_columns,
                    ::testing::Contains(std::string_view("col_super_1")))
            << nested_path;
        break;
      case alp_decision::kNotContained:
        EXPECT_THAT(error, ::testing::IsTrue()) << nested_path;
        break;
    }
  }

  SetUserId(current_id);

  Xen->events->unset_event(EtLakeFormationMockTableResponse);
  Xen->events->unset_event(EtLakeFormationMockRTELocalViaLF);
}

VarLevelALPParam var_level_optimization_cases[] = {
    VarLevelALPParam{
        "Simple SELECT",
        R"(
          SELECT %1%.%2%
          FROM t_dp49994_mutator
        )",
    },
    VarLevelALPParam{
        "Simple WHERE",
        R"(
          SELECT 1
          FROM t_dp49994_mutator
          WHERE %1%.%2% = 'hello'
      )",
    },
    VarLevelALPParam{
        "ORDER BY",
        R"(
          SELECT 1
          FROM t_dp49994_mutator
          ORDER BY %1%.%2%::INT
        )",
    },
    VarLevelALPParam{
        "GROUP BY",
        R"(
          SELECT 1
          FROM t_dp49994_mutator
          GROUP BY %1%.%2%::INT
        )",
    },
    VarLevelALPParam{
        // Where the relation of interest is the second one.
        "Multiple relations in FROM",
        R"(
          SELECT
              A.col_int + B.%1%.%2%::INT
          FROM
              t_dp54610_join A,
              t_dp49994_mutator B
        )",
    },
    VarLevelALPParam{
        "JOIN",
        R"(
          SELECT 1
          FROM t_dp49994_mutator
          JOIN t_dp54610_join ON (
            t_dp49994_mutator.%1%.%2%::INT = t_dp54610_join.col_int
          )
        )",
        // In JOIN clause variable is presented without nested path. So the case
        // when request authorized path is equivalent to obj_transform.
        alp_decision::kContainedPartially /* override_authorized */,
    },
    VarLevelALPParam{
        "Sub-SELECT",
        R"(
          SELECT 1
          FROM t_dp49994_mutator
          WHERE (
            t_dp49994_mutator.%1%.%2% IN (
              SELECT t_dp54610_join.col_int FROM t_dp54610_join
            )
          )
        )",
    },
    VarLevelALPParam{
        "In expression",
        R"(
          SELECT %1%.%2% * 10
          FROM t_dp49994_mutator
        )",
    },
    VarLevelALPParam{
        "CTE with variable in CTE",
        R"(
          WITH cte
          AS
            (
              SELECT
                %1%.%2% AS m
              FROM t_dp49994_mutator
            )
          SELECT m FROM cte;
        )",
    },
    VarLevelALPParam{
        "CTE with variable in outer query",
        R"(
          WITH cte
          AS
            (
              SELECT
                %1% AS m
              FROM t_dp49994_mutator
            )
          SELECT m.%2% FROM cte;
        )",
        // Here the variable reference in CTE will trigger column to be
        // obj_transformed. Because CTE itself (and n RTE) is not LF-ALPed, we
        // don't check anything for the outer reference.
        alp_decision::kContainedPartially /* override_authorized */,
        alp_decision::kContainedPartially /* override_error */,
    },
    VarLevelALPParam{
        "UNPIVOT",
        R"(
          SELECT
            value
          FROM UNPIVOT(
            SELECT
              c.%1%.%2%
            AS cn
            FROM t_dp49994_mutator c
          ) OVER (
            cn AS value
          )
        )",
    },
    VarLevelALPParam{
        "UNNEST",
        R"(
          SELECT
            address
          FROM UNNEST(
            SELECT
              c.%1%.%2% AS cd
            FROM t_dp49994_mutator c
          ) OVER (
            cd AS address
          )
        )",
    },
    VarLevelALPParam{
        "UPDATE",
        R"(
          UPDATE t_dp49994_mutator
          SET col_int = (%1%.%2% + 1)::INT
        )",
    },
    VarLevelALPParam{
        "DELETE",
        R"(
          DELETE FROM t_dp49994_mutator
          WHERE %1%.%2% = 'hello'
        )",
    },
};

INSTANTIATE_TEST_SUITE_P(
    VarLevelALPTests, TestDDMVarLevelLakeFormationALP,
    ::testing::ValuesIn(var_level_optimization_cases),
    [](const testing::TestParamInfo<TestDDMVarLevelLakeFormationALP::ParamType>&
           info) {
      std::string name = info.param.case_name;
      std::replace_if(
          name.begin(), name.end(), [](char c) { return !std::isalnum(c); },
          '_');
      return name;
    });

}  // namespace ddm_test
