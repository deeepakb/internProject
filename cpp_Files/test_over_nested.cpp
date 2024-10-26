/// Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.

// These two includes must go first and in this order.
// clang-format off
#include <gtest/gtest.h>
#include "pg/test/pg_test.hpp"
#include <gmock/gmock.h>
// clang-format on

#include "ddm/ddm_over_nested.hpp"
#include "ddm/ddm_policy.hpp"
#include "ddm/rewriter.h"
#include "omnisql/omnisql_print_sql.hpp"
#include "omnisql/omnisql_translate_query.hpp"
#include "pg2volt/volt_cinterface.h"
#include "rewrite/rewriteHandler.h"
#include "utils/lsyscache.h"

extern bool gconf_ddm_enable_over_super_paths;
extern bool gconf_enable_object_transform;
extern bool gconf_enable_qpc_for_session;
extern bool g_pgtest_ddm;

namespace ddm_test {

class TestDDMOverNested : public pg::PgTest {
 public:
  TestDDMOverNested() {
    main_table_oid = OidFromName("t_dp50000_target");
    main_view_oid = OidFromName("v_dp50000_target");
    main_lbv_oid = OidFromName("lbv_dp50000_target");
  }

 protected:
  Oid main_table_oid;
  Oid main_view_oid;
  Oid main_lbv_oid;

  void RewriteTest(const char* user_query_str, const char* policy_arg_type,
                   const char* policy_expr, const char* attach_in,
                   const char* attach_out, const char* query_expected_str);

  void InPlaceNestedVarsWalkerTest(const char* query_str,
                                   std::vector<std::string> attachment_targets,
                                   ddm::NestedVarMap expected);

  void NavPathOptimizationRewrite(const char* user_query_str,
                                  const char* query_expected_complex_str,
                                  const char* query_expected_optimized_str);

  ddm::MaskMap GenerateExpectedMaskMap(
      std::unordered_map<
          std::string, std::vector<std::pair<int, std::optional<std::string>>>>
          input);

  std::set<ddm::ParsedPolicyAttachment> GenerateParsedAttachments(
      std::vector<std::pair<
          int, std::vector<std::pair<std::string, std::optional<std::string>>>>>
          input);

  bool MaskMapEqual(const ddm::MaskMap& left, const ddm::MaskMap& right);

  const std::string kColSuper1 = "col_super_1";

  Oid OidFromName(const char* name) {
    auto query_str = boost::str(boost::format("SELECT * FROM %1%") % name);
    auto select_q =
        reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(query_str))));
    auto rte = reinterpret_cast<RangeTblEntry*>(linitial(select_q->rtable));
    return GetRelationRelid(rte);
  }

  static void SetUpTestCase() {
    gconf_ddm_enable_over_super_paths = true;
    gconf_enable_object_transform = true;
    gconf_enable_qpc_for_session = false;
    g_pgtest_ddm = true;

    pg::RunQuery(R"(
      CREATE TABLE t_dp50000_target (
        col_int INT,
        col_text TEXT,
        col_bool BOOLEAN,
        col_super_1 SUPER,
        col_super_2 SUPER
      )
    )");
    pg::RunQuery(R"(
      CREATE TABLE t_dp50000_join (
        col_int INT
      )
    )");
    pg::RunQuery(R"(
      CREATE VIEW v_dp50000_target AS
      SELECT
        col_int,
        col_text,
        col_bool,
        (col_super_1 * 2)::SUPER AS col_super_1,
        col_super_2
      FROM t_dp50000_target
    )");
    pg::RunQuery(R"(
      CREATE VIEW v_dp50000_ref_nested AS
      SELECT
        col_super_1.nested AS col_nested
      FROM t_dp50000_target
    )");
    pg::RunQuery(R"(
      CREATE VIEW lbv_dp50000_ref_nested AS
      SELECT
        col_super_1.nested AS col_nested
      FROM pgtest_testcase_schema.t_dp50000_target
      WITH NO SCHEMA BINDING
    )");
    pg::RunQuery(R"(
      CREATE VIEW lbv_dp50000_target AS
      SELECT
        col_int,
        col_text,
        col_bool,
        (col_super_1 * 3)::SUPER AS col_super_1,
        col_super_2
      FROM pgtest_testcase_schema.t_dp50000_target
      WITH NO SCHEMA BINDING
    )");
    pg::RunQuery(R"(
      CREATE FUNCTION udf_dp50000 (INT, INT)
        RETURNS INT
      STABLE
      AS $$
        SELECT CASE WHEN $1 > $2 THEN $1
          ELSE $2
        END
      $$ LANGUAGE SQL
    )");
    pg::RunQuery("CREATE USER u_dp50000 WITH PASSWORD 'Test1234!'");
    pg::RunQuery("ALTER USER u_dp50000 with CREATEUSER");
    pg::RunQuery("CREATE ROLE r_dp50000");
    pg::RunQuery("GRANT ROLE r_dp50000 to u_dp50000");
  }

  static void TearDownTestCase() {
    pg::RunQuery("DROP VIEW lbv_dp50000_target");
    pg::RunQuery("DROP VIEW lbv_dp50000_ref_nested");
    pg::RunQuery("DROP VIEW v_dp50000_target");
    pg::RunQuery("DROP VIEW v_dp50000_ref_nested");
    pg::RunQuery("DROP TABLE t_dp50000_target");
    pg::RunQuery("DROP TABLE t_dp50000_join");
    pg::RunQuery("DROP FUNCTION udf_dp50000(INT, INT) CASCADE");
    pg::RunQuery("DROP ROLE r_dp50000 FORCE");
    // Do not DROP u_dp50000 since it's a master user.
  }
};

void TestDDMOverNested::RewriteTest(const char* user_query_str,
                                    const char* policy_arg_type,
                                    const char* policy_expr,
                                    const char* attach_in,
                                    const char* attach_out,
                                    const char* query_expected_str) {
  auto current_id = GetUserId();
  auto target_id = get_user_id_by_name("u_dp50000");

  pg::RunQuery(boost::str(boost::format(R"SQL(
    CREATE MASKING POLICY mp_dp50000
    WITH(a %1%) USING ( %2% )
  )SQL") % policy_arg_type %
                          policy_expr));
  pg::RunQuery(boost::str(boost::format(R"SQL(
    ATTACH MASKING POLICY mp_dp50000
    ON t_dp50000_target(%1%) USING (%2%)
    TO ROLE r_dp50000
  )SQL") % attach_out % attach_in));
  SetUserId(target_id);

  Query* query = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(std::string(user_query_str)))));
  auto query_rewritten =
      ddm::RewriteQuery(copyObject(query), false /* log_applied_policy */)
          .rewrite;
  auto query_rewritten_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(query_rewritten));

  Query* query_expected = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(std::string(query_expected_str)))));
  std::string query_expected_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(query_expected));

  EXPECT_EQ(query_expected_omni, query_rewritten_omni);

  SetUserId(current_id);
  pg::RunQuery(R"SQL(DROP MASKING POLICY mp_dp50000 CASCADE)SQL");
}

PG_TEST_F(TestDDMOverNested, RewriteSimpleInNested) {
  RewriteTest(
      /* user_query */
      R"SQL(
        SELECT
          col_super_2,
          col_bool
        FROM t_dp50000_target
      )SQL",
      "SUPER",                   /* policy_arg_type */
      "a + 10",                  /* policy_expr */
      "col_super_1.nested.path", /* attach_in */
      "col_super_2",             /* attach_out */
      /* query_expected_str */
      R"SQL(
        SELECT
          col_super_2,
          col_bool
        FROM
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              col_super_1,
              (col_super_1.nested.path + 10) AS col_super_2
            FROM
              t_dp50000_target
          ) AS t_dp50000_target
      )SQL");
}

PG_TEST_F(TestDDMOverNested, RewriteSimpleInNestedPlusNested) {
  RewriteTest(
      /* user_query */
      R"SQL(
        SELECT
          col_super_2,
          col_bool
        FROM t_dp50000_target
      )SQL",
      "SUPER",                   /* policy_arg_type */
      "a.another.level + 10",    /* policy_expr */
      "col_super_1.nested.path", /* attach_in */
      "col_super_2",             /* attach_out */
      /* query_expected_str */
      R"SQL(
        SELECT
          col_super_2,
          col_bool
        FROM
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              col_super_1,
              (col_super_1.nested.path.another.level + 10) AS col_super_2
            FROM
              t_dp50000_target
          ) AS t_dp50000_target
      )SQL");
}

PG_TEST_F(TestDDMOverNested, RewriteSimpleOutNested) {
  RewriteTest(
      /* user_query */
      R"SQL(
        SELECT
          col_super_2,
          col_bool
        FROM t_dp50000_target
      )SQL",
      "SUPER",                   /* policy_arg_type */
      "a + 10",                  /* policy_expr */
      "col_super_1",             /* attach_in */
      "col_super_2.nested.path", /* attach_out */
      /* query_expected_str */
      R"SQL(
        SELECT
          col_super_2,
          col_bool
        FROM
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              col_super_1,
              OBJECT_TRANSFORM(
                col_super_2
                SET
                    '"nested"."path"', (col_super_1 + 10)
              ) AS col_super_2
            FROM
              t_dp50000_target
          ) AS t_dp50000_target
      )SQL");
}

PG_TEST_F(TestDDMOverNested, RewriteSimpleOutNestedPlusNested) {
  RewriteTest(
      /* user_query */
      R"SQL(
        SELECT
          col_super_2,
          col_bool
        FROM t_dp50000_target
      )SQL",
      "SUPER",                   /* policy_arg_type */
      "a.another.level + 10",    /* policy_expr */
      "col_super_1",             /* attach_in */
      "col_super_2.nested.path", /* attach_out */
      /* query_expected_str */
      R"SQL(
        SELECT
          col_super_2,
          col_bool
        FROM
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              col_super_1,
              OBJECT_TRANSFORM(
                col_super_2
                SET
                    '"nested"."path"', (col_super_1.another.level + 10)
              ) AS col_super_2
            FROM
              t_dp50000_target
          ) AS t_dp50000_target
      )SQL");
}

PG_TEST_F(TestDDMOverNested, RewriteSimpleInOutNested) {
  RewriteTest(
      /* user_query */
      R"SQL(
        SELECT
          col_super_1
        FROM t_dp50000_target
      )SQL",
      "INT",                     /* policy_arg_type */
      "a + 10",                  /* policy_expr */
      "col_super_1.nested.path", /* attach_in */
      "col_super_1.nested.path", /* attach_out */
      /* query_expected_str */
      R"SQL(
        SELECT
          col_super_1
        FROM
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              OBJECT_TRANSFORM(
                col_super_1
                SET
                    '"nested"."path"', (col_super_1.nested.path::INT + 10)
              ) AS col_super_1,
              col_super_2
            FROM
              t_dp50000_target
          ) AS t_dp50000_target
      )SQL");
}

PG_TEST_F(TestDDMOverNested, RewriteUPDATEInOutNestedObj) {
  RewriteTest(
      /* user_query */
      R"SQL(
        UPDATE t_dp50000_target
        SET col_int = col_super_1.nested.path::INT
      )SQL",
      "INT",                     /* policy_arg_type */
      "a + 10",                  /* policy_expr */
      "col_super_1.nested.path", /* attach_in */
      "col_super_1.nested.path", /* attach_out */
      /* query_expected_str */
      R"SQL(
        UPDATE t_dp50000_target
        SET col_int =
          GET_OBJECT_PATH(
            GET_OBJECT_PATH(
              OBJECT_TRANSFORM(
                col_super_1
                SET
                    '"nested"."path"', (col_super_1.nested.path::INT + 10)
              ),
              'nested'::VARCHAR
            ),
            'path'::VARCHAR
          )::INT
      )SQL");
}

PG_TEST_F(TestDDMOverNested, RewriteUPDATEInOutNestedArray) {
  RewriteTest(
      /* user_query */
      R"SQL(
        UPDATE t_dp50000_target
        SET col_int = col_super_1.very.nested[2].path::INT
      )SQL",
      "SUPER",            /* policy_arg_type */
      "a + 10",           /* policy_expr */
      "col_super_1.very", /* attach_in */
      "col_super_1.very", /* attach_out */
      /* query_expected_str */
      R"SQL(
        UPDATE t_dp50000_target
        SET col_int =
          GET_OBJECT_PATH(
            GET_ARRAY_PATH(
              GET_OBJECT_PATH(
                GET_OBJECT_PATH(
                  OBJECT_TRANSFORM(
                    col_super_1
                    SET
                        '"very"', (col_super_1.very::SUPER + 10)
                  ),
                  'very'::VARCHAR
                ),
                'nested'::VARCHAR
              ),
              2::INT
            ),
            'path'::VARCHAR
          )::INT
      )SQL");
}

PG_TEST_F(TestDDMOverNested, RewriteComplex) {
  auto current_id = GetUserId();
  auto target_id = get_user_id_by_name("u_dp50000");

  pg::RunQuery(R"SQL(
    CREATE MASKING POLICY mp_dp50000_redact_ssn
    WITH(age INT, state TEXT, ssn TEXT)
    USING (
      CASE
        WHEN age <= 18 THEN 'XXX-XX-XXXX'
        WHEN state != 'CA' THEN 'XXX-XX-'||SUBSTRING(ssn::TEXT FROM 8 FOR 4)
        ELSE ssn
      END
    )
  )SQL");
  pg::RunQuery(R"SQL(
    CREATE MASKING POLICY mp_dp50000_redact_salary
    WITH(salary FLOAT)
    USING (
      FLOOR(salary / 10) * 10
    )
  )SQL");
  pg::RunQuery(R"SQL(
    CREATE MASKING POLICY mp_dp50000_redact_name
    WITH(name TEXT)
    USING ( '<name>'::TEXT )
  )SQL");

  pg::RunQuery(R"SQL(
    ATTACH MASKING POLICY mp_dp50000_redact_ssn
    ON t_dp50000_target(col_super_1.person.ssn)
    USING (
      col_super_1.person.age,
      col_super_1.person.state,
      col_super_1.person.ssn
    )
    TO ROLE r_dp50000
  )SQL");
  pg::RunQuery(R"SQL(
    ATTACH MASKING POLICY mp_dp50000_redact_salary
    ON t_dp50000_target(col_super_1.person.salary)
    TO ROLE r_dp50000
  )SQL");
  pg::RunQuery(R"SQL(
    ATTACH MASKING POLICY mp_dp50000_redact_name
    ON t_dp50000_target(col_super_1.person.name)
    TO ROLE r_dp50000
  )SQL");
  pg::RunQuery(R"SQL(
    ATTACH MASKING POLICY mp_dp50000_redact_name
    ON t_dp50000_target(col_text)
    TO ROLE r_dp50000
  )SQL");
  SetUserId(target_id);

  const char* user_query_str = R"SQL(
    SELECT
      col_super_1,
      col_text
    FROM t_dp50000_target
  )SQL";

  Query* query = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(std::string(user_query_str)))));
  auto query_rewritten =
      ddm::RewriteQuery(copyObject(query), false /* log_applied_policy */)
          .rewrite;
  auto query_rewritten_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(query_rewritten));

  const char* query_expected_str = R"SQL(
    SELECT
      col_super_1,
      col_text
    FROM
      (
        SELECT
          col_int,
          '<name>'::TEXT AS col_text,
          col_bool,
          OBJECT_TRANSFORM(
            col_super_1
            SET
              '"person"."name"', ('<name>'::TEXT),
              '"person"."salary"', (
                FLOOR(col_super_1.person.salary::FLOAT / 10) * 10
              ),
              '"person"."ssn"', (
                CASE
                  WHEN
                    col_super_1.person.age::INT <= 18
                    THEN 'XXX-XX-XXXX'::TEXT
                  WHEN
                    col_super_1.person.state::VARCHAR(256) != 'CA'::TEXT
                    THEN 'XXX-XX-'::TEXT || SUBSTRING(
                      col_super_1.person.ssn::VARCHAR(256) FROM 8 FOR 4
                    )
                  ELSE
                    col_super_1.person.ssn::VARCHAR(256)
                END
              )
          ) AS col_super_1,
          col_super_2
        FROM
          t_dp50000_target
      ) AS t_dp50000_target
  )SQL";

  Query* query_expected = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(std::string(query_expected_str)))));
  std::string query_expected_omni =
      omnisql::PrintSQL(omnisql::TranslateQuery(query_expected));

  EXPECT_EQ(query_expected_omni, query_rewritten_omni);

  SetUserId(current_id);
  pg::RunQuery(R"SQL(DROP MASKING POLICY mp_dp50000_redact_ssn CASCADE)SQL");
  pg::RunQuery(
      R"SQL(DROP MASKING POLICY mp_dp50000_redact_salary CASCADE)SQL");
  pg::RunQuery(R"SQL(DROP MASKING POLICY mp_dp50000_redact_name CASCADE)SQL");
}

void TestDDMOverNested::InPlaceNestedVarsWalkerTest(
    const char* query_str, std::vector<std::string> attachment_targets,
    ddm::NestedVarMap expected) {
  auto current_id = GetUserId();
  auto user_id = get_user_id_by_name("u_dp50000");

  pg::RunQuery(R"SQL(
    CREATE MASKING POLICY mp_dp50000
    WITH(a INT) USING ( a + 10 )
  )SQL");
  for (auto&& target : attachment_targets) {
    pg::RunQuery(boost::str(boost::format(R"SQL(
      ATTACH MASKING POLICY mp_dp50000
      ON %1%(col_super_1.nested.path)
      TO ROLE r_dp50000
    )SQL") % target));
  }

  SetUserId(user_id);

  Query* query = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(std::string(query_str)))));
  // Need to expand views.
  List* pg_rewrite = QueryRewrite(query);
  ASSERT_EQ(1, list_length(pg_rewrite));
  query = reinterpret_cast<Query*>(linitial(pg_rewrite));

  ddm::NestedVarMap map = ddm::WalkInPlaceNestedVars(query);

  EXPECT_EQ(expected, map);

  SetUserId(current_id);
  pg::RunQuery(R"SQL(DROP MASKING POLICY mp_dp50000 CASCADE)SQL");
}

PG_TEST_F(TestDDMOverNested, InPlaceNestedVarsWalkerSimpleTable) {
  InPlaceNestedVarsWalkerTest(
      /* query_str */
      R"SQL(
        SELECT col_super_1.nested
        FROM t_dp50000_target
      )SQL",
      {"t_dp50000_target"}, /* attachment_targets */
      {{main_table_oid, {kColSuper1}}} /* expected */);
}

PG_TEST_F(TestDDMOverNested, InPlaceNestedVarsWalkerSimpleView) {
  InPlaceNestedVarsWalkerTest(
      /* query_str */
      R"SQL(
        SELECT col_super_1.nested
        FROM v_dp50000_target
      )SQL",
      {"v_dp50000_target"}, /* attachment_targets */
      {{main_view_oid, {kColSuper1}}} /* expected */);
}

PG_TEST_F(TestDDMOverNested, InPlaceNestedVarsWalkerSimpleTblViaView) {
  InPlaceNestedVarsWalkerTest(
      /* query_str */
      R"SQL(
        SELECT v_dp50000_target.col_super_1.nested
        FROM v_dp50000_target
      )SQL",
      {"t_dp50000_target"}, /* attachment_targets */
      {{main_table_oid, {kColSuper1}}} /* expected */);
}

PG_TEST_F(TestDDMOverNested, InPlaceNestedVarsWalkerViewRefsNested) {
  InPlaceNestedVarsWalkerTest(
      /* query_str */
      R"SQL(
        SELECT col_nested
        FROM v_dp50000_ref_nested
      )SQL",
      {"t_dp50000_target"}, /* attachment_targets */
      {{main_table_oid, {kColSuper1}}} /* expected */);
}

PG_TEST_F(TestDDMOverNested, InPlaceNestedVarsWalkerLBVRefsNested) {
  InPlaceNestedVarsWalkerTest(
      /* query_str */
      R"SQL(
        SELECT col_nested
        FROM lbv_dp50000_ref_nested
      )SQL",
      {"t_dp50000_target"}, /* attachment_targets */
      {{main_table_oid, {kColSuper1}}} /* expected */);
}

PG_TEST_F(TestDDMOverNested, InPlaceNestedVarsWalkerSimpleLBV) {
  InPlaceNestedVarsWalkerTest(
      /* query_str */
      R"SQL(
        SELECT col_super_1.nested
        FROM lbv_dp50000_target
      )SQL",
      {"lbv_dp50000_target"}, /* attachment_targets */
      {{main_lbv_oid, {kColSuper1}}} /* expected */);
}

PG_TEST_F(TestDDMOverNested, InPlaceNestedVarsWalkerAllRelTypes) {
  InPlaceNestedVarsWalkerTest(
      /* query_str */
      R"SQL(
        SELECT
          T.col_super_1.nested,
          V.col_super_1.nested,
          LBV.col_super_1.nested
        FROM
          t_dp50000_target T
          JOIN v_dp50000_target V ON (T.col_int = V.col_int)
          JOIN lbv_dp50000_target LBV ON (T.col_int = LBV.col_int)
      )SQL",
      {
          "t_dp50000_target",
          "v_dp50000_target",
          "lbv_dp50000_target",
      }, /* attachment_targets */
      {
          {main_table_oid, {kColSuper1}},
          {main_view_oid, {kColSuper1}},
          {main_lbv_oid, {kColSuper1}},
      } /* expected */);
}

// TODO(bogatov): add tests for when nested path is same, shorter, longer, or
// divergent from attached.

// TODO(bogatov): check multiple vars per RTE.

// TODO(bogatov): check nav path with array.

void TestDDMOverNested::NavPathOptimizationRewrite(
    const char* user_query_str, const char* query_expected_complex_str,
    const char* query_expected_optimized_str) {
  auto current_id = GetUserId();

  struct PolicyCase {
    const char* arg_type;
    const char* policy_str;
    const char* policy_sql;
  };
  std::vector<PolicyCase> policies = {
      PolicyCase{
          "INT",                                          /* arg_type */
          R"SQL( a + 10 )SQL",                            /* policy_str */
          R"SQL( col_super_1.nested.path::INT + 10 )SQL", /* policy_sql */
      },
      PolicyCase{
          "INT",                /* arg_type */
          R"SQL( 10::INT )SQL", /* policy_str */
          R"SQL( 10::INT )SQL", /* policy_sql */
      },
      PolicyCase{
          "INT",                          /* arg_type */
          R"SQL( udf_dp50000(a, 5) )SQL", /* policy_str */
          R"SQL(
            udf_dp50000(
              col_super_1.nested.path::INT,
              5::INT
            )
          )SQL",                          /* policy_sql */
      },
      PolicyCase{
          "SUPER",                                         /* arg_type */
          R"SQL( a.other + 10 )SQL",                       /* policy_str */
          R"SQL( col_super_1.nested.path.other + 10 )SQL", /* policy_sql */
      },
      PolicyCase{
          "INT", /* arg_type */
          R"SQL(
            CASE
                WHEN
                    a IN (
                        SELECT J.col_int
                        FROM t_dp50000_join J
                        WHERE J.col_int IS NOT NULL
                    )
                THEN 'found'
                ELSE 'not-found'
            END::TEXT
          )SQL", /* policy_str */
          R"SQL(
            CASE
                WHEN
                    col_super_1.nested.path::INT IN (
                        SELECT J.col_int
                        FROM t_dp50000_join J
                        WHERE J.col_int IS NOT NULL
                    )
                THEN 'found'::TEXT
                ELSE 'not-found'::TEXT
            END
          )SQL", /* policy_sql */
      },
  };

  for (auto&& policy : policies) {
    pg::RunQuery(boost::str(boost::format(R"SQL(
      CREATE MASKING POLICY mp_dp50000
      WITH(a %1%) USING ( %2% )
    )SQL") % policy.arg_type %
                            policy.policy_str));
    pg::RunQuery(R"SQL(
      ATTACH MASKING POLICY mp_dp50000
      ON t_dp50000_target(col_super_1.nested.path)
      TO ROLE r_dp50000
    )SQL");

    auto target_id = get_user_id_by_name("u_dp50000");
    SetUserId(target_id);

    for (auto&& ref_complex : {true, false}) {
      auto ref = ref_complex ? "col_super_1.nested" : "col_super_1.nested.path";

      auto query_str = boost::str(boost::format(user_query_str) % ref);

      auto query_expected_str = ref_complex ? query_expected_complex_str
                                            : query_expected_optimized_str;
      auto query_expected_formatted =
          boost::str(boost::format(query_expected_str) % policy.policy_sql);

      Query* query = reinterpret_cast<Query*>(
          linitial(pg::Analyze(pg::Parse(std::string(query_str)))));
      auto query_rewritten =
          ddm::RewriteQuery(copyObject(query), false /* log_applied_policy */)
              .rewrite;

      auto query_rewritten_omni =
          omnisql::PrintSQL(omnisql::TranslateQuery(query_rewritten));

      Query* query_expected = reinterpret_cast<Query*>(
          linitial(pg::Analyze(pg::Parse(query_expected_formatted))));

      std::string query_expected_omni =
          omnisql::PrintSQL(omnisql::TranslateQuery(query_expected));

      EXPECT_EQ(query_expected_omni, query_rewritten_omni)
          << policy.policy_str << " : "
          << (ref_complex ? "complex" : "optimized");
    }
    SetUserId(current_id);
    pg::RunQuery(R"SQL(DROP MASKING POLICY mp_dp50000 CASCADE)SQL");
  }
}

PG_TEST_F(TestDDMOverNested, NavPathOptimizationRewriteSelect) {
  NavPathOptimizationRewrite(
      /* user_query_str */
      R"SQL(
        SELECT
          %1% AS result
        FROM
          t_dp50000_target
      )SQL",
      /* query_expected_complex_str */
      R"SQL(
        SELECT
          col_super_1.nested AS result
        FROM
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              OBJECT_TRANSFORM(
                col_super_1
                SET
                    '"nested"."path"', (%1%)
              ) AS col_super_1,
              col_super_2
            FROM
              t_dp50000_target
          ) AS t_dp50000_target
      )SQL",
      /* query_expected_optimized_str */
      R"SQL(
        SELECT
          (%1%)::SUPER AS result
        FROM
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              col_super_1,
              col_super_2
            FROM
              t_dp50000_target
          ) AS t_dp50000_target
      )SQL");
}

PG_TEST_F(TestDDMOverNested, NavPathOptimizationRewriteWhere) {
  NavPathOptimizationRewrite(
      /* user_query_str */
      R"SQL(
        SELECT
          1
        FROM
          t_dp50000_target
        WHERE
          %1% = 20
      )SQL",
      /* query_expected_complex_str */
      R"SQL(
        SELECT
          1
        FROM
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              OBJECT_TRANSFORM(
                col_super_1
                SET
                    '"nested"."path"', (%1%)
              ) AS col_super_1,
              col_super_2
            FROM
              t_dp50000_target
          ) AS t_dp50000_target
        WHERE
          col_super_1.nested = 20
      )SQL",
      /* query_expected_optimized_str */
      R"SQL(
        SELECT
          1
        FROM
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              col_super_1,
              col_super_2
            FROM
              t_dp50000_target
          ) AS t_dp50000_target
        WHERE
          (%1%)::SUPER = 20
      )SQL");
}

PG_TEST_F(TestDDMOverNested, NavPathOptimizationRewriteOrderBy) {
  NavPathOptimizationRewrite(
      /* user_query_str */
      R"SQL(
        SELECT
          1
        FROM
          t_dp50000_target
        ORDER BY %1%
      )SQL",
      /* query_expected_complex_str */
      R"SQL(
        SELECT
          1
        FROM
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              OBJECT_TRANSFORM(
                col_super_1
                SET
                    '"nested"."path"', (%1%)
              ) AS col_super_1,
              col_super_2
            FROM
              t_dp50000_target
          ) AS t_dp50000_target
        ORDER BY col_super_1.nested
      )SQL",
      /* query_expected_optimized_str */
      R"SQL(
        SELECT
          1
        FROM
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              col_super_1,
              col_super_2
            FROM
              t_dp50000_target
          ) AS t_dp50000_target
        ORDER BY (%1%)::SUPER
      )SQL");
}

PG_TEST_F(TestDDMOverNested, NavPathOptimizationRewriteGroupBy) {
  NavPathOptimizationRewrite(
      /* user_query_str */
      R"SQL(
        SELECT
          1
        FROM
          t_dp50000_target
        GROUP BY %1%
      )SQL",
      /* query_expected_complex_str */
      R"SQL(
        SELECT
          1
        FROM
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              OBJECT_TRANSFORM(
                col_super_1
                SET
                    '"nested"."path"', (%1%)
              ) AS col_super_1,
              col_super_2
            FROM
              t_dp50000_target
          ) AS t_dp50000_target
        GROUP BY col_super_1.nested
      )SQL",
      /* query_expected_optimized_str */
      R"SQL(
        SELECT
          1
        FROM
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              col_super_1,
              col_super_2
            FROM
              t_dp50000_target
          ) AS t_dp50000_target
        GROUP BY (%1%)::SUPER
      )SQL");
}

PG_TEST_F(TestDDMOverNested, NavPathOptimizationRewriteMultipleFrom) {
  NavPathOptimizationRewrite(
      /* user_query_str */
      R"SQL(
        SELECT
            J.col_int + T.%1%
        FROM
            t_dp50000_join J,
            t_dp50000_target T
      )SQL",
      /* query_expected_complex_str */
      R"SQL(
        SELECT
          J.col_int + T.col_super_1.nested
        FROM
          t_dp50000_join J,
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              OBJECT_TRANSFORM(
                col_super_1
                SET
                    '"nested"."path"', (%1%)
              ) AS col_super_1,
              col_super_2
            FROM
              t_dp50000_target
          ) AS T
      )SQL",
      /* query_expected_optimized_str */
      R"SQL(
        SELECT
          J.col_int + (%1%)::SUPER
        FROM
          t_dp50000_join J,
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              col_super_1,
              col_super_2
            FROM
              t_dp50000_target
          ) AS T
      )SQL");
}

// @warning in this case optimization does not apply! It's because the Var of
// JOIN clause appears not to have a nested path (in Var). Therefore, we must
// handle it as complex projection.
PG_TEST_F(TestDDMOverNested, NavPathOptimizationRewriteJoin) {
  NavPathOptimizationRewrite(
      /* user_query_str */
      R"SQL(
        SELECT
          1
        FROM
          t_dp50000_target
        JOIN t_dp50000_join ON (
          t_dp50000_target.%1% = t_dp50000_join.col_int
        )
      )SQL",
      /* query_expected_complex_str */
      R"SQL(
        SELECT
          1
        FROM
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              OBJECT_TRANSFORM(
                col_super_1
                SET
                    '"nested"."path"', (%1%)
              ) AS col_super_1,
              col_super_2
            FROM
              t_dp50000_target
          ) AS t_dp50000_target
        JOIN t_dp50000_join ON (
          t_dp50000_target.col_super_1.nested = t_dp50000_join.col_int
        )
      )SQL",
      /* query_expected_optimized_str */
      R"SQL(
        SELECT
          1
        FROM
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              OBJECT_TRANSFORM(
                col_super_1
                SET
                    '"nested"."path"', (%1%)
              ) AS col_super_1,
              col_super_2
            FROM
              t_dp50000_target
          ) AS t_dp50000_target
        JOIN t_dp50000_join ON (
          t_dp50000_target.col_super_1.nested.path = t_dp50000_join.col_int
        )
      )SQL");
}

/// @note: no optimization, see above.
PG_TEST_F(TestDDMOverNested, NavPathOptimizationRewriteSelfJoin) {
  NavPathOptimizationRewrite(
      /* user_query_str */
      R"SQL(
        SELECT
          1
        FROM
          t_dp50000_target T1
        JOIN t_dp50000_target T2 ON (
          T1.%1% = T2.%1%
        )
      )SQL",
      /* query_expected_complex_str */
      R"SQL(
        SELECT
          1
        FROM
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              OBJECT_TRANSFORM(
                col_super_1
                SET
                    '"nested"."path"', (%1%)
              ) AS col_super_1,
              col_super_2
            FROM
              t_dp50000_target
          ) AS T1
        JOIN (
          SELECT
            col_int,
            col_text,
            col_bool,
            OBJECT_TRANSFORM(
              col_super_1
              SET
                  '"nested"."path"', (%1%)
            ) AS col_super_1,
            col_super_2
          FROM
            t_dp50000_target
        ) AS T2
        ON (
          T1.col_super_1.nested = T2.col_super_1.nested
        )
      )SQL",
      /* query_expected_optimized_str */
      R"SQL(
        SELECT
          1
        FROM
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              OBJECT_TRANSFORM(
                col_super_1
                SET
                    '"nested"."path"', (%1%)
              ) AS col_super_1,
              col_super_2
            FROM
              t_dp50000_target
          ) AS T1
        JOIN (
          SELECT
            col_int,
            col_text,
            col_bool,
            OBJECT_TRANSFORM(
              col_super_1
              SET
                  '"nested"."path"', (%1%)
            ) AS col_super_1,
            col_super_2
          FROM
            t_dp50000_target
        ) AS T2
        ON (
          T1.col_super_1.nested.path = T2.col_super_1.nested.path
        )
      )SQL");
}

PG_TEST_F(TestDDMOverNested, NavPathOptimizationRewriteSubSelect) {
  NavPathOptimizationRewrite(
      /* user_query_str */
      R"SQL(
        SELECT
          1
        FROM
          t_dp50000_target
        WHERE (
          t_dp50000_target.%1%::INT8 IN (
            SELECT t_dp50000_join.col_int FROM t_dp50000_join
          )
        )
      )SQL",
      /* query_expected_complex_str */
      R"SQL(
        SELECT
          1
        FROM
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              OBJECT_TRANSFORM(
                col_super_1
                SET
                    '"nested"."path"', (%1%)
              ) AS col_super_1,
              col_super_2
            FROM
              t_dp50000_target
          ) AS t_dp50000_target
        WHERE (
          t_dp50000_target.col_super_1.nested::INT8 IN (
            SELECT t_dp50000_join.col_int FROM t_dp50000_join
          )
        )
      )SQL",
      /* query_expected_optimized_str */
      R"SQL(
        SELECT
          1
        FROM
          (
            SELECT
              col_int,
              col_text,
              col_bool,
              col_super_1,
              col_super_2
            FROM
              t_dp50000_target
          ) AS t_dp50000_target
        WHERE (
          (%1%)::SUPER::INT8 IN (
            SELECT t_dp50000_join.col_int FROM t_dp50000_join
          )
        )
      )SQL");
}

std::set<ddm::ParsedPolicyAttachment>
TestDDMOverNested::GenerateParsedAttachments(
    std::vector<std::pair<
        int, std::vector<std::pair<std::string, std::optional<std::string>>>>>
        input) {
  std::set<ddm::ParsedPolicyAttachment> attachments;
  for (auto&& [priority, attributes] : input) {
    ddm::ParsedPolicyAttachment attachment = {};
    attachment.attachment.polpriority = priority;
    for (auto&& [column, path] : attributes) {
      attachment.polattrs_out.push_back(ddm::AttachedAttribute{column, path});
      // Can't do simple std::make_shared<ResTarget> since DDM assumes it was
      // palloc'ed.
      ResTarget* rtgt = makeNode(ResTarget);
      attachment.pol.parsed_pol.push_back(
          std::shared_ptr<ResTarget>(rtgt, PAlloc_deleter()));
    }
    attachments.insert(attachment);
  }
  return attachments;
}

ddm::MaskMap TestDDMOverNested::GenerateExpectedMaskMap(
    std::unordered_map<std::string,
                       std::vector<std::pair<int, std::optional<std::string>>>>
        input) {
  ddm::MaskMap map;
  for (auto&& [key, entries] : input) {
    for (auto&& [priority, path] : entries) {
      map[key].push_back(ddm::MergeEntry{
          nullptr /* rtgt */,
          priority,
          nullptr /* attachment */,
          {} /* lf_alp_column_name */,
          path,
      });
    }
  }

  return map;
}

bool TestDDMOverNested::MaskMapEqual(const ddm::MaskMap& left,
                                     const ddm::MaskMap& right) {
  if (left.size() != right.size()) return false;

  for (auto&& [l_key, l_value] : left) {
    auto r_it = right.find(l_key);
    if (r_it == right.end()) return false;

    if (l_value.size() != r_it->second.size()) return false;

    std::vector<ddm::MergeEntry> l_value_sorted;
    std::vector<ddm::MergeEntry> r_value_sorted;

    std::partial_sort_copy(l_value.begin(), l_value.end(),
                           l_value_sorted.begin(), l_value_sorted.end());
    std::partial_sort_copy(r_it->second.begin(), r_it->second.end(),
                           r_value_sorted.begin(), r_value_sorted.end());

    for (size_t i = 0; i < l_value_sorted.size(); i++) {
      if (l_value_sorted[i].priority != r_value_sorted[i].priority ||
          l_value_sorted[i].nested_path != r_value_sorted[i].nested_path) {
        return false;
      }
    }
  }

  return true;
}

PG_TEST_F(TestDDMOverNested, BuildMaskMapWithNested) {
  {
    // Simple non-conflicting nested different attachments.
    auto attachments = GenerateParsedAttachments({
        {10, {{"col_1", R"("a"."b"."d")"}}},
        {20, {{"col_1", R"("a"."b"."c")"}}},
    });

    auto expected = GenerateExpectedMaskMap({
        {"col_1",
         {
             {20, R"("a"."b"."c")"},
             {10, R"("a"."b"."d")"},
         }},
    });

    ddm::MaskMap actual =
        ddm::BuildMaskMap(attachments, {} /* obj_transform_alp_ctx */);

    ASSERT_TRUE(MaskMapEqual(expected, actual));
  }

  {
    // Simple non-conflicting nested same attachment.
    auto attachments = GenerateParsedAttachments({
        {10,
         {
             {"col_1", R"("a"."b"."d")"},
             {"col_1", R"("a"."b"."c")"},
         }},
    });

    auto expected = GenerateExpectedMaskMap({{
        "col_1",
        {
            {10, R"("a"."b"."c")"},
            {10, R"("a"."b"."d")"},
        },
    }});

    ddm::MaskMap actual =
        ddm::BuildMaskMap(attachments, {} /* obj_transform_alp_ctx */);

    ASSERT_TRUE(MaskMapEqual(expected, actual));
  }

  {
    // Simple conflicting nested.
    auto attachments = GenerateParsedAttachments({
        {10, {{"col_1", R"("a"."b")"}}},
        {20, {{"col_1", R"("a"."b"."c")"}}},
    });

    auto expected = GenerateExpectedMaskMap({{
        "col_1",
        {
            {20, R"("a"."b"."c")"},
        },
    }});

    ddm::MaskMap actual =
        ddm::BuildMaskMap(attachments, {} /* obj_transform_alp_ctx */);

    ASSERT_TRUE(MaskMapEqual(expected, actual));
  }

  {
    // Column-level override, nested wins.
    auto attachments = GenerateParsedAttachments({
        {10, {{"col_1", std::nullopt}}},
        {20, {{"col_1", R"("a"."b"."c")"}}},
    });

    auto expected =
        GenerateExpectedMaskMap({{"col_1", {{20, R"("a"."b"."c")"}}}});

    ddm::MaskMap actual =
        ddm::BuildMaskMap(attachments, {} /* obj_transform_alp_ctx */);

    ASSERT_TRUE(MaskMapEqual(expected, actual));
  }

  {
    // Column-level override, column wins.
    auto attachments = GenerateParsedAttachments({
        {10, {{"col_1", R"("a"."b"."c")"}}},
        {20, {{"col_1", std::nullopt}}},
    });

    auto expected = GenerateExpectedMaskMap({{"col_1", {{20, std::nullopt}}}});

    ddm::MaskMap actual =
        ddm::BuildMaskMap(attachments, {} /* obj_transform_alp_ctx */);

    ASSERT_TRUE(MaskMapEqual(expected, actual));
  }

  {
    // Nested override multiple.
    auto attachments = GenerateParsedAttachments({
        {
            10,
            {
                {"col_1", R"("a"."b"."c")"},
                {"col_1", R"("a"."b"."d")"},
            },
        },
        {
            20,
            {{"col_1", R"("a"."b")"}},
        },
    });

    auto expected = GenerateExpectedMaskMap({{"col_1", {{20, R"("a"."b")"}}}});

    ddm::MaskMap actual =
        ddm::BuildMaskMap(attachments, {} /* obj_transform_alp_ctx */);

    ASSERT_TRUE(MaskMapEqual(expected, actual));
  }

  {
    // Multiple entries for column.
    auto attachments = GenerateParsedAttachments({
        {10,
         {
             {"col_1", R"("a"."b"."c")"},
             {"col_1", R"("a"."b"."d")"},
         }},
        {5,
         {
             {"col_1", R"("a"."b")"},
         }},
    });

    auto expected = GenerateExpectedMaskMap({
        {"col_1",
         {
             {10, R"("a"."b"."c")"},
             {10, R"("a"."b"."d")"},
         }},
    });

    ddm::MaskMap actual =
        ddm::BuildMaskMap(attachments, {} /* obj_transform_alp_ctx */);

    ASSERT_TRUE(MaskMapEqual(expected, actual));
  }

  {
    // Complex example.
    auto attachments = GenerateParsedAttachments({
        {10,
         {
             {"col_1", R"("a"."b"."c")"},
             {"col_1", R"("a"."b"."d")"},
         }},
        {20, {{"col_2", R"("a"."b")"}}},
        {30, {{"col_2", std::nullopt}}},
        {40, {{"col_1", R"("a"."b"."d"."e")"}}},
    });

    auto expected = GenerateExpectedMaskMap({
        {"col_1",
         {
             {10, R"("a"."b"."c")"},
             {40, R"("a"."b"."d"."e")"},
         }},
        {"col_2", {{30, std::nullopt}}},
    });

    ddm::MaskMap actual =
        ddm::BuildMaskMap(attachments, {} /* obj_transform_alp_ctx */);

    ASSERT_TRUE(MaskMapEqual(expected, actual));
  }

  {
    // Example from review: case 1.
    auto attachments = GenerateParsedAttachments({
        {1,
         {
             {"col_1", R"("a"."b"."c")"},
         }},
        {2,
         {
             {"col_1", R"("a"."b"."c")"},
         }},
    });

    auto expected = GenerateExpectedMaskMap({
        {"col_1",
         {
             {2, R"("a"."b"."c")"},
         }},
    });

    ddm::MaskMap actual =
        ddm::BuildMaskMap(attachments, {} /* obj_transform_alp_ctx */);

    ASSERT_TRUE(MaskMapEqual(expected, actual));
  }

  {
    // Example from review: case 2.
    auto attachments = GenerateParsedAttachments({
        {1,
         {
             {"col_1", R"("a"."b"."c")"},
         }},
        {2,
         {
             {"col_1", R"("a"."b")"},
         }},
    });

    auto expected = GenerateExpectedMaskMap({
        {"col_1",
         {
             {2, R"("a"."b")"},
         }},
    });

    ddm::MaskMap actual =
        ddm::BuildMaskMap(attachments, {} /* obj_transform_alp_ctx */);

    ASSERT_TRUE(MaskMapEqual(expected, actual));
  }
}

}  // namespace ddm_test
