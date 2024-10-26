/// Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.

// These two includes must go first and in this order.
// clang-format off
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
#include "pg/test/pg_test.hpp"
#include <gmock/gmock.h>
// clang-format on

#include "ddm/policy_access.h"
#include "ddm/rewriter.h"
#include "omnisql/omnisql_print_sql.hpp"
#include "omnisql/omnisql_translate_query.hpp"
#include "rewrite/rewriteHandler.h"
#include "seclog/seclog_masking.hpp"
#include "utils/lsyscache.h"

#include "systables/systables_seclog.hpp"

extern bool g_pgtest_ddm;
extern bool gconf_enable_secure_logging;

namespace ddm_test {

// SecLoged Systable name and columns (name, type, typoid).
using TestParam = std::pair<const std::string, SecLogSystableEntry>;

class TestDDMSecLog : public pg::PgTest,
                      public ::testing::WithParamInterface<TestParam> {
 public:
  TestDDMSecLog() {}

 protected:
  // Don't forget U suffix for unsigned!
  const std::size_t SYSTABLES_HASH = 2777785633089982489U;

  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

 private:
  AutoValueRestorer<bool> gconf_enable_secure_logging_restorer =
      AutoValueRestorer<bool>(gconf_enable_secure_logging, true);
  AutoValueRestorer<bool> g_pgtest_ddm_restorer =
      AutoValueRestorer<bool>(g_pgtest_ddm, true);
};

/// @brief This test is a litmus strip reacting to ANY change in seclog
/// landscape. In particular any new systable / column marked / unmarked for
/// sensitivity will trigger the failure of this test. If your CR *deliberately*
/// updates the SecLog config (in .tdefs), please update the hash here.
PG_TEST_F(TestDDMSecLog, SensitiveTablesSnapshot) {
  std::size_t hash = 0;

  using PairT = std::pair<std::string, SecLogSystableEntry>;

  std::vector<PairT> entries(seclog_systables.begin(), seclog_systables.end());
  std::sort(entries.begin(), entries.end(),
            [](PairT a, PairT b) { return a.first < b.first; });

  for (auto&& entry : entries) {
    boost::hash_combine(hash, boost::hash_value(entry.first));

    SecLogSystableEntry sorted(entry.second.begin(), entry.second.end());
    std::sort(sorted.begin(), sorted.end(),
              [](std::tuple<std::string, DefnType, int> a,
                 std::tuple<std::string, DefnType, int> b) {
                return std::get<0>(a) < std::get<0>(b);
              });

    for (auto&& column : sorted) {
      boost::hash_combine(hash, boost::hash_value(std::get<0>(column)));
      boost::hash_combine(hash, boost::hash_value(std::get<1>(column)));
      boost::hash_combine(hash, boost::hash_value(std::get<2>(column)));
    }
  }

  ASSERT_EQ(SYSTABLES_HASH, hash);
}

/// @brief Verifies that a known SecLoged stll table is reported as having DDM
/// attachments and rewrite.
PG_TEST_F(TestDDMSecLog, HasDDMAttachments) {
  Oid stll_oid = get_system_catalog_relid("stll_ddltext");
  ASSERT_NE(InvalidOid, stll_oid);

  ASSERT_TRUE(HasDDMAttachmentForRel(stll_oid));

  RangeTblEntry* main_table_rte = reinterpret_cast<RangeTblEntry*>(linitial(
      reinterpret_cast<Query*>(
          linitial(pg::Analyze(pg::Parse("SELECT * from stll_ddltext"))))
          ->rtable));

  ASSERT_TRUE(HasDDMRewrite(main_table_rte));
}

/// @brief Verify explicit SQL rewrite of a known SecLoged stll table.
PG_TEST_F(TestDDMSecLog, OneSTLRewrite) {
  auto input_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse("SELECT * from stll_ddltext"))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;

  auto rewritten_input_sql =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));

  auto expected_sql = R"SQL(
    SELECT * FROM (
      SELECT
        "stll_ddltext"."userid" AS "userid",
        "stll_ddltext"."xid" AS "xid",
        "stll_ddltext"."pid" AS "pid",
        CASE
          WHEN
            (
              GETBIT(
                CAST("stll_ddltext"."access_control" AS VARBYTE(4)),
                CAST(1 AS INT4)
              ) = CAST(1 AS INT4)
            )
          THEN CAST(CAST('******' AS BPCHAR) AS CHAR(320))
          ELSE "stll_ddltext"."label"
        END AS "label",
        "stll_ddltext"."starttime" AS "starttime",
        "stll_ddltext"."endtime" AS "endtime",
        "stll_ddltext"."sequence" AS "sequence",
        CASE
          WHEN
            (
              GETBIT(
                CAST("stll_ddltext"."access_control" AS VARBYTE(4)),
                CAST(1 AS INT4)
              ) = CAST(1 AS INT4)
            )
          THEN CAST(CAST('******' AS BPCHAR) AS CHAR(200))
          ELSE "stll_ddltext"."text"
        END AS "text",
        (-CAST(1 AS INT4)) AS "access_control"
      FROM "pg_catalog"."stll_ddltext" AS "stll_ddltext"
    ) AS "stll_ddltext"
  )SQL";

  auto expected_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(expected_sql))));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));

  ASSERT_EQ(expected_q_omni, rewritten_input_sql);
}

/// @brief Verify an example (known failure) where multiple SecLog-masked STLs
/// are JOINed.
PG_TEST_F(TestDDMSecLog, MultipleMasked) {
  auto input_q = reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(R"SQL(
      SELECT
        rtf.segment,
        rtf.step,
        rtf.hash_segment,
        rtf.hash_step,
        h.runtime_filter_type,
        h.runtime_filter_size,
        sum(CASE WHEN rtf.was_disabled = 't' THEN 1 ELSE 0 END) AS was_disabled,
        sum(CASE WHEN rtf.distributed = 't' THEN 1 ELSE 0 END) AS distributed,
        sum(CASE WHEN rtf.prefetched = 't' THEN 1 ELSE 0 END) AS prefetched,
        sum(rtf.checked) AS CHECKED,
        sum(rtf.rejected) AS rejected,
        CASE sum(rtf.checked)
          WHEN 0 THEN 0
          ELSE CAST((sum(rtf.checked) - sum(rtf.rejected))*100.0 / sum(rtf.checked) AS NUMERIC(5,2))
        END AS pass_percent,
        sum(rtf.checked) - sum(rtf.rejected) AS passed, -- should be equal to rows_pre_filter
        sum(s.rows_pre_filter) AS rows_pre_filter
      FROM stl_rtf_stats rtf
      LEFT JOIN stl_scan s
        ON rtf.query = s.query
        AND rtf.segment = s.segment
        AND rtf.step = s.step
        AND rtf.slice = s.slice
      LEFT JOIN stl_hash h
        ON rtf.query = h.query
        AND rtf.hash_segment = h.segment
        AND rtf.hash_step = h.step
        AND h.slice % 2 = 0
        AND (rtf.slice = h.slice OR rtf.slice = h.slice + 1)
      WHERE rtf.query = pg_last_query_id()
      GROUP BY 1,2,3,4,5,6
      ORDER BY 1,2,3,4,5,6
    )SQL"))));
  ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */);
  // Expect no exception. This query was a know failure case.
}

/// @brief Verify explicit SQL rewrite of a known SecLoged STV table which is
/// masked based on sensitive localizations set.
PG_TEST_F(TestDDMSecLog, OneSTVTableRewrite) {
  auto input_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse("SELECT * from stv_tbl_trans"))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;

  auto rewritten_input_sql =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));

  auto expected_sql = R"SQL(
SELECT
    *
FROM (
    SELECT "stv_tbl_trans"."slice" AS "slice",
        CASE
            WHEN IS_OBJECT_SENSITIVE_INTERNAL(
                GET_SENSITIVE_LOCALIZATIONS_INTERNAL(),
                "stv_tbl_trans"."id"
            ) THEN (- CAST(1 AS INT4))
            ELSE "stv_tbl_trans"."id"
        END AS "id",
        CASE
            WHEN IS_OBJECT_SENSITIVE_INTERNAL(
                GET_SENSITIVE_LOCALIZATIONS_INTERNAL(),
                "stv_tbl_trans"."id"
            ) THEN (- CAST(CAST(1 AS INT4) AS INT8))
            ELSE "stv_tbl_trans"."rows"
        END AS "rows",
        CASE
            WHEN IS_OBJECT_SENSITIVE_INTERNAL(
                GET_SENSITIVE_LOCALIZATIONS_INTERNAL(),
                "stv_tbl_trans"."id"
            ) THEN (- CAST(CAST(1 AS INT4) AS INT8))
            ELSE "stv_tbl_trans"."size"
        END AS "size",
        "stv_tbl_trans"."query_id" AS "query_id",
        "stv_tbl_trans"."ref_cnt" AS "ref_cnt",
        "stv_tbl_trans"."from_suspended" AS "from_suspended",
        "stv_tbl_trans"."prep_swap" AS "prep_swap"
    FROM "pg_catalog"."stv_tbl_trans" AS "stv_tbl_trans"
) AS "stv_tbl_trans"
  )SQL";

  auto expected_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(expected_sql))));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));

  ASSERT_EQ(expected_q_omni, rewritten_input_sql);
}

/// @brief Verify explicit SQL rewrite of a known SecLoged STV table which is
/// masked based on sensitive queries set.
PG_TEST_F(TestDDMSecLog, OneSTVQueryRewrite) {
  auto input_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse("SELECT * from stv_query_metrics"))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;

  auto rewritten_input_sql =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));

  auto expected_sql = R"SQL(
SELECT
  *
FROM (
  SELECT
    "stv_query_metrics"."userid" AS "userid",
    "stv_query_metrics"."service_class" AS "service_class",
    "stv_query_metrics"."query" AS "query",
    "stv_query_metrics"."segment" AS "segment",
    "stv_query_metrics"."step_type" AS "step_type",
    "stv_query_metrics"."starttime" AS "starttime",
    "stv_query_metrics"."slices" AS "slices",
    CASE
      WHEN
        IS_OBJECT_SENSITIVE_INTERNAL(
          GET_SENSITIVE_QUERIES_INTERNAL(),
          CAST("stv_query_metrics"."query" AS INT4)
        )
      THEN (- CAST(CAST(1 AS INT4) AS INT8))
      ELSE "stv_query_metrics"."max_rows"
    END AS "max_rows",
    CASE
      WHEN
        IS_OBJECT_SENSITIVE_INTERNAL(
          GET_SENSITIVE_QUERIES_INTERNAL(),
          CAST("stv_query_metrics"."query" AS INT4)
        )
      THEN (- CAST(CAST(1 AS INT4) AS INT8))
      ELSE "stv_query_metrics"."rows"
    END AS "rows",
    "stv_query_metrics"."max_cpu_time" AS "max_cpu_time",
    "stv_query_metrics"."cpu_time" AS "cpu_time",
    CASE
      WHEN
        IS_OBJECT_SENSITIVE_INTERNAL(
          GET_SENSITIVE_QUERIES_INTERNAL(),
          CAST("stv_query_metrics"."query" AS INT4)
        )
      THEN (- CAST(1 AS INT4))
      ELSE "stv_query_metrics"."max_blocks_read"
    END AS "max_blocks_read",
    CASE
      WHEN
        IS_OBJECT_SENSITIVE_INTERNAL(
          GET_SENSITIVE_QUERIES_INTERNAL(),
          CAST("stv_query_metrics"."query" AS INT4)
        )
      THEN (- CAST(CAST(1 AS INT4) AS INT8))
      ELSE "stv_query_metrics"."blocks_read"
    END AS "blocks_read",
    "stv_query_metrics"."max_run_time" AS "max_run_time",
    "stv_query_metrics"."run_time" AS "run_time",
    CASE
      WHEN
        IS_OBJECT_SENSITIVE_INTERNAL(
          GET_SENSITIVE_QUERIES_INTERNAL(),
          CAST("stv_query_metrics"."query" AS INT4)
        )
      THEN (- CAST(CAST(1 AS INT4) AS INT8))
      ELSE "stv_query_metrics"."max_blocks_to_disk"
    END AS "max_blocks_to_disk",
    CASE
      WHEN
        IS_OBJECT_SENSITIVE_INTERNAL(
          GET_SENSITIVE_QUERIES_INTERNAL(),
          CAST("stv_query_metrics"."query" AS INT4)
        )
      THEN (- CAST(CAST(1 AS INT4) AS INT8))
      ELSE "stv_query_metrics"."blocks_to_disk"
    END AS "blocks_to_disk",
    "stv_query_metrics"."step" AS "step",
    CASE
      WHEN
        IS_OBJECT_SENSITIVE_INTERNAL(
          GET_SENSITIVE_QUERIES_INTERNAL(),
          CAST("stv_query_metrics"."query" AS INT4)
        )
      THEN (- CAST(CAST(1 AS INT4) AS INT8))
      ELSE "stv_query_metrics"."max_query_scan_size"
    END AS "max_query_scan_size",
    CASE
      WHEN
        IS_OBJECT_SENSITIVE_INTERNAL(
          GET_SENSITIVE_QUERIES_INTERNAL(),
          CAST("stv_query_metrics"."query" AS INT4)
        )
      THEN (- CAST(CAST(1 AS INT4) AS INT8))
      ELSE "stv_query_metrics"."query_scan_size"
    END AS "query_scan_size",
    "stv_query_metrics"."query_priority" AS "query_priority",
    "stv_query_metrics"."query_queue_time" AS "query_queue_time"
  FROM "pg_catalog"."stv_query_metrics" AS "stv_query_metrics"
) AS "stv_query_metrics"
  )SQL";

  auto expected_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(expected_sql))));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));

  ASSERT_EQ(expected_q_omni, rewritten_input_sql);
}

/// @brief Tests sanity of JOIN over a sample of three STVs and three STLLs.
/// Sanity check includes the absence of exceptions and that resulting SQL
/// contains rewritten parts relating to each of the relations.
PG_TEST_F(TestDDMSecLog, STLLsAndSTVsJoined) {
  const std::string query = R"SQL(
    SELECT *
    FROM
      -- 3 STVs
           stv_query_metrics M
      JOIN stv_exec_state E ON (M.query = E.query)
      JOIN stv_user_query_state U ON (M.query = U.query_id)
      -- 3 STLLs
      JOIN stll_plan_explain P ON (M.query = P.query)
      JOIN stll_s3client_error S ON (M.query = S.query)
      JOIN stll_s3query Q ON (M.query = Q.query)
  )SQL";

  auto input_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(query))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;
  auto rewritten_input_sql =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));

  // Here we check that for each of the 6 relations above all columns are masked
  // in the rewritten SQL.
  std::set<std::string> rels_in_query{
      "stv_query_metrics", "stv_exec_state",      "stv_user_query_state",
      "stll_plan_explain", "stll_s3client_error", "stll_s3query",
  };
  for (auto&& [table, columns] : seclog_systables) {
    if (rels_in_query.find(table) == rels_in_query.end()) continue;
    for (auto&& column : columns) {
      if (std::get<0>(column) == systable::constants::kAccessControl) continue;
      std::string substr = boost::str(boost::format(R"(ELSE "%1%"."%2%")") %
                                      table % std::get<0>(column));
      EXPECT_THAT(rewritten_input_sql, ::testing::HasSubstr(substr));
    }
  }
}

/// @brief Iterate over all SecLoged systabled and verify that SELECT * FROM
/// them produces a valid rewrite (no errors). This would catch incompatibility
/// errors with generated policies and attachments.
PG_TEST_P(TestDDMSecLog, CheckRewriteForSystable) {
  std::string systable = GetParam().first;

  auto input_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse("SELECT * from " + systable))));
  auto rewritten_input_q =
      ddm::RewriteQuery(copyObject(input_q), false /* log_applied_policy */)
          .rewrite;

  auto rewritten_input_sql =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));

  // Due to backwards compatibility we have some STLs which only have
  // access_control column and no other sensitive columns. For this set we check
  // masking a bit differently.
  bool orphan_access_control =
      GetParam().second.size() == 1 &&
      std::get<0>(GetParam().second[0]) == std::string("access_control");
  if (orphan_access_control) {
    ASSERT_THAT(rewritten_input_sql,
                ::testing::HasSubstr("(- CAST(1 AS INT4))"));
    return;
  }

  ASSERT_THAT(rewritten_input_sql, ::testing::HasSubstr("CASE"));
  ASSERT_THAT(rewritten_input_sql, ::testing::HasSubstr("WHEN"));
  ASSERT_THAT(rewritten_input_sql, ::testing::HasSubstr("THEN"));
  ASSERT_THAT(rewritten_input_sql, ::testing::HasSubstr("END"));

  if (systable.rfind("stv_", 0) == 0) {
    ASSERT_THAT(rewritten_input_sql,
                ::testing::HasSubstr("IS_OBJECT_SENSITIVE_INTERNAL"));
  } else {
    ASSERT_THAT(rewritten_input_sql, ::testing::HasSubstr("access_control"));
    ASSERT_THAT(rewritten_input_sql, ::testing::HasSubstr("GETBIT"));
  }
}

struct PrintSystable {
  const char* operator()(const ::testing::TestParamInfo<TestParam> info) const {
    return info.param.first.c_str();
  }
};

INSTANTIATE_TEST_SUITE_P(TestDDMSecLogSystableTests, TestDDMSecLog,
                         ::testing::ValuesIn(seclog_systables),
                         PrintSystable());

enum UserType { kRegular, kSuperuser, kBootstrap, kSecAdmin };

class TestDDMSecLogUserTypes : public pg::PgTest,
                               public ::testing::WithParamInterface<UserType> {
 public:
  TestDDMSecLogUserTypes() {}

 private:
  AutoValueRestorer<bool> gconf_enable_secure_logging_restorer =
      AutoValueRestorer<bool>(gconf_enable_secure_logging, true);
  AutoValueRestorer<bool> g_pgtest_ddm_restorer =
      AutoValueRestorer<bool>(g_pgtest_ddm, true);
};

/// @brief Check that SecLog rewrite works for all relevant user types: regular,
/// super, bootstrap and secadmin.
PG_TEST_P(TestDDMSecLogUserTypes, UserTypes) {
  std::string nonce = boost::str(boost::format("%1%_%2%_%3%") % std::rand() %
                                 std::rand() % std::rand());
  auto RunQueryNonce = [&nonce](const std::string& query) {
    pg::RunQuery(boost::str(boost::format(query) % nonce));
  };
  auto GetUserIdNonce = [&nonce](const std::string& user) {
    return get_user_id_by_name(boost::str(boost::format(user) % nonce).c_str());
  };

  auto current_id = GetUserId();
  AclId user_id = GetUserId();

  // In PG tests for some reason regular users don't have default access to
  // pg_catalog schema and STLs in it. So we have to GRANT it manually.
  switch (GetParam()) {
    case kRegular:
      RunQueryNonce("CREATE USER reg_%1% WITH PASSWORD 'Test123!'");
      RunQueryNonce("GRANT ALL ON SCHEMA pg_catalog TO reg_%1%");
      RunQueryNonce("GRANT ALL ON pg_catalog.stl_querytext TO reg_%1%");
      user_id = GetUserIdNonce("reg_%1%");
      break;
    case kSuperuser:
      RunQueryNonce(
          "CREATE USER super_%1% WITH CREATEUSER PASSWORD 'Test123!'");
      user_id = GetUserIdNonce("super_%1%");
      break;
    case kSecAdmin:
      RunQueryNonce("CREATE USER secadmin_%1% WITH PASSWORD 'Test123!'");
      RunQueryNonce("GRANT ROLE sys:secadmin TO secadmin_%1%");
      RunQueryNonce("GRANT ALL ON SCHEMA pg_catalog TO secadmin_%1%");
      RunQueryNonce("GRANT ALL ON pg_catalog.stl_querytext TO secadmin_%1%");
      user_id = GetUserIdNonce("secadmin_%1%");
      break;
    case kBootstrap:
      break;
  }
  SetUserId(user_id);

  const std::string query = R"SQL( SELECT text FROM stl_querytext )SQL";

  // Note, due to an OMNI SQL printing quirk, we have to pass the query through
  // parse-analyze twice. The issue is that it inserts a useless extra pair of
  // parenthesis around a predicate and thus the SQL string is different from
  // the expected.
  ResultCacheState rcs = makeResultCacheState(kResultCacheNoLookup);
  auto input_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(query))));
  auto rewritten_input_q = reinterpret_cast<Query*>(
      linitial(QueryRewriteAll(copyObject(input_q), NULL, rcs, false)));
  auto rewritten_input_sql =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));
  rewritten_input_q = reinterpret_cast<Query*>(
      linitial(pg::Analyze(pg::Parse(rewritten_input_sql))));
  rewritten_input_sql =
      omnisql::PrintSQL(omnisql::TranslateQuery(rewritten_input_q));

  auto expected_sql = R"SQL(
SELECT "stl_querytext"."text" AS "text"
FROM (
    SELECT
      "stll_querytext"."userid" AS "userid",
      "stll_querytext"."xid" AS "xid",
      "stll_querytext"."pid" AS "pid",
      "stll_querytext"."query" AS "query",
      "stll_querytext"."sequence" AS "sequence",
      "stll_querytext"."text" AS "text"
    FROM (
        SELECT
          "stll_querytext"."userid" AS "userid",
          "stll_querytext"."xid" AS "xid",
          "stll_querytext"."pid" AS "pid",
          "stll_querytext"."query" AS "query",
          "stll_querytext"."sequence" AS "sequence",
          CASE
            WHEN (
              GETBIT(
                CAST("stll_querytext"."access_control" AS VARBYTE(4)),
                CAST(1 AS INT4)
              ) = CAST(1 AS INT4)
            ) THEN CAST(CAST('******' AS BPCHAR) AS CHAR(200))
            ELSE "stll_querytext"."text"
          END AS "text",
          (- CAST(1 AS INT4)) AS "access_control"
        FROM "pg_catalog"."stll_querytext" AS "stll_querytext"
      ) AS "stll_querytext"
    WHERE (
        ("stll_querytext"."access_control" = CAST(0 AS INT4))
        OR "stll_querytext"."access_control" IS NULL
      )
      OR (NOT IS_SECLOG_DOWNGRADE_ENABLED())
  ) AS "stl_querytext"
  )SQL";
  auto expected_q =
      reinterpret_cast<Query*>(linitial(pg::Analyze(pg::Parse(expected_sql))));
  auto expected_q_omni = omnisql::PrintSQL(omnisql::TranslateQuery(expected_q));

  EXPECT_EQ(expected_q_omni, rewritten_input_sql);

  SetUserId(current_id);

  RunQueryNonce("DROP USER IF EXISTS reg_%1%");
  RunQueryNonce("DROP USER IF EXISTS super_%1%");
  RunQueryNonce("DROP USER IF EXISTS secadmin_%1%");
}

struct PrintUserType {
  const char* operator()(const ::testing::TestParamInfo<UserType> info) const {
    switch (info.param) {
      case kRegular:
        return "regular";
      case kSuperuser:
        return "superuser";
      case kBootstrap:
        return "bootstrap";
      case kSecAdmin:
        return "secadmin";
    }
    XCHECK_UNREACHABLE("Invalid enum value");
  }
};

INSTANTIATE_TEST_SUITE_P(TestDDMSecLogUserTypesTests, TestDDMSecLogUserTypes,
                         ::testing::ValuesIn({
                             kRegular,
                             kSuperuser,
                             kBootstrap,
                             kSecAdmin,
                         }),
                         PrintUserType());

}  // namespace ddm_test
