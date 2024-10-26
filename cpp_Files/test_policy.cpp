/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#include "pg/ddm/test_policy.hpp"

#include "access/heapam.h"
#include "catalog/namespace.h"
#include "catalog/pg_policy_mask.h"
#include "catalog/pg_type.h"
#include "ddm/constants.hpp"
#include "ddm/ddm_policy.hpp"
#include "ddm/policy_access.h"
#include "nodes/makefuncs.h"
#include "rewrite/rewriteSupport.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#include "pg/ddm/ddm_test_helper.hpp"
#include "pg/src/include/utils/elog.h"

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/format.hpp>

#include <cstdio>
#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

extern bool gconf_enable_ddm;
extern bool g_pgtest_ddm;
extern bool gconf_enable_qpc_for_session;
extern bool gconf_enable_ddm_on_views;

namespace pgtest {
Oid test_relid, test_relid_2, test_view_relid, test_lbv_relid;
RangeTblEntry *test_rte, *test_view_rte, *test_lbv_rte;

void TestDDMPolicy::SetUpTestCase() {
  g_pgtest_ddm = true;
  gconf_enable_ddm = true;
  gconf_enable_ddm_on_views = true;
  gconf_enable_qpc_for_session = false;
  // Setting DDM policy commands to exist within a Transaction context.
  TopTransactionContext = AllocSetContextCreate(
      TopMemoryContext, "TopTransactionContext", ALLOCSET_DEFAULT_MINSIZE,
      ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

  CurTransactionContext = TopTransactionContext;

  pg::RunQuery(
      "CREATE TABLE test (foo_col int, bar_col int, car_col int, dar_col "
      "int)");
  auto q = pgtest::ParseAnalyzeQuery("select * from test;");
  test_rte = reinterpret_cast<RangeTblEntry*>(linitial(q->rtable));
  test_relid = GetRelID(q);
  pg::RunQuery("CREATE VIEW v1 AS SELECT * FROM TEST;");
  q = pgtest::ParseAnalyzeQuery("SELECT * FROM v1;");
  test_view_rte = reinterpret_cast<RangeTblEntry*>(linitial(q->rtable));
  test_view_relid = test_view_rte->relid;

  pg::RunQuery(
      "CREATE VIEW pollbv1 AS SELECT * FROM pgtest_testcase_schema.TEST WITH "
      "NO SCHEMA BINDING;");
  q = pgtest::ParseAnalyzeQuery("SELECT * FROM pollbv1;");
  test_lbv_rte = reinterpret_cast<RangeTblEntry*>(linitial(q->rtable));
  if (!OidIsValid(test_lbv_rte->relid)) {
    RangeTblEntry* view_old = GetViewOldRte(test_lbv_rte);
    if (view_old != nullptr) {
      XCHECK(OidIsValid(view_old->relid));
      test_lbv_rte->relid = view_old->relid;
    }
  }
  test_lbv_relid = test_lbv_rte->relid;

  pg::RunQuery(
      "CREATE TABLE target_table(a INT, b VARCHAR(12),"
      "s super, \"a\"\"b\"\"\" INT, f FLOAT)");
  auto q_2 = pgtest::ParseAnalyzeQuery("select * from target_table");
  test_relid_2 = GetRelID(q_2);
  pg::RunQuery("CREATE TABLE lookup (c int)");
  pg::RunQuery(
      "CREATE MASKING POLICY mp1pol with (a int, b int) USING (2 * a, 5 * b)");
  pg::RunQuery("CREATE USER u1 PASSWORD DISABLE");
  pg::RunQuery("CREATE USER u2 PASSWORD DISABLE");
  pg::RunQuery("CREATE USER u3 PASSWORD DISABLE");
  pg::RunQuery("CREATE ROLE r1");
  pg::RunQuery("GRANT ROLE r1 to u2");
  pg::RunQuery("ATTACH MASKING POLICY mp1pol ON test (foo_col, bar_col) TO u1");
  pg::RunQuery(
      "ATTACH MASKING POLICY mp1pol ON test (foo_col, bar_col) TO ROLE"
      " r1 priority 1;");
  pg::RunQuery(
      "ATTACH MASKING POLICY mp1pol ON v1 (foo_col, bar_col) TO ROLE"
      " r1 priority 1;");
  pg::RunQuery(
      "ATTACH MASKING POLICY mp1pol ON pollbv1 (foo_col, bar_col) TO ROLE r1 "
      "priority 1;");
}

void TestDDMPolicy::TearDownTestCase() {
  g_pgtest_ddm = false;
  pg::RunQuery("DROP VIEW v1;");
  pg::RunQuery("DROP VIEW pollbv1;");
  pg::RunQuery("DROP TABLE test;");
  pg::RunQuery("DROP TABLE lookup;");
  pg::RunQuery("DROP USER u1;");
  pg::RunQuery("DROP USER u2;");
  pg::RunQuery("DROP USER u3;");
  pg::RunQuery("DROP ROLE r1;");
}

PG_TEST_F(TestDDMPolicy, TestParsePolicy) {
  std::string expected =
      "{\n"
      "\tRESTARGET :name <> :indirection <> :val {\n"
      "\tAEXPR  :name (\"*\") :lexpr {\n"
      "\tA_CONST2 :typname <>} :rexpr {\n"
      "\tCOLUMNREF :fields (\"masked_table\" \"a\")}}}";

  const char* name = "test_pol";
  Name polname = palloc(NAMEDATALEN);
  namestrcpy(polname, reinterpret_cast<const char*>(name));
  ddm::Policy pol = {
      0 /* polid */,
      0 /* poldbid */,
      false /* maskoningest */,
      *polname,
      0 /* polmodifiedby */,
      "[{\"colname\": \"foo_col\", \"typoid\": 0, \"typmod\": 2}]",
      "[{\"expr\": \"2 * masked_table.a\",\"typoid\":0,\"typmod\":0}]"};
  ddm::ParsedPolicy res = ddm::ParsePolicy(pol);
  EXPECT_TRUE(res.parsed_pol.size() == 1) << "wrong number of parsed policies";

  char* query_str = nodeToString(res.parsed_pol[0].get());
  EXPECT_TRUE(strcmp(expected.c_str(), nodeToString(res.parsed_pol[0].get())) ==
              0)
      << boost::format(
             "expected parse_tree %s did not match actual parse_tree %s") %
             expected.c_str() % query_str;

  EXPECT_TRUE(res.parsed_attrs.size() == 1) << "wrong number of attrs";
  EXPECT_TRUE(res.parsed_attrs[0]->colname != NULL);
  EXPECT_TRUE(strcmp(res.parsed_attrs[0]->colname, "foo_col") == 0)
      << boost::format("wrong col_name %s") % res.parsed_attrs[0]->colname;
  EXPECT_TRUE(res.parsed_attrs[0]->typname != NULL);
};

PG_TEST_F(TestDDMPolicy, TestParseLookupPolicy) {
  std::string expected =
      "{\n"
      "\tRESTARGET :name <> :indirection <> :val {\n"
      "\tSUBLINK :subLinkType 2 :useOr false :notIn false :lefthand ({\n"
      "\tCOLUMNREF :fields (\"masked_table\" \"a\")}) :operName (\"=\") "
      ":operOids <> :subselect {\n"
      "\tSELECT :distinctClause <> :into <> :intoColNames <> :intoHasOids 2 "
      ":targetList ({\n"
      "\tRESTARGET :name <> :indirection <> :val {\n"
      "\tCOLUMNREF :fields (\"c\")}}) :fromClause ({\n"
      "\tRANGEVAR :schemaname <> :relname lookup_table :inhOpt 2 :istemp false "
      ":alias <> :physProps <> :unnest_type 0}) :whereClause <> :groupClause "
      "<> :groupByAll false "
      ":havingClause <> :sortClause <> :limitOffset <> :limitCount {\n"
      "\tTYPECAST :arg {\n"
      "\tA_CONST1 :typname <>} :typname {\n"
      "\tTYPENAME :names (\"int4\") :typid 0 :timezone false :setof false "
      ":pct_type false :typmod -1 :arrayBounds <>}} :forUpdate <> :with_clause "
      "<> :op 0 :all false :larg <> :rarg <> :hintspec {\n"
      "\tHINTSPEC :hintstring <> :hintlist <>} :isMaterializedView false "
      ":mvSelectStmtString <>}}}";

  const char* name = "test_pol";
  Name polname = palloc(NAMEDATALEN);
  namestrcpy(polname, reinterpret_cast<const char*>(name));
  ddm::Policy pol = {
      0 /* polid */,
      0 /* poldbid */,
      false /* maskoningest */,
      *polname,
      0 /* polmodifiedby */,
      "[{\"colname\": \"a\", \"typoid\": 23, \"typmod\": -1}]" /* polattrs */,
      "[{\"expr\": \"ROW(\\\"masked_table\\\".\\\"a\\\") = ANY (SELECT "
      "\\\"c\\\" FROM \\\"lookup_table\\\" LIMIT CAST(1 AS INT4))\", "
      "\"typoid\": 16, \"typmod\": -1}]" /* polexpr */
  };
  ddm::ParsedPolicy res = ddm::ParsePolicy(pol);
  EXPECT_TRUE(res.parsed_pol.size() == 1) << "wrong number of parsed policies";

  char* query_str = nodeToString(res.parsed_pol[0].get());
  EXPECT_TRUE(strcmp(expected.c_str(), nodeToString(res.parsed_pol[0].get())) ==
              0)
      << boost::format(
             "expected parse_tree %s did not match actual parse_tree %s") %
             expected.c_str() % query_str;

  EXPECT_TRUE(res.parsed_attrs.size() == 1) << "wrong number of attrs";
  EXPECT_TRUE(res.parsed_attrs[0]->colname != NULL);
  EXPECT_TRUE(strcmp(res.parsed_attrs[0]->colname, "a") == 0)
      << boost::format("wrong col_name %s") % res.parsed_attrs[0]->colname;
  EXPECT_TRUE(res.parsed_attrs[0]->typname != NULL);
};

PG_TEST_F(TestDDMPolicy, TestParseAttachment) {
  const char* name = "test_pol";
  Name polname = palloc(NAMEDATALEN);
  namestrcpy(polname, reinterpret_cast<const char*>(name));
  ddm::Policy pol = {
      0 /* polid */,
      0 /* poldbid */,
      false /* maskoningest */,
      *polname,
      0 /* polmodifiedby */,
      "[{\"colname\": \"a\", \"typoid\": 0, \"typmod\": 0},"
      "{\"colname\": \"b\", \"typoid\": 1, \"typmod\": 1}]",
      "[{\"expr\": \"2 * masked_table.a\", \"typoid\": 1, \"typmod\": "
      "0},{\"expr\": \"5 * masked_table.b\", \"typoid\": 1, "
      "\"typmod\": 0}]" /* polexpr */
  };
  ddm::Attachment attach = {0 /* polid */, 0 /* permid */, test_relid,
                            0 /* polpriority */, 0 /* polmodifiedby */,
                            // foo_col, bar_col
                            "[1, 2]" /* polattrsin */,
                            // car_col, dar_col
                            "[3, 4]" /* polattrsout */, pol};

  auto parsed = ddm::ParseAttachment(attach, std::nullopt);

  EXPECT_TRUE(parsed.pol.parsed_pol.size() == 2)
      << boost::format("wrong number of targets %lu, expected 2") %
             parsed.pol.parsed_pol.size();

  EXPECT_TRUE(parsed.polattrs_in.size() == 2)
      << boost::format("wrong number of input attributes %lu expected 2") %
             parsed.polattrs_in.size();
  EXPECT_TRUE(parsed.polattrs_out.size() == 2)
      << boost::format("wrong number of output attributes %lu expected 2") %
             parsed.polattrs_out.size();
  EXPECT_TRUE(strcmp((std::get<std::shared_ptr<ColumnDef>>(
                          parsed.polattrs_in[0].attribute))
                         ->colname,
                     "foo_col") == 0)
      << boost::format("wrong col expected foo_col was %s") %
             (std::get<std::shared_ptr<ColumnDef>>(
                  parsed.polattrs_in[0].attribute))
                 ->colname;
  EXPECT_TRUE(strcmp((std::get<std::shared_ptr<ColumnDef>>(
                          parsed.polattrs_out[1].attribute))
                         ->colname,
                     "dar_col") == 0)
      << boost::format("wrong col expected dar_col was %s") %
             (std::get<std::shared_ptr<ColumnDef>>(
                  parsed.polattrs_out[1].attribute))
                 ->colname;
};

PG_TEST_F(TestDDMPolicy, TestParseLBVAttachment) {
  const char* name = "test_pol";
  Name polname = palloc(NAMEDATALEN);
  namestrcpy(polname, reinterpret_cast<const char*>(name));
  ddm::Policy pol = {
      0 /* polid */,
      0 /* poldbid */,
      false /* maskoningest */,
      *polname,
      0 /* polmodifiedby */,
      "[{\"colname\": \"a\", \"typoid\": 0, \"typmod\": 0},"
      "{\"colname\": \"b\", \"typoid\": 1, \"typmod\": 1}]",
      "[{\"expr\": \"2 * masked_table.a\", \"typoid\": 1, \"typmod\": "
      "0},{\"expr\": \"5 * masked_table.b\", \"typoid\": 1, "
      "\"typmod\": 0}]" /* polexpr */
  };
  ddm::Attachment attach = {
      0 /* polid */,
      0 /* permid */,
      test_lbv_relid,
      0 /* polpriority */,
      0 /* polmodifiedby */,
      R"([{"t":0,"column":"a"},{"t":0,"column":"b"}])" /* polattrsin */,
      R"([{"t":0,"column":"a"},{"t":0,"column":"b"}])" /* polattrsout */,
      pol};

  auto parsed = ddm::ParseAttachment(attach, std::nullopt);

  EXPECT_TRUE(parsed.pol.parsed_pol.size() == 2)
      << boost::format("wrong number of targets %lu, expected 2") %
             parsed.pol.parsed_pol.size();

  EXPECT_TRUE(parsed.polattrs_in.size() == 2)
      << boost::format("wrong number of input attributes %lu expected 2") %
             parsed.polattrs_in.size();
  EXPECT_TRUE(parsed.polattrs_out.size() == 2)
      << boost::format("wrong number of output attributes %lu expected 2") %
             parsed.polattrs_out.size();
  EXPECT_TRUE(
      std::get<std::string>(parsed.polattrs_in[0].attribute).compare("a") == 0)
      << boost::format("wrong col expected a was %s") %
             (std::get<std::string>(parsed.polattrs_in[0].attribute));
  EXPECT_TRUE(
      std::get<std::string>(parsed.polattrs_out[1].attribute).compare("b") == 0)
      << boost::format("wrong col expected b was %s") %
             (std::get<std::string>(parsed.polattrs_out[1].attribute));
};

PG_TEST_F(TestDDMPolicy, TestBuildMaskMap) {
  const char* name = "test_pol";
  Name polname = palloc(NAMEDATALEN);
  namestrcpy(polname, reinterpret_cast<const char*>(name));

  ddm::Policy pol = {
      0 /* polid */,
      0 /* poldbid */,
      false /* maskoningest */,
      *polname,
      0 /* polmodifiedby */,
      "[{\"colname\": \"a\", \"typoid\": 0, \"typmod\": 0},"
      "{\"colname\": \"b\", \"typoid\": 1, \"typmod\": 1}]"  // polattrs
      ,
      "[{\"expr\": \"2 * masked_table.a\", \"typoid\": 1, \"typmod\": "
      "0},{\"expr\": \"5 * masked_table.b\", \"typoid\": 1, "
      "\"typmod\": 0}]" /* polexpr */
  };

  auto attach_0 =
      ddm::ParseAttachment({0 /* polid */, 0 /* permid */, test_relid,
                            0 /* polpriority */, 0 /* polmodifiedby */,
                            // foo_col, bar_col
                            "[1, 2]" /* polattrsin */,
                            // car_col, dar_col
                            "[3, 4]" /* polattrsout */, pol},
                           std::nullopt);

  auto attach_1 =
      ddm::ParseAttachment({0 /* polid */, 0 /* permid */, test_relid,
                            1 /* polpriority */, 0 /* polmodifiedby */,
                            // foo_col, bar_col
                            "[1, 2]" /* polattrsin */,
                            // car_col, bar_col
                            "[3, 2]" /* polattrsout */, pol},
                           std::nullopt);

  std::set<ddm::ParsedPolicyAttachment> pols;
  pols.insert(attach_0);
  pols.insert(attach_1);

  std::unordered_set<std::string_view> obj_transform_columns;
  auto masks = ddm::BuildMaskMap(pols, {} /* obj_transform_alp_ctx */);
  EXPECT_TRUE(masks.size() == 3)
      << boost::format("Wrong number of masks %lu") % masks.size();
  EXPECT_TRUE(masks.find("foo_col") == masks.end())
      << "Did not find foo_col in masks.";
  EXPECT_TRUE(masks.find("bar_col") != masks.end())
      << "Unexpectedly found bar_col in masks.";
  EXPECT_TRUE(masks.find("dar_col") != masks.end())
      << "Unexpectedly found dar_col in masks.";

  auto result = masks.find("car_col");
  EXPECT_TRUE(result != masks.end()) << "Did not find car_col in masks";
  ASSERT_EQ(1, result->second.size());
  ASSERT_TRUE(result->second[0].attachment.get() != nullptr);
  EXPECT_TRUE(result->second[0].attachment->attachment.polpriority == 1)
      << boost::format("Wrong priority %d, expected 1") %
             result->second[0].attachment->attachment.polpriority;
  EXPECT_TRUE(result->second[0].priority == 1)
      << boost::format("Wrong priority %d, expected 1") %
             result->second[0].priority;
  std::string expected_0 =
      "{\n\tRESTARGET :name <> :indirection <> :val {\n\tAEXPR  :name (\"*\") "
      ":lexpr {\n\tA_CONST2 :typname <>} :rexpr {\n\tCOLUMNREF :fields "
      "(\"foo_col\")}}}";
  EXPECT_TRUE(strcmp(expected_0.c_str(),
                     nodeToString(result->second[0].rtgt.get())) == 0)
      << boost::format("Wrong restarget %s") %
             nodeToString(result->second[0].rtgt.get());

  result = masks.find("dar_col");
  EXPECT_TRUE(result != masks.end()) << "Did not find dar_col in masks";
  ASSERT_EQ(1, result->second.size());
  EXPECT_TRUE(result->second[0].priority == 0)
      << boost::format("Wrong priority %d, expected 0") %
             result->second[0].priority;
  std::string expected_1 =
      "{\n\tRESTARGET :name <> :indirection <> :val {\n\tAEXPR  :name (\"*\") "
      ":lexpr {\n\tA_CONST5 :typname <>} :rexpr {\n\tCOLUMNREF :fields "
      "(\"bar_col\")}}}";
  EXPECT_TRUE(strcmp(expected_1.c_str(),
                     nodeToString(result->second[0].rtgt.get())) == 0)
      << boost::format("wrong restarget %s") %
             nodeToString(result->second[0].rtgt.get());
};

PG_TEST_F(TestDDMPolicy, TestMergePolicies) {
  const char* name = "mp1pol";
  Oid pol_id = GetDDMPolicyId(name);
  Name polname = palloc(NAMEDATALEN);
  namestrcpy(polname, reinterpret_cast<const char*>(name));

  ddm::Policy pol = {
      pol_id /* polid */,
      0 /* poldbid */,
      false /* maskoningest */,
      *polname,
      0 /* polmodifiedby */,
      "[{\"colname\": \"a\", \"typoid\": 0, \"typmod\": 0},"
      "{\"colname\": \"b\", \"typoid\": 1, \"typmod\": 1}]" /*polattrs */
      ,
      "[{\"expr\": \"2 * masked_table.a\", \"typoid\": 1, \"typmod\": "
      "0},{\"expr\": \"5 * masked_table.b\", \"typoid\": 1, "
      "\"typmod\": 0}]" /* polexpr */
  };

  auto attach_0 =
      ddm::ParseAttachment({pol_id /* polid */, 0 /* permid */, test_relid,
                            0 /* polpriority */, 0 /* polmodifiedby */,
                            // foo_col, bar_col
                            "[1, 2]" /* polattrsin */,
                            // car_col, dar_col
                            "[3, 4]" /* polattrsout */, pol},
                           std::nullopt);

  auto attach_1 =
      ddm::ParseAttachment({pol_id /* polid */, 0 /* permid */, test_relid,
                            1 /* polpriority */, 0 /* polmodifiedby */,
                            // foo_col, bar_col
                            "[1, 2]" /* polattrsin */,
                            // car_col, bar_col
                            "[3, 2]" /* polattrsout */, pol},
                           std::nullopt);
  std::set<ddm::ParsedPolicyAttachment> pols;
  pols.insert(attach_0);
  pols.insert(attach_1);

  auto q = pgtest::ParseAnalyzeQuery("select * from test;");
  auto rte = reinterpret_cast<RangeTblEntry*>(linitial(q->rtable));
  auto res = ddm::MergePolsForRTE(rte, q, pols, {} /* ddm_obj_transform_vars */,
                                  {} /* all_obj_transform_columns */);
  auto targets = res->targetList;
  EXPECT_TRUE(list_length(targets) == 4)
      << boost::format("Wrong number of targets %d, expected 4") %
             list_length(targets);
  EXPECT_TRUE(IsA(res, Query))
      << boost::format("Expected query got %s") % nodeToString(res);

  auto expect_q = pgtest::ParseAnalyzeQuery(
      "select foo_col, 5*bar_col as bar_col, 2*foo_col as car_col, 5*bar_col "
      "as dar_col from test;");
  auto expected_rte =
      reinterpret_cast<RangeTblEntry*>(linitial(expect_q->rtable));
  expected_rte->has_ddm_rewrite = false;

  std::string expect_res = nodeToString(expect_q);
  // Replace requiredPerms to 0, because Perm Checks are disabled via
  // DisableRTEPermCheck on the unfolded DDM SubQuery.
  boost::algorithm::replace_all(expect_res, ":requiredPerms 2",
                                ":requiredPerms 0");

  EXPECT_TRUE(strcmp(nodeToString(res), expect_res.c_str()) == 0)
      << boost::format(
             "Did not receive expected results. expected %s \ngot %s") %
             expect_res % nodeToString(res);
};

PG_TEST_F(TestDDMPolicy, TestColumnAlias) {
  const char* name = "mp1pol";
  Oid pol_id = GetDDMPolicyId(name);
  Name polname = palloc(NAMEDATALEN);
  namestrcpy(polname, reinterpret_cast<const char*>(name));

  ddm::Policy pol = {
      pol_id /* polid */,
      0 /* poldbid */,
      false /* maskoningest */,
      *polname,
      0 /* polmodifiedby */,
      "[{\"colname\": \"a\", \"typoid\": 0, \"typmod\": 0},"
      "{\"colname\": \"b\", \"typoid\": 1, \"typmod\": 1}]" /*polattrs */
      ,
      "[{\"expr\": \"2 * masked_table.a\", \"typoid\": 1, \"typmod\": "
      "0},{\"expr\": \"5 * masked_table.b\", \"typoid\": 1, "
      "\"typmod\": 0}]" /* polexpr */
  };

  auto attach =
      ddm::ParseAttachment({pol_id /* polid */, 0 /* permid */, test_relid,
                            0 /* polpriority */, 0 /* polmodifiedby */,
                            // foo_col, bar_col
                            "[1, 2]" /* polattrsin */,
                            // car_col, dar_col
                            "[3, 4]" /* polattrsout */, pol},
                           std::nullopt);

  std::set<ddm::ParsedPolicyAttachment> pols;
  pols.insert(attach);

  auto q = pgtest::ParseAnalyzeQuery("select * from test fake (a, b, c, d);");
  auto rte = reinterpret_cast<RangeTblEntry*>(linitial(q->rtable));
  auto res = ddm::MergePolsForRTE(rte, q, pols, {} /* ddm_obj_transform_vars */,
                                  {} /* alp_obj_transform_columns */);
  auto targets = res->targetList;
  EXPECT_TRUE(list_length(targets) == 4)
      << boost::format("Wrong number of targets %d, expected 4") %
             list_length(targets);
  EXPECT_TRUE(IsA(res, Query))
      << boost::format("Expected query got %s") % nodeToString(res);

  auto expect_q = pgtest::ParseAnalyzeQuery(
      "select foo_col, bar_col, 2 * foo_col as car_col, 5 * bar_col as "
      "dar_col from test");
  auto expected_rte =
      reinterpret_cast<RangeTblEntry*>(linitial(expect_q->rtable));
  expected_rte->has_ddm_rewrite = false;

  std::string expect_res = nodeToString(expect_q);
  // Replace requiredPerms to 0, because Perm Checks are disabled via
  // DisableRTEPermCheck on the unfolded DDM SubQuery.
  boost::algorithm::replace_all(expect_res, ":requiredPerms 2",
                                ":requiredPerms 0");

  EXPECT_TRUE(strcmp(nodeToString(res), expect_res.c_str()) == 0)
      << boost::format(
             "Did not receive expected results. expected %s \ngot %s") %
             expect_res % nodeToString(res);
};

PG_TEST_F(TestDDMPolicy, TestTraverseExpression) {
  // The mutator expects all column references in DDM expressions to have either
  // ddm::MASK_PSEUDO_TBL alias, or a lookup table prefix.
  auto prefix = [](std::string ref) {
    return std::string(ddm::MASK_PSEUDO_TBL) + "." + ref;
  };

  boost::format query_format =
      boost::format(
          "SELECT                                                            "
          "  %2% IS NULL,                                                    "
          "  1 == %3%,                                                       "
          "  %2% IS NOT NULL,                                                "
          "  (%2% + 2) :: INT IS NULL,                                       "
          "  CAST(%1% AS CHAR(4)),                                           "
          "  SHA2(%2%),                                                      "
          "  CASE SHA2(%1%) WHEN %3% THEN %2% WHEN %1% THEN %3% ELSE %1% END,"
          "  %1% [5:%2%] [%1%:2] [3] [%3%],                                  "
          "  TRY_CAST(%1% AS INT),                                           "
          "  GREATEST(%1%, %2%, %3%),                                        "
          "  GROUPING(%1%, %2%, %3%),                                        "
          "  %1%.super.path,                                                 "
          "  %1%[4].super.path,                                              "
          "  %1%.super.b, -- \"b\" must not be rewritten \n                  "
          "  %1% IS TRUE,                                                    "
          "  %2% == FALSE;                                                 ") %
      prefix("a") % prefix("b") % prefix("c");

  auto q = pg::Parse(query_format.str().c_str());
  EXPECT_TRUE(list_length(q) == 1) << "Expected a single parse_tree.";
  SelectStmt* select = reinterpret_cast<SelectStmt*>(linitial(q));
  ListCell* lc = NULL;
  ddm::SwapColDefMutatorCtx ctx = {};
  ctx.swap_map.insert({"a", {"test_col1", InvalidOid, 0, std::nullopt}});
  ctx.swap_map.insert({"b", {"test_col2", InvalidOid, 0, std::nullopt}});
  ctx.swap_map.insert({"c", {"test_col3", InvalidOid, 0, std::nullopt}});
  foreach(lc, select->targetList) {
    auto rtgt = reinterpret_cast<ResTarget*>(lfirst(lc));
    rtgt->val =
        ddm::parse_tree_mutator(rtgt->val, ddm::SwapColRefMutator, &ctx);
    replace_pointer(lc, rtgt);
  }
  SelectStmt* expected = reinterpret_cast<SelectStmt*>(linitial(
      pg::Parse("SELECT                                                   "
                "  test_col2 IS NULL,                                     "
                "  1 == test_col3,                                        "
                "  test_col2 IS NOT NULL,                                 "
                "  (test_col2 + 2) :: INT IS NULL,                        "
                "  CAST(test_col1 AS CHAR(4)),                            "
                "  SHA2(test_col2),                                       "
                "  CASE SHA2(test_col1)                                   "
                "    WHEN test_col3 THEN test_col2                        "
                "    WHEN test_col1 THEN test_col3                        "
                "    ELSE test_col1 END,                                  "
                "  test_col1 [5:test_col2] [test_col1:2] [3] [test_col3], "
                "  TRY_CAST(test_col1 AS INT),                            "
                "  GREATEST(test_col1, test_col2, test_col3),             "
                "  GROUPING(test_col1, test_col2, test_col3),             "
                "  test_col1.super.path,                                  "
                "  test_col1[4].super.path,                               "
                "  test_col1.super.b, -- b must not be rewritten \n       "
                "  test_col1 IS TRUE,                                     "
                "  test_col2 == FALSE;                                    ")));
  EXPECT_TRUE(strcmp(nodeToString(expected), nodeToString(select)) == 0)
      << "Actual and expected parse trees differ.";
}

PG_TEST_F(TestDDMPolicy, TestHasPolicy) {
  EXPECT_TRUE(HasDDMRewrite(test_rte));
  EXPECT_TRUE(HasDDMRewrite(test_view_rte));
  EXPECT_TRUE(HasDDMRewrite(test_lbv_rte));
}

PG_TEST_F(TestDDMPolicy, TestHasPolicyDDMDisable) {
  // change user to non-super user.
  auto current_id = GetUserId();
  auto u1_id = get_user_id_by_name("u1");
  SetUserId(u1_id);
  gconf_enable_ddm = false;

  PG_TRY();
  {
    HasDDMRewrite(test_rte);
    FAIL() << "Failed disabling DDM on Relation.";
  }
  PG_CATCH();
  { FlushErrorState(); }
  PG_END_TRY();

  PG_TRY();
  {
    HasDDMRewrite(test_view_rte);
    FAIL() << "Failed disabling DDM on Views.";
  }
  PG_CATCH();
  { FlushErrorState(); }
  PG_END_TRY();

  PG_TRY();
  {
    HasDDMRewrite(test_lbv_rte);
    FAIL() << "Failed disabling DDM on LBVs.";
  }
  PG_CATCH();
  { FlushErrorState(); }
  PG_END_TRY();

  gconf_enable_ddm = true;
  EXPECT_TRUE(HasDDMRewrite(test_rte));

  gconf_enable_ddm_on_views = false;

  PG_TRY();
  {
    HasDDMRewrite(test_view_rte);
    FAIL() << "Failed disabling DDM on Views.";
  }
  PG_CATCH();
  { FlushErrorState(); }
  PG_END_TRY();

  PG_TRY();
  {
    HasDDMRewrite(test_lbv_rte);
    FAIL() << "Failed disabling DDM on LBVs.";
  }
  PG_CATCH();
  { FlushErrorState(); }
  PG_END_TRY();

  gconf_enable_ddm_on_views = true;
  EXPECT_TRUE(HasDDMRewrite(test_view_rte));
  EXPECT_TRUE(HasDDMRewrite(test_lbv_rte));

  // change user to original user (bootstrap).
  SetUserId(current_id);
  gconf_enable_ddm = false;
  gconf_enable_ddm_on_views = false;
  // Verify that super user doesn't throw and that we get HasDDMRewrite = True
  // for all combinations of DDM on views gucs.
  EXPECT_TRUE(HasDDMRewrite(test_rte));
  EXPECT_TRUE(HasDDMRewrite(test_view_rte));
  EXPECT_TRUE(HasDDMRewrite(test_lbv_rte));

  gconf_enable_ddm = true;
  // Verify that we do not throw for superuser/bootstrap.
  EXPECT_TRUE(HasDDMRewrite(test_rte));
  EXPECT_TRUE(HasDDMRewrite(test_view_rte));
  EXPECT_TRUE(HasDDMRewrite(test_lbv_rte));
  gconf_enable_ddm_on_views = true;
  EXPECT_TRUE(HasDDMRewrite(test_rte));
  EXPECT_TRUE(HasDDMRewrite(test_view_rte));
  EXPECT_TRUE(HasDDMRewrite(test_lbv_rte));
}

/// Tests that after creating a masking policy and attaching it
/// to public/user/role, the attached policy can be detached from
/// public/user/role.
PG_TEST_F(TestDDMPolicy, TestDetachPolicy) {
  pg::RunQuery(
      "CREATE MASKING POLICY ddm_detach_policy WITH "
      "(col1 INT) USING ( 1 )");
  const char* name = "ddm_detach_policy";
  Oid pol_id = GetDDMPolicyId(name);
  EXPECT_TRUE(pol_id != InvalidOid)
      << boost::format("Expected to find '%s' policy") % name;
  pg::RunQuery(
      "ATTACH MASKING POLICY ddm_detach_policy ON "
      "target_table (a) USING (a) TO PUBLIC");
  EXPECT_TRUE(ddm::HasAttachmentsForPolicy(pol_id))
      << boost::format(
             "Expected to find '%s' policy "
             "with attachment") %
             name;
  pg::RunQuery(
      "DETACH MASKING POLICY ddm_detach_policy ON "
      "target_table (a) FROM PUBLIC");
  EXPECT_FALSE(ddm::HasAttachmentsForPolicy(pol_id))
      << boost::format(
             "Expected to find '%s' policy "
             "with no attachment") %
             name;
  pg::RunQuery(
      "ATTACH MASKING POLICY ddm_detach_policy ON "
      "target_table (a) USING (a) TO ROLE r1");
  EXPECT_TRUE(ddm::HasAttachmentsForPolicy(pol_id))
      << boost::format(
             "Expected to find '%s' policy "
             "with attachment") %
             name;
  pg::RunQuery(
      "DETACH MASKING POLICY ddm_detach_policy ON "
      "target_table (a) FROM ROLE r1");
  EXPECT_FALSE(ddm::HasAttachmentsForPolicy(pol_id))
      << boost::format(
             "Expected to find '%s' policy "
             "with no attachment") %
             name;
};

/// Tests that after creating a masking policy, that policy can be dropped.
PG_TEST_F(TestDDMPolicy, TestDropPolicy) {
  pg::RunQuery(
      "CREATE MASKING POLICY ddm_drop_policy WITH "
      "(col1 INT) USING ( 1 )");
  const char* name = "ddm_drop_policy";
  Oid pol_id = GetDDMPolicyId(name);
  EXPECT_TRUE(pol_id != InvalidOid)
      << boost::format("Expected to find '%s' policy") % name;
  pg::RunQuery("DROP MASKING POLICY ddm_drop_policy");
  pol_id = GetDDMPolicyId(name);
  EXPECT_TRUE(pol_id == InvalidOid)
      << boost::format("Expected not to find '%s' policy") % name;
};

/// Tests that an attached policy can not be dropped unless the drop
/// is CASCADE.
PG_TEST_F(TestDDMPolicy, TestDropPolicyWithAttachment) {
  pg::RunQuery(
      "CREATE MASKING POLICY ddm_drop_policy_with_attachment WITH "
      "(col1 INT) USING ( 1 )");
  const char* name = "ddm_drop_policy_with_attachment";
  Oid pol_id = GetDDMPolicyId(name);
  EXPECT_TRUE(pol_id != InvalidOid)
      << boost::format("Expected to find '%s' policy") % name;
  pg::RunQuery(
      "ATTACH MASKING POLICY ddm_drop_policy_with_attachment ON "
      "target_table (a) USING (a) TO PUBLIC");
  EXPECT_TRUE(ddm::HasAttachmentsForPolicy(pol_id))
      << boost::format(
             "Expected to find '%s' policy "
             "with attachment") %
             name;
  try {
    pg::RunQuery("DROP MASKING POLICY ddm_drop_policy_with_attachment");
    FAIL() << "Expected policy with attachment cannot be dropped exception";
  } catch (std::exception& e) {
    std::string error_message = e.what();
    boost::algorithm::to_lower(error_message);
    EXPECT_TRUE(error_message.find("ddm_drop_policy_with_attachment") !=
                    std::string::npos &&
                error_message.find("depend") != std::string::npos)
        << boost::format(
               "Expected to find error message regarding "
               "policy with attachment cannot be dropped ");
  }
  pol_id = GetDDMPolicyId(name);
  EXPECT_TRUE(pol_id != InvalidOid)
      << boost::format("Expected to find '%s' policy") % name;
  pg::RunQuery(
      "DROP MASKING POLICY ddm_drop_policy_with_attachment "
      "CASCADE;");
  pol_id = GetDDMPolicyId(name);
  EXPECT_TRUE(pol_id == InvalidOid)
      << boost::format("Expected not to find '%s' policy") % name;
};

/// Tests that an UDF attached to a masking policy can not be dropped
/// unless that masking policy is dropped.
PG_TEST_F(TestDDMPolicy, TestDropUDFAttachedToPolicy) {
  pg::RunQuery(
      "CREATE FUNCTION b_mask (VARCHAR(12)) RETURNS VARCHAR(12) STABLE AS $$ "
      "SELECT regexp_replace($1,'[a-z]', 'X') $$ LANGUAGE SQL");
  pg::RunQuery(
      "CREATE MASKING POLICY mp_udf_attachment WITH  (b VARCHAR(12)) USING ( "
      "b_mask (b))");
  try {
    pg::RunQuery("DROP FUNCTION b_mask(VARCHAR(12))");
    FAIL() << "Expected UDF cannot be dropped exception";
  } catch (std::exception& e) {
    std::string error_message = e.what();
    boost::algorithm::to_lower(error_message);
    EXPECT_TRUE(error_message.find("b_mask") != std::string::npos &&
                error_message.find("depend") != std::string::npos)
        << boost::format(
               "Expected to find error message regarding "
               "UDF cannot be dropped");
  }
  pg::RunQuery("DROP MASKING POLICY mp_udf_attachment");
  pg::RunQuery("DROP FUNCTION b_mask(VARCHAR(12))");
  SUCCEED();
};

/// Tests that a lookup table attached to a masking policy can not be dropped
/// unless that masking policy is dropped.
PG_TEST_F(TestDDMPolicy, TestDropLookupTableAttachedToPolicy) {
  pg::RunQuery("CREATE TABLE lookup_ids (id INT)");
  pg::RunQuery(
      "CREATE MASKING POLICY mp_lookup_table_attached WITH (a INT) USING (CASE "
      "WHEN (a in (select id from lookup_ids)) THEN a ELSE 0 END)");
  try {
    pg::RunQuery("DROP TABLE lookup_ids");
    FAIL() << "Expected lookup Table cannot be dropped exception";
  } catch (std::exception& e) {
    std::string error_message = e.what();
    boost::algorithm::to_lower(error_message);
    EXPECT_TRUE(error_message.find("lookup_ids") != std::string::npos &&
                error_message.find("depend") != std::string::npos)
        << boost::format(
               "Expected to find error message regarding "
               "lookup Table cannot be dropped");
  }
  pg::RunQuery("DROP MASKING POLICY mp_lookup_table_attached");
  pg::RunQuery("DROP TABLE lookup_ids");
  SUCCEED();
};

enum RelationType { kTable = 't', kView = 'v', kLBV = 'l' };
void testAttributesColumnDefConversion(RelationType type);

PG_TEST_F(TestDDMPolicy, TestAttributesColumnDefConversionTable) {
  testAttributesColumnDefConversion(kTable);
}
PG_TEST_F(TestDDMPolicy, TestAttributesColumnDefConversionView) {
  testAttributesColumnDefConversion(kView);
}
PG_TEST_F(TestDDMPolicy, TestAttributesColumnDefConversionLBV) {
  testAttributesColumnDefConversion(kLBV);
}

/// This test verifies correctness of converting
/// 1. Column names to attribute numbers
/// 2. Attribute numbers to ColumnDef objects
/// for relation types table, regular view and partially LBV. LBV is special
/// since it does not have attributes in the catalog.
/// ColumnNamesToAttributeNumbers would ereport (and we test it), but
/// AttributeNumbersToColumnDefs would fail XPGCHECK.
void testAttributesColumnDefConversion(RelationType type) {
  const std::string relation_suffix = "test_serialization";

  auto relation_name = [&relation_suffix](RelationType type) -> std::string {
    return (boost::format("%1%_%2%") % static_cast<char>(type) %
            relation_suffix)
        .str();
  };

  pg::RunQuery("CREATE TABLE public.", relation_name(kTable),
               "(                       "
               "    column_int int,     "
               "    column_text text,   "
               "    column_super super, "
               "    column_float float  "
               ")                       ");

  pg::RunQuery("CREATE VIEW public.", relation_name(kView),
               " AS SELECT * FROM public.", relation_name(kTable));
  pg::RunQuery("CREATE VIEW public.", relation_name(kLBV),
               " AS SELECT * FROM public.", relation_name(kTable),
               " WITH NO SCHEMA BINDING");

  const Oid relation_oid = RangeVarGetRelid(
      makeRangeVar("public", pstrdup(relation_name(type).c_str())),
      false /* failOK */);

  std::vector<ddm::AttachedAttribute> attributes = {
      {1, "nested.path"},
      {3, "another.path"},
      {4, std::nullopt},
  };

  std::vector<std::shared_ptr<Value>> column_names = {
      std::shared_ptr<Value>(makeString("column_int"), PAlloc_deleter()),
      // Skipping column_text for testing purposes.
      std::shared_ptr<Value>(makeString("column_super"), PAlloc_deleter()),
      std::shared_ptr<Value>(makeString("column_float"), PAlloc_deleter()),
  };

  std::vector<ddm::AttachedAttribute> column_definitions = {
      {std::shared_ptr<ColumnDef>(makeColumnDef("column_int", INT4OID, -1),
                                  PAlloc_deleter()),
       "nested.path"},
      {std::shared_ptr<ColumnDef>(
           makeColumnDef("column_super", PARTIQLOID, MAX_PARTIQL_SIZE),
           PAlloc_deleter()),
       "another.path"},
      {std::shared_ptr<ColumnDef>(makeColumnDef("column_float", FLOAT8OID, -1),
                                  PAlloc_deleter()),
       std::nullopt},
  };

  std::vector<int> actual_attribute_numbers;

  bool caught_error = false;
  PG_TRY();
  {
    actual_attribute_numbers =
        ddm::ColumnNamesToAttributeNumbers(relation_oid, column_names);
  }
  PG_CATCH();
  { caught_error = true; }
  PG_END_TRY();

  ASSERT_EQ(type == kLBV, caught_error);

  // The rest does not apply to LBVs, since AttributesToColumnDefs would fail
  // XPGCHECK for LBV.
  if (type != kLBV) {
    ASSERT_EQ(attributes.size(), actual_attribute_numbers.size());
    for (size_t i = 0; i < attributes.size(); i++) {
      ASSERT_EQ(std::get<int>(attributes[i].attribute),
                actual_attribute_numbers[i]);
    }

    Relation table_as_relation = relation_open(relation_oid, AccessShareLock);

    std::vector<ddm::AttachedAttribute> actual_column_definitions =
        ddm::AttributesToColumnDefs(table_as_relation, attributes);

    // Note that we cannot use SCOPE_EXIT_GUARD here since there is a RunQuery
    // involving the relation before function terminates.
    relation_close(table_as_relation, AccessShareLock);

    ASSERT_TRUE(std::equal(column_definitions.begin(), column_definitions.end(),
                           actual_column_definitions.begin(),
                           pgtest::AttachedAttributeComparator));
  }

  pg::RunQuery("DROP VIEW public.", relation_name(kView));
  pg::RunQuery("DROP VIEW public.", relation_name(kLBV));
  pg::RunQuery("DROP TABLE public.", relation_name(kTable));
}

};  // namespace pgtest
