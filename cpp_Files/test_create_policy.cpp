/// Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#include "pg/ddm/test_create_policy.hpp"

#include "ddm/constants.hpp"
#include "ddm/ddm_policy.hpp"
#include "ddm/policy_access.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#include "pg/ddm/ddm_test_helper.hpp"
#include "pg/src/include/catalog/pg_policy_mask.h"
#include "pg/src/include/utils/lsyscache.h"
#include "pg/test/pg_test.hpp"

#include <boost/algorithm/string/replace.hpp>
#include <boost/format.hpp>
#include <gtest/gtest.h>

#include <cstdio>
#include <set>
#include <string>

extern bool gconf_enable_ddm;
extern bool g_pgtest_ddm;

namespace pgtest {

Oid relid_create;

void TestDDMPolicyCreation::SetUpTestCase() {
  g_pgtest_ddm = true;
  gconf_enable_ddm = true;
  // Setting DDM policy commands to exist within a Transaction context.
  TopTransactionContext = AllocSetContextCreate(
      TopMemoryContext, "DDMPolicyCreateContext", ALLOCSET_DEFAULT_MINSIZE,
      ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
  CurTransactionContext = TopTransactionContext;
  pg::RunQuery(
      "CREATE TABLE target_table(a INT, b VARCHAR(12),"
      "s super, \"a\"\"b\"\"\" INT, f FLOAT)");
  auto q = pgtest::ParseAnalyzeQuery("SELECT * FROM target_table");
  relid_create = GetRelID(q);
}

void TestDDMPolicyCreation::TearDownTestCase() {
  g_pgtest_ddm = false;
}

PG_TEST_P(TestDDMPolicyCreation, TestCreatePolicy) {
  pg::RunQuery(GetParam().query.c_str());
  Oid pol_id = GetDDMPolicyId(GetParam().name.c_str());
  EXPECT_TRUE(pol_id != InvalidOid)
      << boost::format("Expected to find '%s' policy") %
             (GetParam().name.c_str());
}

PolicyCreationParam policy_create_test_cases[] = {
    PolicyCreationParam{// Simple policy
                        "CREATE MASKING POLICY mp_simple WITH "
                        "(a int, b float) USING(a + 1, a + b > 5)",
                        "mp_simple"},
    PolicyCreationParam{
        // More columns than expressions
        "CREATE MASKING POLICY mp_more_columns WITH "
        "(a int, b float, c VARCHAR(10)) USING (a + 1, a + b > 5)",
        "mp_more_columns"},
    PolicyCreationParam{// More expressions than columns
                        "CREATE MASKING POLICY mp_more_expressions WITH "
                        "(a int, b float) USING (a + 1, a + b > 5, b + 2)",
                        "mp_more_expressions"},
    PolicyCreationParam{// Constant in the USING clause
                        "CREATE MASKING POLICY mp_constant_int WITH "
                        " (a int, b float) USING (2)",
                        "mp_constant_int"},
    PolicyCreationParam{"CREATE MASKING POLICY mp_constant_bool WITH "
                        "(a int, b float) USING (TRUE)",
                        "mp_constant_bool"},
    PolicyCreationParam{"CREATE MASKING POLICY mp_constant_null WITH "
                        "(a int, b float) USING (NULL)",
                        "mp_constant_null"},
    PolicyCreationParam{"CREATE MASKING POLICY mp_constant_null_case WITH "
                        "(a int, b float) USING ("
                        "CASE a WHEN 4 THEN NULL ELSE 5*2 END)",
                        "mp_constant_null_case"},
    PolicyCreationParam{// Quoted column
                        "CREATE MASKING POLICY mp_quoted_column WITH "
                        "(\"a\"\"b\"\"\" int) USING (\"a\"\"b\"\"\" + 3)",
                        "mp_quoted_column"},
    PolicyCreationParam{// SUPER type
                        "CREATE MASKING POLICY mp_super_type WITH "
                        "(a super) USING (a.x.y + 4)",
                        "mp_super_type"},
    PolicyCreationParam{
        // Policy with CASES
        "CREATE MASKING POLICY mp_cases WITH "
        "(a int) USING (CASE a WHEN 3 THEN a + 1 ELSE a - 1 END)",
        "mp_cases"},
    PolicyCreationParam{// policy with expression type error
                        "CREATE MASKING POLICY mp_expression_type_error WITH "
                        "(a text, b float) USING (a - 1, a + b > 5)",
                        "mp_expression_type_error"}};

INSTANTIATE_TEST_SUITE_P(PolicyCreationTests, TestDDMPolicyCreation,
                         ::testing::ValuesIn(policy_create_test_cases));
};  // namespace pgtest
