/// Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#include "pg/ddm/test_attach_policy.hpp"

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

#include <algorithm>
#include <cstdio>
#include <memory>
#include <set>
#include <string>

extern bool gconf_enable_ddm;
extern bool g_pgtest_ddm;
extern bool gconf_ddm_enable_over_super_paths;
extern bool gconf_enable_object_transform;

namespace pgtest {

Oid relid_attach;

void TestDDMPolicyAttachment::SetUpTestCase() {
  g_pgtest_ddm = true;
  gconf_enable_ddm = true;
  gconf_ddm_enable_over_super_paths = true;
  gconf_enable_object_transform = true;
  // Setting DDM policy commands to exist within a Transaction context.
  TopTransactionContext = AllocSetContextCreate(
      TopMemoryContext, "DDMPolicyAttachContext", ALLOCSET_DEFAULT_MINSIZE,
      ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
  CurTransactionContext = TopTransactionContext;
  pg::RunQuery(
      "CREATE TABLE target_table(a INT, b VARCHAR(12),"
      "s super, \"a\"\"b\"\"\" INT, f FLOAT)");
  pg::RunQuery("CREATE VIEW target_view AS SELECT * FROM target_table");
  pg::RunQuery(
      "CREATE VIEW target_lbv AS SELECT * FROM public.target_table WITH NO "
      "SCHEMA BINDING");
  auto q = pgtest::ParseAnalyzeQuery("select * from target_table");
  relid_attach = GetRelID(q);
  pg::RunQuery("CREATE USER u1 PASSWORD DISABLE");
  pg::RunQuery("CREATE ROLE r1");
}

void TestDDMPolicyAttachment::TearDownTestCase() {
  g_pgtest_ddm = false;
  pg::RunQuery("DROP USER u1");
  pg::RunQuery("DROP ROLE r1");
}

/// Executes queries of the PolicyAttachmentParam to
/// create and attach policy to target. Parses policy
/// attachment and tests parsed policy against expected
/// policy size, input attributes size, output attributes size
/// and output attribute column name.
PG_TEST_P(TestDDMPolicyAttachment, TestAttachPolicy) {
  for (auto& s : GetParam().queries) {
    pg::RunQuery(s);
  }
  const char* name = GetParam().pol_name.c_str();
  Oid pol_id = GetDDMPolicyId(name);
  Name polname = palloc(NAMEDATALEN);
  namestrcpy(polname, reinterpret_cast<const char*>(name));
  ddm::Policy pol = {pol_id /* polid */,       0 /* poldbid */,
                     false /* maskoningest */, *polname,
                     0 /* polmodifiedby */,    GetParam().pol_attrs,
                     GetParam().pol_expr};
  auto parsed = ddm::ParseAttachment(
      {pol_id /* polid */, 0 /* permid */, relid_attach, 0 /* polpriority */,
       0 /* polmodifiedby */, GetParam().pol_attrsin /* polattrsin */,
       // car_col, dar_col
       GetParam().pol_attrsout /* polattrsout */, pol},
      std::nullopt);
  EXPECT_TRUE(parsed.pol.parsed_pol.size() == GetParam().expected_pol_size)
      << boost::format("wrong number of targets %lu, expected %lu") %
             parsed.pol.parsed_pol.size() % GetParam().expected_pol_size;
  EXPECT_TRUE(parsed.polattrs_in.size() == GetParam().expected_polattrsin_size)
      << boost::format("wrong number of input attributes %lu expected %lu") %
             parsed.polattrs_in.size() % GetParam().expected_polattrsin_size;
  EXPECT_TRUE(parsed.polattrs_out.size() ==
              GetParam().expected_polattrsout_size)
      << boost::format("wrong number of output attributes %lu expected %lu") %
             parsed.polattrs_out.size() % GetParam().expected_polattrsout_size;
  EXPECT_TRUE(strcmp((std::get<std::shared_ptr<ColumnDef>>(
                          parsed.polattrs_in[0].attribute))
                         ->colname,
                     GetParam().expected_polattrsin_0_colname.c_str()) == 0)
      << boost::format("wrong col expected %s was %s") %
             GetParam().expected_polattrsin_0_colname %
             (std::get<std::shared_ptr<ColumnDef>>(
                  parsed.polattrs_in[0].attribute))
                 ->colname;
  EXPECT_TRUE(strcmp((std::get<std::shared_ptr<ColumnDef>>(
                          parsed.polattrs_out[0].attribute))
                         ->colname,
                     GetParam().expected_polattrsout_0_colname.c_str()) == 0)
      << boost::format("wrong col expected %s was %s") %
             GetParam().expected_polattrsout_0_colname %
             (std::get<std::shared_ptr<ColumnDef>>(
                  parsed.polattrs_out[0].attribute))
                 ->colname;
};

/// Each PolicyAttachmentParam entry in policy_attachment_test_cases
/// corresponds to a test case of TestDDMPolicyAttachment. Parameters
/// of each PolicyAttachmentParam entry include
/// list of queries to create and attach masking policy,
/// name of the policy to be attached,
/// policy attributes, policy expression,
/// input attributes for the masking policy expression in JSON,
/// output attributes for the masking policy expression in JSON,
/// expected total number of the parsed policies,
/// expected parsed policy's total number of input attributes,
/// expected parsed policy's total number of output attributes,
/// expected column name of the parsed policy's attrsin[0],
/// expected column name of the parsed policy's attrsout[0].
PolicyAttachmentParam policy_attachment_test_cases[] = {
    PolicyAttachmentParam{
        {"CREATE MASKING POLICY ddm_policy_1 WITH "
         "(col1 INT) USING ( 1 )",
         "ATTACH MASKING POLICY ddm_policy_1 ON "
         "target_table (a) USING (a) TO PUBLIC"},
        "ddm_policy_1",
        R"([{"colname":"col1","typoid":23,"typmod":-1}])",
        R"([{"expr":" CAST(1 AS INT4) ","typoid":23,"typmod":-1}])",
        "[1]",
        "[1]",
        1,
        1,
        1,
        "a",
        "a"},
    PolicyAttachmentParam{
        {"CREATE MASKING POLICY ddm_policy_2 WITH "
         "(col1 INT, col2 FLOAT) USING ( col1 * 1, col2 + 1.0 )",
         "ATTACH MASKING POLICY ddm_policy_2 ON "
         "target_table (a, f) USING (a, f) TO PUBLIC PRIORITY 1;",
         "ATTACH MASKING POLICY ddm_policy_2 ON "
         "target_table (a, f) TO PUBLIC PRIORITY 2"},
        "ddm_policy_2",
        R"([{"colname":"col1","typoid":23,"typmod":-1},
        {"colname":"col2","typoid":701,"typmod":-1}])",
        R"([{"expr":" masked_table.col1 * CAST(1 AS INT4) ",
        "typoid":23,"typmod":-1},
        {"expr":"masked_table.col2 + 1.0",
        "typoid":701,"typmod":-1}])",
        "[1,5]",
        "[1,5]",
        2,
        2,
        2,
        "a",
        "a"},
    PolicyAttachmentParam{
        {"CREATE MASKING POLICY ddm_policy_3 WITH "
         "(col1 INT, col2 FLOAT) USING ( col1 * 1, col1 + 1.0 )",
         "ATTACH MASKING POLICY ddm_policy_3 ON "
         "target_table (a, f) TO PUBLIC PRIORITY 3"},
        "ddm_policy_3",
        R"([{"colname":"col1","typoid":23,"typmod":-1},
        {"colname":"col2","typoid":701,"typmod":-1}])",
        R"([{"expr":" masked_table.col1 * CAST(1 AS INT4) ",
        "typoid":23,"typmod":-1},{"expr":"masked_table.col1 +
        1.0","typoid":1700,"typmod":-1}])",
        "[1,5]",
        "[1,5]",
        2,
        2,
        2,
        "a",
        "a"},
    PolicyAttachmentParam{
        {"CREATE MASKING POLICY ddm_policy_4 WITH "
         "(a INT) USING ( 1 )",
         " ATTACH MASKING POLICY ddm_policy_4 ON "
         "target_table (a) TO ROLE r1 PRIORITY 5"},
        "ddm_policy_4",
        R"([{"colname":"col1","typoid":23,"typmod":-1}])",
        R"([{"expr":" CAST(1 AS INT4) ","typoid":23,"typmod":-1}])",
        "[1]",
        "[1]",
        1,
        1,
        1,
        "a",
        "a"},
    PolicyAttachmentParam{
        {"CREATE MASKING POLICY ddm_policy_5 WITH "
         "(col1 INT, col2 FLOAT) USING ( col1, col2 )",
         "ATTACH MASKING POLICY ddm_policy_5 ON "
         "target_table (a, f) TO ROLE r1 PRIORITY 6"},
        "ddm_policy_5",
        R"([{"colname":"col1","typoid":23,"typmod":-1},
        {"colname":"col2","typoid":701,"typmod":-1}])",
        R"([{"expr":"masked_table.col1","typoid":23,"typmod":-1},
        {"expr":"masked_table.col2","typoid":701,"typmod":-1}])",
        "[1,5]",
        "[1,5]",
        2,
        2,
        2,
        "a",
        "a"},
    PolicyAttachmentParam{
        {"CREATE MASKING POLICY ddm_policy_6 WITH "
         "(col1 INT) USING ( 1 )",
         "ATTACH MASKING POLICY ddm_policy_6 ON "
         "target_table (a) USING (a) TO u1 PRIORITY 7"},
        "ddm_policy_6",
        R"([{"colname":"col1","typoid":23,"typmod":-1}])",
        R"([{"expr":" CAST(1 AS INT4) ","typoid":23,"typmod":-1}])",
        "[1]",
        "[1]",
        1,
        1,
        1,
        "a",
        "a"},
    PolicyAttachmentParam{
        {"CREATE MASKING POLICY ddm_policy_7 WITH "
         "(col1 INT, col2 FLOAT, col3 FLOAT) USING ( col1 * 1, col2 + 1.0 "
         ")",
         "ATTACH MASKING POLICY ddm_policy_7 ON "
         "target_table (a, f) USING (a, f, f) TO PUBLIC PRIORITY 4"},
        "ddm_policy_7",
        R"([{"colname":"col1","typoid":23,"typmod":-1},
        {"colname":"col2","typoid":701,"typmod":-1},
        {"colname":"col3","typoid":701,"typmod":-1}])",
        R"([{"expr":" masked_table.col1 * CAST(1 AS INT4) ",
        "typoid":23,"typmod":-1},{"expr":"masked_table.col2 +
        1.0","typoid":701,"typmod":-1}])",
        "[1,5,5]",
        "[1,5]",
        2,
        3,
        2,
        "a",
        "a"},
    PolicyAttachmentParam{
        {"CREATE TABLE attach(a INT)",
         "CREATE MASKING POLICY attach WITH (a INT) USING ( 1 )",
         "ATTACH MASKING POLICY attach ON attach (a) TO PUBLIC"},
        "attach",
        R"([{"colname":"a","typoid":23,"typmod":-1}])",
        R"([{"expr":" CAST(1 AS INT4) ","typoid":23,"typmod":-1}])",
        "[1]",
        "[1]",
        1,
        1,
        1,
        "a",
        "a"},
    PolicyAttachmentParam{
        {"CREATE TABLE masking(a INT)",
         "CREATE MASKING POLICY masking WITH (a INT) USING ( 1 )",
         "ATTACH MASKING POLICY masking ON masking (a) TO PUBLIC"},
        "masking",
        R"([{"colname":"a","typoid":23,"typmod":-1}])",
        R"([{"expr":" CAST(1 AS INT4) ","typoid":23,"typmod":-1}])",
        "[1]",
        "[1]",
        1,
        1,
        1,
        "a",
        "a"},
    PolicyAttachmentParam{
        {"CREATE TABLE policy(a INT)",
         "CREATE MASKING POLICY policy WITH (a INT) USING ( 1 )",
         "ATTACH MASKING POLICY policy ON policy (a) TO PUBLIC"},
        "policy",
        R"([{"colname":"a","typoid":23,"typmod":-1}])",
        R"([{"expr":" CAST(1 AS INT4) ","typoid":23,"typmod":-1}])",
        "[1]",
        "[1]",
        1,
        1,
        1,
        "a",
        "a"},
    PolicyAttachmentParam{
        {"CREATE TABLE priority(a INT)",
         "CREATE MASKING POLICY priority WITH (a INT) USING ( 1 )",
         "ATTACH MASKING POLICY priority ON priority (a) TO PUBLIC"},
        "priority",
        R"([{"colname":"a","typoid":23,"typmod":-1}])",
        R"([{"expr":" CAST(1 AS INT4) ","typoid":23,"typmod":-1}])",
        "[1]",
        "[1]",
        1,
        1,
        1,
        "a",
        "a"}};
INSTANTIATE_TEST_SUITE_P(PolicyAttachmentTests, TestDDMPolicyAttachment,
                         ::testing::ValuesIn(policy_attachment_test_cases));

PG_TEST_F(TestDDMPolicyAttachment, TestAttachPolicySuperMaxPath) {
  pg::RunQuery(
      "CREATE MASKING POLICY mp_super_max_path with (a int) USING (a)");

  std::stringstream ss;
  std::string path_component = "path";
  for (unsigned int i = 0; i < MAX_PARTIQL_NESTING_LEVELS - 1; ++i) {
    ss << path_component << PathSetBase::kDelimiter;
  }
  ss << path_component;

  std::vector<std::string> rel_names = {"target_table", "target_view",
                                        "target_lbv"};

  for (std::string rel_name : rel_names) {
    std::string attach_statement =
        "ATTACH MASKING POLICY mp_super_max_path ON " + rel_name + " (s." +
        ss.str() + ") TO PUBLIC";
    pg::RunQuery(attach_statement);
    SUCCEED();

    // Adding one more level to exceed MAX_PARTIQL_NESTING_LEVELS.
    attach_statement = "ATTACH MASKING POLICY mp_super_max_path ON " +
                       rel_name + " (s.another_path." + ss.str() +
                       ") TO PUBLIC";

    bool query_failed = false;
    PG_TRY();
    { pg::RunQuery(attach_statement); }
    PG_CATCH();
    {
      query_failed = true;
      // Note, the framework seems to only copy first 256 characters of the
      // error!
      ErrorData* errData = CopyErrorData();
      std::string error_message = std::string(errData->message);
      std::transform(error_message.begin(), error_message.end(),
                     error_message.begin(),
                     [](unsigned char c) { return std::tolower(c); });

      EXPECT_TRUE(error_message.find("super path") != std::string::npos);
      EXPECT_TRUE(error_message.find("exceeds") != std::string::npos);
      EXPECT_TRUE(error_message.find("nesting levels") != std::string::npos);

      FreeErrorData(errData);
      FlushErrorState();
    }
    PG_END_TRY();
    ASSERT_TRUE(query_failed);
  }
}
};  // namespace pgtest
