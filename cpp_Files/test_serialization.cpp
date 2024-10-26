/// Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.

// These two includes must go first and in this order.
// clang-format off
#include <gtest/gtest.h>
#include "pg/test/pg_test.hpp"
// clang-format on

#include "catalog/pg_type.h"
#include "ddm/ddm_policy.hpp"
#include "ddm/ddm_serialization.hpp"
#include "nodes/makefuncs.h"

#include "pg/ddm/ddm_test_helper.hpp"

#include <boost/format.hpp>

#include <type_traits>
#include <vector>

namespace ddm_test {

class TestDDMSerialization : public pg::PgTest {};

/// Test serialization and deserialization of ParsedPolicyExpression objects.
/// Verify that deserialized value matches the original.
PG_TEST_F(TestDDMSerialization, ParsedPolicyExpressions) {
  std::vector<ddm::ParsedPolicyExpression> input = {
      {"a + 5", 10, 0},
      {"b * 10", 20, 1},
  };

  std::string serialized = ddm::SerializeParsedPolicyExpressions(input);

  ASSERT_FALSE(serialized.empty());

  std::vector<ddm::ParsedPolicyExpression> deserialized =
      ddm::DeserializeParsedPolicyExpressions(serialized);

  ASSERT_EQ(deserialized, input);
}

/// Test serialization and deserialization of ColumnDef objects. Verify that
/// deserialized value matches the original.
PG_TEST_F(TestDDMSerialization, ColumnDefs) {
  std::shared_ptr<ColumnDef> column_definition_1(
      makeColumnDef("column_1", INT4OID, 0), PAlloc_deleter());
  std::shared_ptr<ColumnDef> column_definition_2(
      makeColumnDef("column_2", FLOAT8OID, 1), PAlloc_deleter());

  std::vector<std::shared_ptr<ColumnDef>> input = {
      column_definition_1,
      column_definition_2,
  };

  std::string serialized = ddm::SerializeColumnDefs(input);

  ASSERT_FALSE(serialized.empty());

  std::vector<std::shared_ptr<ColumnDef>> deserialized =
      ddm::DeserializeColumnDefs(serialized);

  ASSERT_TRUE(std::equal(input.begin(), input.end(), deserialized.begin(),
                         pgtest::ColumnDefComparator));
}

/// Test AttachedAttribute comparator varying attribute type and values and
/// path values.
PG_TEST_F(TestDDMSerialization, AttachedAttributeComparator) {
  std::shared_ptr<ColumnDef> column_definition_1(
      makeColumnDef("column_1", INT4OID, 0), PAlloc_deleter());
  std::shared_ptr<ColumnDef> column_definition_2(
      makeColumnDef("column_2", FLOAT8OID, 1), PAlloc_deleter());

  ddm::AttachedAttribute aa_int_no_path = {1, std::nullopt};
  ddm::AttachedAttribute aa_int_with_path = {1, R"("some"."nested"."path")"};
  ddm::AttachedAttribute aa_int_with_path_2 = {2,
                                               R"("another"."nested"."path")"};
  ddm::AttachedAttribute aa_int_with_path_3 = {1,
                                               R"("another"."nested"."path")"};

  ddm::AttachedAttribute aa_str_no_path = {"col", std::nullopt};
  ddm::AttachedAttribute aa_str_with_path = {"col",
                                             R"("some"."nested"."path")"};
  ddm::AttachedAttribute aa_str_with_path_2 = {"col2",
                                               R"("some"."nested"."path")"};
  ddm::AttachedAttribute aa_str_with_path_3 = {"col",
                                               R"("another"."nested"."path")"};

  ddm::AttachedAttribute aa_coldef_no_path = {column_definition_1,
                                              std::nullopt};
  ddm::AttachedAttribute aa_coldef_with_path = {column_definition_1,
                                                R"("some"."nested"."path")"};
  ddm::AttachedAttribute aa_coldef_with_path_2 = {column_definition_2,
                                                  R"("some"."nested"."path")"};
  ddm::AttachedAttribute aa_coldef_with_path_3 = {
      column_definition_1, R"("another"."nested"."path")"};

  ASSERT_TRUE(aa_int_no_path == aa_int_no_path);
  ASSERT_TRUE(aa_int_with_path == aa_int_with_path);
  ASSERT_FALSE(aa_int_with_path == aa_int_with_path_3);
  ASSERT_FALSE(aa_int_with_path_2 == aa_int_with_path_3);

  ASSERT_TRUE(aa_str_no_path == aa_str_no_path);
  ASSERT_TRUE(aa_str_with_path == aa_str_with_path);
  ASSERT_FALSE(aa_str_with_path == aa_str_with_path_3);
  ASSERT_FALSE(aa_str_with_path_2 == aa_str_with_path_3);

  ASSERT_TRUE(aa_coldef_no_path == aa_coldef_no_path);
  ASSERT_TRUE(aa_coldef_with_path == aa_coldef_with_path);
  ASSERT_FALSE(aa_coldef_with_path == aa_coldef_with_path_3);
  ASSERT_FALSE(aa_coldef_with_path_2 == aa_coldef_with_path_3);

  ASSERT_FALSE(aa_int_with_path == aa_str_with_path);
  ASSERT_FALSE(aa_str_with_path == aa_coldef_with_path);
  ASSERT_FALSE(aa_coldef_with_path == aa_int_with_path);
}

/// Test serialization and deserialization of attribute_info_t(std::vector of
/// AttachedAttribute). Verify that deserialized value matches the original.
PG_TEST_F(TestDDMSerialization, AttachedAttributes) {
  ddm::attribute_info_t input = {{0, std::nullopt},
                                 {1, R"("some"."nested"."path")"},
                                 {"col1", std::nullopt},
                                 {"col2", R"("another"."nested"."path")"}};
  std::string expected_json_out =
      R"([)"
      R"(0,)"
      R"({"t":0,"column":1,"path":"\"some\".\"nested\".\"path\""},)"
      R"({"t":0,"column":"col1"},)"
      R"({"t":0,"column":"col2","path":"\"another\".\"nested\".\"path\""})"
      R"(])";

  std::string serialized_json_out = ddm::SerializeAttachedAttributes(input);
  ASSERT_TRUE(serialized_json_out == expected_json_out);

  ddm::attribute_info_t deserialized =
      ddm::DeserializeAttachedAttributes(serialized_json_out);
  ASSERT_TRUE(input == deserialized);
}

/// Test serialization and deserialization of ColumnDef objects via attribute
/// numbers and a relation. Verify that deserialized value matches the original.
PG_TEST_F(TestDDMSerialization, AttributesOldEncoding) {
  std::vector<int> input = {3, 4, 6};

  std::string serialized = ddm::SerializeAttributeNumbers(input);

  ASSERT_FALSE(serialized.empty());

  std::vector<ddm::AttachedAttribute> deserialized =
      ddm::DeserializeAttachedAttributes(serialized);

  ASSERT_EQ(deserialized.size(), input.size());
  for (size_t i = 0; i < deserialized.size(); i++) {
    ASSERT_TRUE(std::holds_alternative<int>(deserialized[i].attribute));
    ASSERT_EQ(std::get<int>(deserialized[i].attribute), input[i]);
    ASSERT_FALSE(deserialized[i].path.has_value());
  }
}

enum AttachmentType { kTable = 't', kView = 'v', kLBV = 'l' };

template <typename C>
void TypeCheck(AttachmentType type) {
  switch (type) {
    case kTable:
    case kView:
      if constexpr (!std::is_same<C, int>::value) {
        FAIL();
      }
      break;
    case kLBV:
      if constexpr (!std::is_same<C, std::string>::value) {
        FAIL();
      }
      break;
  }
}

template <typename C>
void AttributesNewEncoding(AttachmentType type) {
  TypeCheck<C>(type);

  std::string json_string;
  std::vector<C> expected_attributes;
  std::vector<std::optional<std::string>> expected_paths = {
      "nested.path", "another.nested.path", std::nullopt};
  if (type == kLBV) {
    json_string =
        R"( [                                                             )"
        R"(   { "t": %1%, "column": "a", "path": "nested.path" },         )"
        R"(   { "t": %1%, "column": "b", "path": "another.nested.path" }, )"
        R"(   { "t": %1%, "column": "c" }                                 )"
        R"( ]                                                             )";
    if constexpr (std::is_same<C, std::string>::value) {
      expected_attributes = {"a", "b", "c"};
    }
  } else {
    json_string =
        R"( [                                                           )"
        R"(   { "t": %1%, "column": 1, "path": "nested.path" },         )"
        R"(   { "t": %1%, "column": 2, "path": "another.nested.path" }, )"
        R"(   3                                                         )"
        R"( ]                                                           )";
    if constexpr (std::is_same<C, int>::value) {
      expected_attributes = {1, 2, 3};
    }
  }
  json_string = (boost::format(json_string) %
                 static_cast<int>(ddm::AttachedAttributeType::kLBVsPaths))
                    .str();

  std::vector<ddm::AttachedAttribute> deserialized_attributes =
      ddm::DeserializeAttachedAttributes(json_string);

  ASSERT_EQ(deserialized_attributes.size(), expected_attributes.size());
  for (size_t i = 0; i < deserialized_attributes.size(); i++) {
    ASSERT_TRUE(
        std::holds_alternative<C>(deserialized_attributes[i].attribute));
    ASSERT_EQ(std::get<C>(deserialized_attributes[i].attribute),
              expected_attributes[i]);
    ASSERT_EQ(deserialized_attributes[i].path, expected_paths[i]);
  }
}

/// Test deserialization of ColumnDef objects with paths. Verify that
/// found_paths flag is set correctly, and that the paths are read correctly.
PG_TEST_F(TestDDMSerialization, AttributesNewEncodingTable) {
  AttributesNewEncoding<int>(AttachmentType::kTable);
}
PG_TEST_F(TestDDMSerialization, AttributesNewEncodingView) {
  AttributesNewEncoding<int>(AttachmentType::kView);
}
PG_TEST_F(TestDDMSerialization, AttributesNewEncodingLBV) {
  AttributesNewEncoding<std::string>(AttachmentType::kLBV);
}

}  // namespace ddm_test
