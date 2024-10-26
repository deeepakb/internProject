/// Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#pragma once

#include "ddm/ddm_policy.hpp"
#include "policy/policy_serialization.hpp"
#include "sys/pg_utils.hpp"

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

/// @brief This file is for serialization-related code of DDM (i.e., JSON).
///
/// Please, follow the existing structure, in particular:
/// 1. Name functions with Serialize of Deserialize prefix if they consume /
/// produce JSON.
/// 2. Use the object name that the function produces / consumes in the name.
/// 3. Use singular if a function operates on object and plural if on arrays.
/// 4. Use @example tag in docs for expected / produced JSON format.
/// 5. If a function produces a pointer, specify how it's allocated (e.g.,
/// palloc'd)
namespace ddm {

enum AttachedAttributeType { kLBVsPaths };

/// @brief Serializes the sequence of expressions (as strings) and their types
/// into a JSON string. Output JSON is of the following form:
///
/// @example
/// [
///  {
///    "expr": "\"masked_table\".\"a\" + CAST(1 AS INT4)",
///    "typoid": 23,
///    "typmod": -1
///  }
/// ]
///
/// @param expressions the input sequence of expressions and types.
/// @return std::string the serialized JSON string.
std::string SerializeParsedPolicyExpressions(
    const std::vector<ParsedPolicyExpression>& expressions);

/// @brief Constructs a vector of ParsedPolicyExpression objects from a JSON
/// document given as string. Expects JSON in the form
///
/// @example
/// [
///     { "expr": "2 * "masked_table"."a"", "typid": 5, "typmod": 10 },
///     { "expr": "3 + "masked_table"."b"", "typid": 6, "typmod": 0 }
/// ]
///
/// @param json_string the JSON-encoded array.
/// @return a vector of ParsedPolicyExpression objects.
/// @throws XPGCHECK on invalid JSON input.
std::vector<ParsedPolicyExpression> DeserializeParsedPolicyExpressions(
    const std::string& json_string);

/// @brief Serializes the sequence of ColumnDef objects into a JSON string.
/// Note, it calls typenameTypeId for type OID, instead of looking directly at
/// column_definition->typname->typid. Output JSON is of the following form:
///
/// @example
/// [
///  {"colname":"a", "typoid":23, "typmod":-1},
///  {"colname":"b", "typoid":701, "typmod":-1}
/// ]
///
/// @param column_definitons the input sequence of ColumnDef objects.
/// @return std::string the serialized JSON string.
std::string SerializeColumnDefs(
    const std::vector<std::shared_ptr<ColumnDef>>& column_definitons);

/// Constructs a vector of ColumnDef shared pointers from a JSON document
/// given as string. Expects JSON in the form
///
/// @example
///   [
///     {"colname": "my_col_1", "typoid": 50, "typmod": 10},
///     {"colname": "my_col_2", "typoid": 60, "typmod": 0},
///   ]
///
/// @param json_string the JSON-encoded array.
/// @return a vector of palloc'd shared_ptr ColumnDefs.
/// @throws XPGCHECK on invalid JSON input.
std::vector<std::shared_ptr<ColumnDef>> DeserializeColumnDefs(
    const std::string& json_string);

/// @brief Serializes the sequence of attribute numbers (integers) into a JSON
/// string.
///
/// @example
/// [ 3, 4, 6 ]
///
/// @param attribute_numbers the input sequence of attribute numbers.
/// @return std::string the serialized JSON string.
std::string SerializeAttributeNumbers(const std::vector<int> attribute_numbers);

/// @brief Serializes the sequence of attribute names (std::strings) into a
/// string of a JSON array of objects.
///
/// @example
/// input: [ "col1", "col2", "col2" ]
/// output: "[{ type: kLBVsPaths, column: "col1" },{ type: kLBVsPaths,
/// column: "column_2" }]"
///
/// @param attribute_names the input sequence of attribute
/// names(column names).
/// @return std::string the serialized JSON string of array of objects.
std::string SerializeAttributeNames(
    const std::vector<std::string> attribute_names);

/// @brief Serializes a vector of AttachedAttribute to a string of a JSON
/// array of objects. Serialized output depends on the type of
/// AttachedAttribute.attribute( int when regular table / view, std::string when
/// late binding view) and AttachedAttribute.path(std::string or std::nullopt).
///
/// @example
/// input:
///       { {0, std::nullopt},
///         {1, "some"."nested"."path"},
///         {"col1", std::nullopt},
///         {"col2","another"."nested"."path"}
///       }
/// output:
///        "[0,
///         {"t":0,"column":1,"path":"\"some\".\"nested\".\"path\""},
///         {"t":0,"column":"col1"},
///         {"t":0,"column":"col2","path":"\"another\".\"nested\".\"path\""}]"
/// @param attached_attributes vector of AttachedAttribute.
/// @return std::string the serialized JSON string of array of objects.
std::string SerializeAttachedAttributes(
    const attribute_info_t attached_attributes);

/// @brief Constructs a vector of attributes (attribute description plus
/// optional SUPER path) from JSON-encoded array of numbers or objects.
///
/// @param json_string the JSON-encoded array.
/// @return a vector of attributes.
/// @throws XPGCHECK on invalid JSON input.
std::vector<AttachedAttribute> DeserializeAttachedAttributes(
    const std::string& json_string);

/// Constructs a vector of TypeName shared pointers from a JSON document
/// given as string. Expects JSON in the form
///
/// @example
///   [
///     {"typoid": 50, "typmod": 10},
///     {"typoid": 60, "typmod": 0},
///   ]
///
/// @param json_string the JSON-encoded array.
/// @return a vector of palloc'd shared_ptr TypeName.
/// @throws XPGCHECK on invalid JSON input.
std::vector<std::shared_ptr<TypeName>> DeserializeTypeNames(
    const std::string& json_string);

/// For the given the AttachedAttribute returns the column_name appended with
/// the path value(If exists).
/// @param attached_attribute input AttachedAttribute.
/// @param relid the OID of the relation the attached_attribute belongs to.
/// @return std::string value containing the column name with path value(If
/// exists)
std::string GetAttributeName(const AttachedAttribute& attached_attribute,
                             Oid relid);

/// For the given the AttachedAttribute returns the column_name.
/// @param attached_attribute input AttachedAttribute.
/// @param relid the OID of the relation the attached_attribute belongs to.
/// @return std::string value containing the column name.
std::string GetColumnNameFromAttachedAttribute(
    const ddm::AttachedAttribute& attached_attribute, Oid relid);
}  // namespace ddm
