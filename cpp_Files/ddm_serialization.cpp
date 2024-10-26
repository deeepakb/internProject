/// Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#include "ddm/ddm_serialization.hpp"

#include "ddm/constants.hpp"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"

#include "pg/src/include/utils/lsyscache.h"

namespace serialization = policy::serialization;
using serialization::JSON;
using serialization::JSONAllocator;
using serialization::JSONDoc;

namespace ddm {

// Attribute names.
const char* kAttributeColumnName = "colname";
const char* kAttributeExpression = "expr";
const char* kAttributeTypeOid = "typoid";
const char* kAttributeTypeMode = "typmod";
const char* kAttributeType = "t";
const char* kAttributeColumn = "column";
const char* kAttributePath = "path";

AttachedAttribute DeserializeAttachmentAttribute(const JSON& input);

std::string SerializeParsedPolicyExpressions(
    const std::vector<ParsedPolicyExpression>& expressions) {
  JSONDoc json_document;

  auto serializer = [](const ParsedPolicyExpression& expression,
                       JSONAllocator& allocator) {
    JSON json_value(rapidjson::kObjectType);

    serialization::SerializeValueInObject(json_value, kAttributeExpression,
                                          expression.expression, allocator);
    serialization::SerializeValueInObject(json_value, kAttributeTypeOid,
                                          expression.type_oid, allocator);
    serialization::SerializeValueInObject(json_value, kAttributeTypeMode,
                                          expression.type_mode, allocator);

    return json_value;
  };

  JSON json_value = serialization::SerializeToArray<ParsedPolicyExpression>(
      expressions, serializer, json_document.GetAllocator());

  json_document.Swap(json_value);

  return serialization::JSONToString(json_document);
}

std::vector<ParsedPolicyExpression> DeserializeParsedPolicyExpressions(
    const std::string& json_string) {
  // Custom deserializer that extracts and constructs ParsedPolicyExpression.
  auto deserializer = [](const JSON& input) {
    std::string expr = serialization::DeserializeValueInObject<std::string>(
        input, kAttributeExpression);
    int atttypid =
        serialization::DeserializeValueInObject<int>(input, kAttributeTypeOid);
    int atttypmod =
        serialization::DeserializeValueInObject<int>(input, kAttributeTypeMode);

    return ParsedPolicyExpression{expr, static_cast<Oid>(atttypid), atttypmod};
  };

  return serialization::DeserializeArray<ParsedPolicyExpression>(json_string,
                                                                 deserializer);
}

std::string SerializeColumnDefs(
    const std::vector<std::shared_ptr<ColumnDef>>& column_definitons) {
  JSONDoc json_document;

  auto serializer = [](const std::shared_ptr<ColumnDef>& column_definition,
                       JSONAllocator& allocator) {
    JSON json_value(rapidjson::kObjectType);

    serialization::SerializeValueInObject(json_value, kAttributeColumnName,
                                          column_definition->colname,
                                          allocator);
    serialization::SerializeValueInObject(
        json_value, kAttributeTypeOid,
        typenameTypeId(column_definition->typname), allocator);
    serialization::SerializeValueInObject(json_value, kAttributeTypeMode,
                                          column_definition->typname->typmod,
                                          allocator);

    return json_value;
  };

  JSON json_value = serialization::SerializeToArray<std::shared_ptr<ColumnDef>>(
      column_definitons, serializer, json_document.GetAllocator());

  json_document.Swap(json_value);

  return serialization::JSONToString(json_document);
}

std::vector<std::shared_ptr<ColumnDef>> DeserializeColumnDefs(
    const std::string& json_string) {
  // Deserializer that extracts and constructs ColumnDef.
  auto deserializer = [](const JSON& input) {
    std::string attname = serialization::DeserializeValueInObject<std::string>(
        input, kAttributeColumnName);
    Oid atttypid =
        serialization::DeserializeValueInObject<int>(input, kAttributeTypeOid);
    int atttypmod =
        serialization::DeserializeValueInObject<int>(input, kAttributeTypeMode);

    return makeColumnDef(attname.c_str(), atttypid, atttypmod);
  };

  return serialization::DeserializeArrayToPointers<ColumnDef>(json_string,
                                                              deserializer);
}

std::string SerializeAttributeNumbers(
    const std::vector<int> attribute_numbers) {
  JSONDoc json_document;

  JSON json_value = serialization::SerializeToArray<int>(
      attribute_numbers,
      [](int attribute_number, JSONAllocator& allocator UNUSED) {
        return JSON(attribute_number);
      },
      json_document.GetAllocator());

  json_document.Swap(json_value);

  return serialization::JSONToString(json_document);
}

std::string SerializeAttributeNames(
    const std::vector<std::string> attribute_names) {
  JSONDoc json_document;

  JSON json_value = serialization::SerializeToArray<std::string>(
      attribute_names,
      [](std::string attribute_name, JSONAllocator& allocator) {
        JSON json_value(rapidjson::kObjectType);
        serialization::SerializeValueInObject(
            json_value, kAttributeType,
            static_cast<int>(ddm::AttachedAttributeType::kLBVsPaths),
            allocator);
        serialization::SerializeValueInObject(json_value, kAttributeColumn,
                                              attribute_name, allocator);
        return json_value;
      },
      json_document.GetAllocator());

  json_document.Swap(json_value);

  return serialization::JSONToString(json_document);
}

std::string SerializeAttachedAttributes(
    const attribute_info_t attached_attributes) {
  JSONDoc json_document;

  JSON json_val_serialized_attached_attrs = serialization::SerializeToArray<
      AttachedAttribute>(
      attached_attributes,
      [](AttachedAttribute attached_attribute, JSONAllocator& allocator) {
        if (std::holds_alternative<int>(attached_attribute.attribute) &&
            !attached_attribute.path) {
          // Attached attribute is from a table or regular view and does not
          // contain path value.
          JSON json_value(rapidjson::kNumberType);
          json_value.SetInt(std::get<int>(attached_attribute.attribute));

          return json_value;
        }

        JSON json_value(rapidjson::kObjectType);

        serialization::SerializeValueInObject(
            json_value, kAttributeType,
            static_cast<int>(ddm::AttachedAttributeType::kLBVsPaths),
            allocator);

        if (std::holds_alternative<int>(attached_attribute.attribute)) {
          serialization::SerializeValueInObject(
              json_value, kAttributeColumn,
              std::get<int>(attached_attribute.attribute), allocator);
        } else {
          // Attached attribute is from a late binding view.
          XCHECK(std::holds_alternative<std::string>(
              attached_attribute.attribute));
          serialization::SerializeValueInObject(
              json_value, kAttributeColumn,
              std::get<std::string>(attached_attribute.attribute), allocator);
        }

        if (attached_attribute.path) {
          serialization::SerializeValueInObject(json_value, kAttributePath,
                                                attached_attribute.path.value(),
                                                allocator);
        }

        return json_value;
      },
      json_document.GetAllocator());

  json_document.Swap(json_val_serialized_attached_attrs);

  return serialization::JSONToString(json_document);
}

/// @brief Constructs a single AttachedAttribute object (attribute description
/// plus optional SUPER path) from JSON-encoded object.
///
/// Supports both old and new encoding of attachments (not the type of
/// attachment is std::variant to accommodate different possible attachment C++
/// types).
///
/// For the reference, here are the examples of old and new encodings.
/// @example old encoding: plain number
/// - 5
///
/// @example new encoding, object
/// - note, table / view w/o path uses old encoding
/// - table / view w/ path: { t: kLBVsPaths, column: 5, path: "a.b.c" }
/// - LBV w/ path: { t: kLBVsPaths, column: "column_a" }
/// - LBV w/o path: { t: kLBVsPaths, column: "column_a", path: "a.b.c" }
///
/// @param input the JSON object of the attached attribute (w/ optional path)
/// @return AttachedAttribute the object of attached attribute (attribute
/// description plus optional SUPER path)
/// @throws XPGCHECK on invalid JSON input or invalid object type (the
/// kAttributeType attribute in JSON object).
AttachedAttribute DeserializeAttachmentAttribute(const JSON& input) {
  if (input.IsObject()) {
    // New encoding scheme -- array of objects where each object has an
    // explicit type, a column (int attribute or string name) and an optional
    // SUPER path.
    int type =
        serialization::DeserializeValueInObject<int>(input, kAttributeType);

    AttachedAttribute::Attribute attribute;

    switch (type) {
      case AttachedAttributeType::kLBVsPaths: {
        if (serialization::CheckTypeInObject<int>(input, kAttributeColumn)) {
          attribute = serialization::DeserializeValueInObject<int>(
              input, kAttributeColumn);
        } else if (serialization::CheckTypeInObject<std::string>(
                       input, kAttributeColumn)) {
          attribute = serialization::DeserializeValueInObject<std::string>(
              input, kAttributeColumn);
        }

        std::optional<std::string> path =
            serialization::DeserializeValueInObjectOpt<std::string>(
                input, kAttributePath);

        return {attribute, path};
      } break;
    }
    // For now kLBVsPaths is the only supported (and expected) type tag. The
    // block is not in switch-default to resolve GCC -Werror=return-type .
    XCHECK_UNREACHABLE("Unexpected type tag: ", type);
  } else {
    // Old encoding scheme -- array of integers where each integer is an
    // attribute number for a table or view.
    XPGCHECK(input.IsInt());

    return {serialization::DeserializeInt(input), std::nullopt};
  }
}

std::vector<AttachedAttribute> DeserializeAttachedAttributes(
    const std::string& json_string) {
  if (json_string.empty()) return {};

  return serialization::DeserializeArray<AttachedAttribute>(
      json_string, DeserializeAttachmentAttribute);
}

std::vector<std::shared_ptr<TypeName>> DeserializeTypeNames(
    const std::string& json_string) {
  // Custom deserializer that extracts and constructs TypeName.
  auto deserializer = [](const JSON& input) {
    Oid atttypid =
        serialization::DeserializeValueInObject<int>(input, kAttributeTypeOid);
    int atttypmod =
        serialization::DeserializeValueInObject<int>(input, kAttributeTypeMode);

    return makeTypeNameFromOid(atttypid, atttypmod);
  };

  return serialization::DeserializeArrayToPointers<TypeName>(json_string,
                                                             deserializer);
}

std::string GetAttributeName(const AttachedAttribute& attached_attribute,
                             Oid relid) {
  std::string attr_name =
      GetColumnNameFromAttachedAttribute(attached_attribute, relid);
  if (attached_attribute.path.has_value()) {
    attr_name =
        attr_name + PathSetBase::kDelimiter + attached_attribute.path.value();
  }

  return attr_name;
}

std::string GetColumnNameFromAttachedAttribute(
    const AttachedAttribute& attached_attribute, Oid relid) {
  std::string attr_name;

  if (IsLbv(relid)) {
    XCHECK(std::holds_alternative<std::string>(attached_attribute.attribute));
    return std::get<std::string>(attached_attribute.attribute);
  }

  XCHECK(std::holds_alternative<int>(attached_attribute.attribute));
  return GetColumnName(relid, std::get<int>(attached_attribute.attribute));
}
}  // namespace ddm
