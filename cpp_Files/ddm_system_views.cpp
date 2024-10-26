/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#include "ddm/ddm_system_views.h"

#include "ddm/ddm_permission.hpp"
#include "ddm/ddm_serialization.hpp"
#include "policy/policy_system_views.h"
#include "policy/policy_utils.h"
#include "policy/policy_utils.hpp"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "redcat/metadata_security.h"
#include "xen_utils/xen_except.h"

#include "pg/src/include/access/heapam.h"
#include "pg/src/include/access/tupdesc.h"
#include "pg/src/include/c.h"
#include "pg/src/include/catalog/namespace.h"
#include "pg/src/include/catalog/pg_attribute.h"
#include "pg/src/include/catalog/pg_datashare_objects.h"
#include "pg/src/include/custom_int_types.h"
#include "pg/src/include/nodes/makefuncs.h"
#include "pg/src/include/nodes/primnodes.h"
#include "pg/src/include/postgres.h"
#include "pg/src/include/postgres_ext.h"
#include "pg/src/include/storage/lmgr.h"
#include "pg/src/include/utils/builtins.h"
#include "pg/src/include/utils/lsyscache.h"
#include "pg/src/include/utils/palloc.h"
#include "pg/src/include/utils/rel.h"

#include <algorithm>
#include <iomanip>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <string.h>

namespace serialization = policy::serialization;

VarChar* policy_expression_out_internal(const VarChar* input) {
  check_policy_attach_privilege(kPolicyDDM);

  return policy_component_out_common(input, "expr", kPolicyDDM);
}

VarChar* mask_policy_attached_atts_out_internal(Oid relation_id,
                                                const VarChar* input) {
  check_policy_attach_privilege(kPolicyDDM);

  std::vector<ddm::AttachedAttribute> attributes;
  serialization::JSONDoc json_document;
  serialization::JSON json_array;
  std::function<serialization::JSON(const ddm::AttachedAttribute& attribute,
                                    serialization::JSONAllocator& allocator)>
      serializer;

  if (get_rel_relkind(relation_id) == RELKIND_VIEW &&
      get_view_kind(relation_id) == OBJTYPE_LBV) {
    attributes = ddm::DeserializeAttachedAttributes(
        std::string(VARDATA(input), VARSIZE(input) - VARHDRSZ));

    serializer = [](const ddm::AttachedAttribute& attribute,
                    serialization::JSONAllocator& allocator) {
      XPGCHECK(std::holds_alternative<std::string>(attribute.attribute));

      std::string column_path(std::get<std::string>(attribute.attribute));
      if (attribute.path.has_value()) {
        column_path += "." + attribute.path.value();
      }

      serialization::JSON json_value(rapidjson::kStringType);
      json_value.SetString(column_path.data(), allocator);
      return json_value;
    };
  } else {
    // Note NoLock here. We only need relation name here, and we never read or
    // write the tuples from the relation itself. Therefore, we don't care if
    // anything happens to the relation in the meantime.
    Relation relation = relation_open(relation_id, NoLock);
    SCOPE_EXIT_GUARD(relation_close(relation, NoLock));

    // @warning ddm::AttributesToColumnDefs XPGCHECKs that the attribute type is
    // int. For LBV you may not need to call ddm::AttributesToColumnDefs.
    attributes = ddm::AttributesToColumnDefs(
        relation, ddm::DeserializeAttachedAttributes(
                      std::string(VARDATA(input), VARSIZE(input) - VARHDRSZ)));

    serializer = [](const ddm::AttachedAttribute& attribute,
                    serialization::JSONAllocator& allocator) {
      XPGCHECK(std::holds_alternative<std::shared_ptr<ColumnDef>>(
          attribute.attribute));

      std::string column_path(
          std::get<std::shared_ptr<ColumnDef>>(attribute.attribute)->colname);
      if (attribute.path.has_value()) {
        column_path += "." + attribute.path.value();
      }

      serialization::JSON json_value(rapidjson::kStringType);
      json_value.SetString(column_path.data(), allocator);
      return json_value;
    };
  }

  json_array = serialization::SerializeToArray<ddm::AttachedAttribute>(
      attributes, serializer, json_document.GetAllocator());
  json_document.Swap(json_array);

  std::string output = serialization::JSONToString(json_document);

  VarChar* result = palloc(output.size() + VARHDRSZ);
  VARATT_SIZEP(result) = output.size() + VARHDRSZ;
  memcpy(VARDATA(result), output.c_str(), output.size());

  return result;
}

VarChar* get_ddm_policy_name_for_column(VarChar* schema_name,
                                        VarChar* relation_name,
                                        VarChar* column_name,
                                        VarChar* user_name,
                                        VarChar* role_name) {
  check_policy_attach_privilege(kPolicyDDM);

  XCHECK((user_name == nullptr) ^ (role_name == nullptr),
         policy::tag_for_policy_type(kPolicyDDM));

  // Convert input to null-terminated form.
  auto convert_varchar = [](VarChar* input) -> char* {
    int length = VARSIZE(input) - VARHDRSZ;
    char* result = palloc(length + 1);
    memcpy(result, VARDATA(input), length);
    *(result + length) = '\0';
    return result;
  };

  RangeVar* relation = makeRangeVar(convert_varchar(schema_name),
                                    convert_varchar(relation_name));
  Oid relation_id = RangeVarGetRelid(relation, true /* failOK */);
  if (relation_id == InvalidOid) return nullptr;

  if (!skip_metadata_security_check(GetUserId(), RelOid_pg_attribute,
                                    PG_CATALOG_NAMESPACE) &&
      !is_relation_owner_or_has_any_permission(relation_id, GetUserId()) &&
      pg_attribute_aclcheck(
          relation_id, convert_column_name(relation_id, column_name),
          GetUserId(), ACL_ALL_RIGHTS_COLUMNS) == ACLCHECK_NO_PRIV) {
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("column does not exist")));
  }

  // get_usesysid and get_roleid will abort query with error if the user or role
  // does not exist and there's nothing we can do aside from duplicating these
  // function's implementations.

  std::optional<AclId> user_id = std::nullopt;
  if (user_name != nullptr) {
    user_id = get_usesysid(convert_varchar(user_name));
    if (user_id == InvalidOid) return nullptr;
  }

  std::optional<AclId> role_id = std::nullopt;
  if (role_name != nullptr) {
    role_id = get_roleid(convert_varchar(role_name));
    if (role_id == InvalidOid) return nullptr;
  }

  std::unordered_set<std::string> policy_names =
      ddm::GetDDMPolicyNamesForColumn(
          relation_id, user_id, role_id,
          std::string(convert_varchar(column_name)));

  if (policy_names.empty()) return nullptr;

  std::string output = {};
  if (policy_names.size() == 1) {
    output = *policy_names.begin();
  } else {
    std::vector<std::string> policy_names_sorted(policy_names.begin(),
                                                 policy_names.end());
    std::sort(policy_names_sorted.begin(), policy_names_sorted.end());
    std::stringstream ss;
    ss << "[";
    bool first = true;
    for (auto&& name : policy_names_sorted) {
      if (!first) ss << ", ";
      ss << std::quoted(name, '\"', '\"');
      first = false;
    }
    ss << "]";
    output = ss.str();
  }
  VarChar* result = palloc(output.size() + VARHDRSZ);
  VARATT_SIZEP(result) = output.size() + VARHDRSZ;
  memcpy(VARDATA(result), output.data(), output.size());

  return result;
}
