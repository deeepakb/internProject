/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#include "ddm/ddm_permission.hpp"

#include "ddm/constants.hpp"
#include "ddm/ddm_permission.h"
#include "ddm/ddm_policy.hpp"
#include "ddm/ddm_serialization.hpp"
#include "ddm/unsupported.h"
#include "fmgroids.h"  // NOLINT(build/include_subdir)
#include "policy/policy_utils.h"
#include "policy/policy_utils.hpp"
#include "redcat/redcat_role.h"
#include "sys/pg_defs.hpp"
#include "sys/pg_utils.hpp"
#include "sys/stats_indicator.hpp"
#include "xen_utils/scope_exit_guard.hpp"
#include "xen_utils/xen_except.h"

#include "pg/src/backend/catalog/permission.hpp"
#include "pg/src/backend/catalog/pg_class_extended.hpp"
#include "pg/src/include/access/genam.h"
#include "pg/src/include/access/heapam.h"
#include "pg/src/include/access/htup.h"
#include "pg/src/include/access/skey.h"
#include "pg/src/include/access/xact.h"
#include "pg/src/include/c.h"
#include "pg/src/include/catalog/catname.h"
#include "pg/src/include/catalog/indexing.h"
#include "pg/src/include/catalog/pg_class.h"
#include "pg/src/include/catalog/pg_class_extended.h"
#include "pg/src/include/catalog/pg_datashare_objects.h"
#include "pg/src/include/catalog/pg_permission.h"
#include "pg/src/include/catalog/pg_permission_mask.h"
#include "pg/src/include/catalog/pg_policy_mask.h"
#include "pg/src/include/commands/user.h"
#include "pg/src/include/fmgr.h"
#include "pg/src/include/miscadmin.h"
#include "pg/src/include/nodes/parsenodes.h"
#include "pg/src/include/nodes/pg_list.h"
#include "pg/src/include/postgres.h"
#include "pg/src/include/postgres_ext.h"
#include "pg/src/include/storage/lmgr.h"
#include "pg/src/include/utils/acl.h"
#include "pg/src/include/utils/builtins.h"
#include "pg/src/include/utils/catcache.h"
#include "pg/src/include/utils/elog.h"
#include "pg/src/include/utils/elog_sqlstate.h"
#include "pg/src/include/utils/lsyscache.h"
#include "pg/src/include/utils/rel.h"
#include "pg/src/include/utils/syscache.h"
#include "pg/src/include/utils/tqual.h"

#include <boost/functional/hash.hpp>

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

extern bool gconf_ddm_enable_identical_attachments;
extern bool gconf_ddm_enable_for_datashares;

namespace ddm {
namespace {
/// Create a DDM permission for the relation based on PermissionEntry.
/// @see GetOrCreatePermission.
/// @param perm_entry PermissionEntry of the policy.
/// @return A newly created permission id if successful else InvalidOid.
Oid CreateDDMPolicyPermission(PermissionEntry const& perm_entry) {
  PrivPermMap map;
  int hashed_val = perm_hash_value(perm_entry.key);
  std::pair<Oid, bool> perm_pair = GetOrCreatePermission(
      true /* create */, MyDatabaseId, RelOid_pg_class, perm_entry.relId,
      hashed_val /* objsubid */, DDM_POLICY_PRIV_TYPE, perm_entry.polId, &map);
  XCHECK(OidIsValid(perm_pair.first));
  return perm_pair.first;
}

/// Undo DoGrant specifically for DDM.
/// To cleanup a corresponding permission entry for a grantee, we drop the
/// tuple from both pg_user_permission and pg_permission_users catalog when the
/// grantee is a user or PUBLIC and from pg_role_permission and
/// pg_permission_roles if the grantee is a Role.
/// @param permid Unique permission ID to delete.
/// @param grantee PolicyGrantee for deleting the corresponding tuples.
void RevokeDDMPermissions(const Oid& permid, PolicyGrantee const& grantee) {
  if (!OidIsValid(permid)) {
    return;
  }

  switch (grantee.type) {
    case kGranteeRole: {
      List* role_ids = nullptr;

      Relation pg_permission_roles =
          heap_openr(PermissionRolesRelationName, RowExclusiveLock);
      SCOPE_EXIT_GUARD(relation_close(pg_permission_roles, RowExclusiveLock));

      Relation pg_role_permission =
          heap_openr(RolePermissionRelationName, RowExclusiveLock);
      SCOPE_EXIT_GUARD(relation_close(pg_role_permission, RowExclusiveLock));

      role_ids = lappend_int(role_ids, grantee.id);
      // Delete records from pg_role_permission and pg_permission_roles.
      // Missing role id case is gracefully handled by RBAC API.
      UpsertPermRoles(pg_permission_roles, pg_role_permission, ACL_MODECHG_DEL,
                      permid, FLAG_NO_RIGHTS, GetUserId(), role_ids);
      break;
    }
    case kGranteeUser:
    case kGranteePublic: {
      List* user_ids = nullptr;

      Relation pg_permission_users =
          heap_openr(PermissionUsersRelationName, RowExclusiveLock);
      SCOPE_EXIT_GUARD(relation_close(pg_permission_users, RowExclusiveLock));

      Relation pg_user_permission =
          heap_openr(UserPermissionRelationName, RowExclusiveLock);
      SCOPE_EXIT_GUARD(relation_close(pg_user_permission, RowExclusiveLock));

      user_ids = lappend_int(user_ids, grantee.id);
      // Delete records from pg_user_permission and pg_permission_users.
      // Missing user id case is gracefully handled by RBAC API.
      UpsertPermUsers(pg_permission_users, pg_user_permission, ACL_MODECHG_DEL,
                      permid, FLAG_NO_RIGHTS, GetUserId(), GetUserId(),
                      user_ids);
      break;
    }
  }
}

/// Check that DDM policy can be attached or detached on relation to given
/// PolicyGrantee.
/// Details on validation requirements are mentioned in DDM Design doc:
/// https://quip-amazon.com/ECuoAEd3bVzo/
/// Redshift-Dynamic-Data-Masking-DDM-Design#temp:C:bLa6e96a02231fb44a09865791e0
/// @param perm_entry PermissionEntry of the policy.
/// @param pol_name Name of the DDM policy.
/// @param in_attribute_info attribute_info_t for the input columns of the
/// policy.
/// @param out_attribute_info attribute_info_t for the output columns of the
/// policy.
/// @param rel_name Name of the relation.
/// @param grantee PolicyGrantee for attaching DDM policy.
/// @param is_detach Flag to denote if it's ATTACH/DETACH statement.
/// @return optional(perm_id) if duplicate policy(same policyname, inattnums,
/// outattnums, priority) already attached. Otherwise nullopt.
std::optional<AclId> CheckDDMPolicyOnRel(
    PermissionEntry const& perm_entry, const std::string& pol_name,
    attribute_info_t const& in_attribute_info,
    attribute_info_t const& out_attribute_info, const std::string& rel_name,
    PolicyGrantee const& grantee, bool is_detach) {
  Oid relid = perm_entry.relId;
  int4 priority = perm_entry.key.priority;
  switch (grantee.type) {
    case kGranteeRole: {
      // Check for role.
      AclId role_id = grantee.id;
      // Check that role exists.
      if (!RoleExists(role_id)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("role \"%d\" does not exist", role_id)));
      }
      break;
    }
    case kGranteeUser: {
      // Checks for user.
      AclId user_id = grantee.id;
      if (AclIdIsValid(user_id)) {
        // Check that user exists.
        if (!DoesUserExist(user_id)) {
          ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                          errmsg("user \"%d\" does not exist", user_id)));
        }
      }
      break;
    }
    case kGranteePublic: {
      // No need to verify the user id for public as it's set to Invalid id.
      break;
    }
  }

  std::string err_policy_name;
  Oid err_polid, permid;
  std::optional<ddm::AttachedAttribute> err_attribute;
  bool is_identical_attachment = false;
  // There can be several policies attached to a relation with different
  // priority on each column. We iterate over pg_permission_mask with the
  // help of PermissionMaskRelPriorityIndex based on relation and priority
  // to fetch attached policies. We check over individual columns to see if
  // there is an identical attached policy(same relation, input attinfo,
  // output attinfo, priority) and, in case of no identical attached policy,
  // is there an attached policy with equal priority and at least one common
  // output attinfo. If an identical policy attachment is found and grantee is
  // not attached to that identical attachment, we allow attachment.
  // If no identical attached policy is found, with the help of
  // PermissionPrivIndex, we also check if a conflicting attached policy with
  // same output attrs is found(same relation, output attinfo and priority).

  // Get Oid of the attached DDM policy, the output column for specified
  // priority and if an identical attachment is found.
  Oid polid = GetDDMPolicyId(pol_name.c_str());
  std::tie(err_polid, permid, err_attribute, is_identical_attachment) =
      GetAttachedPolicyInfo(polid, relid, in_attribute_info, out_attribute_info,
                            priority);

  if (gconf_ddm_enable_identical_attachments && is_identical_attachment) {
    if (!is_detach) {
      // If grantee is already attached to the identical attachment, attachment
      // is not allowed and outputs error.
      if (IsPermissionAttached(permid, grantee)) {
        err_policy_name = GetDDMPolicyName(err_polid);
        ReportIdenticalPolicyAttachedToGranteeError(err_policy_name, rel_name,
                                                    grantee);
      }
      // If identical attachment is found and, current request is for attachment
      // and, grantee is not attached to that identical attachment, then
      // attachment is allowed. Returns true.
      return permid;
    }
  } else {
    // If no identical attachment is found, we check if a conflicting policy
    // attachment with the same output attrs is present.
    Oid confilicting_polid = GetPolicyAttachedToRel(perm_entry);
    if (confilicting_polid != InvalidOid) {
      err_polid = confilicting_polid;
      err_attribute = std::nullopt;
    }
  }

  // Store the policy name for error reporting based on ATTACH or DETACH
  // statement.
  if (err_polid != InvalidOid) {
    err_policy_name = GetDDMPolicyName(err_polid);
  }

  bool is_already_attached = (err_polid != InvalidOid);

  // If it's ATTACH statement and any DDM policy is already attached to the
  // grantee with given priority, and no identical policy is there, throw an
  // error.
  if (!is_detach && is_already_attached) {
    if (err_attribute.has_value()) {
      // `err_attribute` is searched and set with the help of
      // GetAttachedPolicyInfo(). An existing value for `err_attribute`
      // indicates that a conflicting policy with same priority is attached on
      // that err_attribute.
      ereport(ERROR,
              (errcode(ERRCODE_INVALID_GRANT_OPERATION),
               errmsg("DDM policy \"%s\" is already attached on relation "
                      "\"%s\" column \"%s\" with same priority",
                      err_policy_name.c_str(), rel_name.c_str(),
                      GetAttributeName(err_attribute.value(), relid).c_str())));
    }
    // `err_attribute` without having values means a conflicting policy with
    // same priority and same output attrs is already attached.
    ereport(ERROR, (errcode(ERRCODE_INVALID_GRANT_OPERATION),
                    errmsg("DDM policy \"%s\" is already attached on relation "
                           "\"%s\" for given column with same priority",
                           err_policy_name.c_str(), rel_name.c_str())));
  }

  // If it's DETACH and policy is not attached to the grantee, throw an
  // error.
  if (is_detach && !is_already_attached) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_GRANT_OPERATION),
                    errmsg("DDM policy \"%s\" is not attached on relation "
                           "\"%s\"",
                           pol_name.c_str(), rel_name.c_str())));
  }
  if (!is_identical_attachment) {
    return std::nullopt;
  }
  return permid;
}
}  // namespace

std::unordered_set<std::string> GetDDMPolicyNamesForColumn(
    Oid relation_id, std::optional<AclId> user_id, std::optional<AclId> role_id,
    const std::string column_name) {
  // This function requires a filter over one of user or role. Note that PUBLIC
  // is viewed as a user with ID InvalidAclId.
  PGCHECK((user_id.has_value()) ^ (role_id.has_value()),
          "%s, user_id set: %d, role_id set: %d",
          policy::tag_for_policy_type(kPolicyDDM).c_str(), user_id.has_value(),
          role_id.has_value());

  // Fetch the attachments for the given filter.
  std::set<Attachment> attachments =
      FetchAttachmentsForRel(relation_id, user_id, role_id);

  if (attachments.empty()) return {};

  std::set<ParsedPolicyAttachment> parsed_attachments;

  // Parse the attachments in the functional style.
  std::transform(attachments.begin(), attachments.end(),
                 std::inserter(parsed_attachments, parsed_attachments.begin()),
                 [](Attachment attachment) -> ParsedPolicyAttachment {
                   return ParseAttachment(attachment);
                 });

  // Build a map of column names to a relevant (highest priority) attachment
  // (and some metadata), and take the entry for the requested column name.
  MaskMap column_map =
      BuildMaskMap(parsed_attachments, {} /* obj_transform_alp_ctx */);

  auto map_it = column_map.find(column_name);
  if (map_it == column_map.end()) return {};

  // Extract the policy name from the map entry.
  std::unordered_set<std::string> policy_names;
  std::transform(
      map_it->second.begin(), map_it->second.end(),
      std::inserter(policy_names, policy_names.begin()),
      [](MergeEntry entry) -> std::string {
        XCHECK(entry.attachment != nullptr);
        return std::string(NameStr(entry.attachment->pol.pol.polname));
      });

  return policy_names;
}

int perm_hash_value(const PermissionKey& key) {
  std::size_t hash = 0;
  boost::hash_combine(hash, boost::hash_value(key.priority));
  boost::hash_combine(hash, boost::hash_value(key.attrsOut));
  return static_cast<int>(hash);
}

bool IsPermissionAttached(Oid permId, PolicyGrantee const& grantee) {
  if (!OidIsValid(permId)) {
    return false;
  }
  switch (grantee.type) {
    case kGranteeRole: {
      RoleList* roles = get_perm_rolelist(permId);
      RoleItem* rip = search_rolelist(roles, grantee.id);
      return rip != nullptr;
    }
    case kGranteeUser:
    case kGranteePublic: {
      UserList* users = get_perm_userlist(permId);
      UserItem* uip = search_userlist(users, grantee.id);
      return uip != nullptr;
    }
  }

  return false;
}

bool IsPermissionAttached(Oid permId) {
  if (!OidIsValid(permId)) {
    return false;
  }
  RoleList* roles = get_perm_rolelist(permId);
  if (roles && USERLIST_NUM(roles) > 0) {
    return true;
  }
  UserList* users = get_perm_userlist(permId);
  if (users && USERLIST_NUM(users) > 0) {
    return true;
  }
  return false;
}

Oid GetPolicyAttachedToRel(PermissionEntry const& perm_entry) {
  int hashed_val = perm_hash_value(perm_entry.key);
  // Get all policies applicable on the relation.
  CatCList* cat_list = SearchSysCacheExtendedList(
      PERMPRIV, 5, MyDatabaseId, RelOid_pg_class, perm_entry.relId, hashed_val,
      DDM_POLICY_PRIV_TYPE, 0);
  SCOPE_EXIT_GUARD(ReleaseSysCacheList(cat_list));
  XCHECK(cat_list != nullptr, "DDM");
  // Filter out policies applicable on the given user.
  for (int i = 0; i < cat_list->n_members; ++i) {
    HeapTuple tup = &cat_list->members[i]->tuple;
    if (!HeapTupleIsValid(tup)) {
      elog(ERROR, "could not find tuple for relation %u", perm_entry.relId);
    }
    Immutable_pg_permission form =
        reinterpret_cast<Immutable_pg_permission>(GETSTRUCT(tup));
    return form->privid;
  }
  return InvalidOid;
}

attached_policy_info_t GetAttachedPolicyInfo(Oid polid_to_attach, Oid relid,
                                             attribute_info_t const& in_attrs,
                                             attribute_info_t const& out_attrs,
                                             int4 priority) {
  std::optional<ddm::AttachedAttribute> matched_outatt;
  std::optional<std::string> matched_path;
  std::unordered_map<std::string, PathSet> out_colname_to_pathset_map;
  Oid matched_polid = InvalidOid;
  Oid matched_permid = InvalidOid;

  ScanKeyData key[2];
  SysScanDesc scan;
  HeapTuple tup;
  int nkeys;

  // Populate out_colname_to_pathset_map with the output column names and
  // corresponding path values from the output AttachedAttributes. We use this
  // map to identify any output attribute that conflicts with an already
  // existing attachment's output attribute.
  for (AttachedAttribute attached_attribute : out_attrs) {
    std::string colname =
        GetColumnNameFromAttachedAttribute(attached_attribute, relid);
    if (attached_attribute.path.has_value()) {
      std::string_view string_view_path = {attached_attribute.path.value()};
      out_colname_to_pathset_map[colname].Add(string_view_path);
    } else {
      out_colname_to_pathset_map[colname];
    }
  }

  Relation pg_permission_mask =
      heap_openr(PermissionMaskRelationName, AccessShareLock);
  SCOPE_EXIT_GUARD(heap_close(pg_permission_mask, AccessShareLock));

  ScanKeyInit(&key[0], Anum_pg_permission_mask_relid, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(relid));
  if (priority >= 0) {
    nkeys = 2;
    ScanKeyInit(&key[1], Anum_pg_permission_mask_polpriority,
                BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(priority));
  } else {
    nkeys = 1;
  }
  scan = systable_beginscan(pg_permission_mask, PermissionMaskRelPriorityIndex,
                            true, SnapshotNow, nkeys, key);
  SCOPE_EXIT_GUARD(systable_endscan(scan));

  bool is_identical_attachment;
  while (HeapTupleIsValid(tup = systable_getnext(scan))) {
    is_identical_attachment =
        CompareWithAttachment(pg_permission_mask, tup, polid_to_attach, relid,
                              in_attrs, out_attrs, out_colname_to_pathset_map,
                              matched_outatt, matched_polid, matched_permid);
    if (is_identical_attachment) {
      return {matched_polid, matched_permid, std::nullopt /* matched_outatt */,
              true /* is_identical_attachment */};
    }
  }

  if (matched_outatt) {
    return {matched_polid, matched_permid, matched_outatt,
            false /* is_identical_attachment */};
  }

  // If no identitcal attachment is found and no attachment on any specified
  // columns of the relation, return InvalidOid for found Permission ID and
  // Policy ID.
  return {InvalidOid, InvalidOid, std::nullopt,
          false /* is_identical_attachment */};
}

bool CompareWithAttachment(
    Relation pg_permission_mask, HeapTuple tup, Oid polid_to_attach, Oid relid,
    attribute_info_t const& in_attinfo, attribute_info_t const& out_attinfo,
    std::unordered_map<std::string, PathSet>& out_colname_to_pathset_map,
    std::optional<ddm::AttachedAttribute>& matched_outatt, Oid& matched_polid,
    Oid& matched_permid) {
  Immutable_pg_permission_mask ptup =
      reinterpret_cast<Immutable_pg_permission_mask>(GETSTRUCT(tup));
  Oid polid = ptup->polid;
  Oid permid = ptup->permid;

  bool is_outattrs_null = false;
  Datum datum_outattrs =
      fastgetattr(tup, Anum_pg_permission_mask_polattrsout,
                  RelationGetDescr(pg_permission_mask), &is_outattrs_null);
  if (is_outattrs_null) {
    return false;
  }
  char* output_outattrs =
      DatumGetCString(DirectFunctionCall1(textout, datum_outattrs));
  XCHECK(output_outattrs != nullptr);

  attribute_info_t parsed_out_attached_atts =
      ddm::DeserializeAttachedAttributes(output_outattrs);

  bool is_inattrs_null = false;
  Datum datum_inattrs =
      fastgetattr(tup, Anum_pg_permission_mask_polattrsin,
                  RelationGetDescr(pg_permission_mask), &is_inattrs_null);
  if (gconf_ddm_enable_identical_attachments && polid_to_attach == polid &&
      !is_inattrs_null && out_attinfo == parsed_out_attached_atts) {
    char* output_inattrs =
        DatumGetCString(DirectFunctionCall1(textout, datum_inattrs));
    XCHECK(output_inattrs != nullptr);
    attribute_info_t parsed_in_attached_atts =
        ddm::DeserializeAttachedAttributes(output_inattrs);

    // If the in_attinfo and out_attinfo  are equal with the
    // parsed_in_attached_atts and parsed_out_attached_atts, an identical policy
    // is already attached and we return true.
    if (in_attinfo == parsed_in_attached_atts) {
      matched_polid = polid;
      matched_permid = permid;
      return true;
    }
  }

  if (matched_outatt.has_value()) return false;

  for (ddm::AttachedAttribute attached_attribute : parsed_out_attached_atts) {
    if (IsConflictingAttachedAttribute(attached_attribute,
                                       out_colname_to_pathset_map, relid)) {
      matched_outatt = attached_attribute;
      matched_polid = polid;
      matched_permid = permid;
      break;
    }
  }

  return false;
}

bool IsConflictingAttachedAttribute(
    const ddm::AttachedAttribute& attached_attribute,
    const std::unordered_map<std::string, PathSet>& colname_to_pathset_map,
    Oid relid) {
  std::string colname =
      GetColumnNameFromAttachedAttribute(attached_attribute, relid);
  auto attrs_map_it = colname_to_pathset_map.find(colname);

  if (attrs_map_it == colname_to_pathset_map.end()) return false;

  // if AttachedAttribute has path, we also need to check if the path is
  // contained (fully / partically) to be a conflicting column.
  if (attached_attribute.path) {
    RawPath path(attached_attribute.path->data(),
                 attached_attribute.path->size());
    std::vector<std::string_view> components(path.begin(), path.end());
    PathSet::PathMembership membership =
        attrs_map_it->second.CheckPathMembership(components);

    if (membership == PathSet::PathMembership::kNotContained) return false;
  }

  return true;
}

std::unordered_set<Oid> GetAttachedPermsForRelOutput(
    PermissionEntry const& permEntry, PolicyGrantee const& grantee) {
  ScanKeyData key[2];
  SysScanDesc scan;
  HeapTuple tup;
  std::unordered_set<Oid> permIds;

  /* Grab an appropriate lock on the pg_permission_mask relation */
  Relation pg_permission_mask =
      heap_openr(PermissionMaskRelationName, RowExclusiveLock);
  SCOPE_EXIT_GUARD(heap_close(pg_permission_mask, RowExclusiveLock));

  ScanKeyInit(&key[0], Anum_pg_permission_mask_relid, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(permEntry.relId));
  Datum outputAttNumDatum = DirectFunctionCall1(
      textin, CStringGetDatum(permEntry.key.attrsOut.c_str()));
  ScanKeyInit(&key[1], Anum_pg_permission_mask_polattrsout,
              BTEqualStrategyNumber, F_TEXTEQ, outputAttNumDatum);
  scan = systable_beginscan(pg_permission_mask, PermissionMaskRelAttrsoutIndex,
                            true, SnapshotNow, 2 /* nkeys */, key);
  SCOPE_EXIT_GUARD(systable_endscan(scan));

  while (HeapTupleIsValid(tup = systable_getnext(scan))) {
    Immutable_pg_permission_mask ptup =
        reinterpret_cast<Immutable_pg_permission_mask>(GETSTRUCT(tup));
    // Check whether policy ID matches with deleting policy.
    if (ptup->polid != permEntry.polId) {
      continue;
    }
    // Check whether permission is attached to the grantee.
    if (!IsPermissionAttached(ptup->permid, grantee)) {
      continue;
    }
    permIds.insert(ptup->permid);
  }
  return permIds;
}

PolicyGrantee ResolvePolicyGrantee(const PolicyPrivGrantee* grantee) {
  PolicyGrantee resolvedGrantee;
  if (grantee->sharename != nullptr) {
    // Datashare is not supported.
    XCHECK(grantee->username == nullptr);
    LogPolicyUnsupportedFeature("Datashare", PolicyType::kPolicyDDM);
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("attaching a policy to a datashare is not supported.")));
  }

  if (grantee->rolename != nullptr) {
    char* authname = grantee->rolename;
    AclId authid = get_roleid(authname);
    XCHECK(authid != InvalidAclId);
    // Reserved roles such as sys:dba or sys:operator are not supported.
    if (IsRoleReserved(authname)) {
      ereport(ERROR, (errcode(ERRCODE_INVALID_GRANT_OPERATION),
                      errmsg("cannot attach policy to reserved role \"%s\"",
                             authname)));
    }
    resolvedGrantee.type = GranteeType::kGranteeRole;
    resolvedGrantee.id = authid;
    resolvedGrantee.name = authname;
    return resolvedGrantee;
  }

  if (grantee->username != nullptr) {
    char* authname = grantee->username;
    AclId authid = get_usesysid(authname);
    XCHECK(authid != InvalidAclId);
    if (superuser_arg(authid)) {
      ereport(ERROR,
              (errcode(ERRCODE_INVALID_GRANT_OPERATION),
               errmsg("cannot attach policy to superuser \"%s\"", authname)));
    }
    resolvedGrantee.type = GranteeType::kGranteeUser;
    resolvedGrantee.id = authid;
    resolvedGrantee.name = authname;
  } else {
    resolvedGrantee.type = GranteeType::kGranteePublic;
    resolvedGrantee.id = InvalidAclId;
    resolvedGrantee.name = nullptr;
  }
  return resolvedGrantee;
}

std::pair<Oid, bool> AttachPolicyOnRel(
    PermissionEntry const& perm_entry, std::string pol_name,
    attribute_info_t const& in_attribute_info,
    attribute_info_t const& out_attribute_info, std::string rel_name,
    PolicyGrantee const& grantee) {
  std::optional<AclId> existing_attachment = CheckDDMPolicyOnRel(
      perm_entry, pol_name, in_attribute_info, out_attribute_info, rel_name,
      grantee, false /* isDetach */);

  Oid permid = InvalidAclId;
  if (existing_attachment.has_value()) {
    permid = existing_attachment.value();
  } else {
    permid = ddm::CreateDDMPolicyPermission(perm_entry);
  }
  XCHECK(permid != InvalidAclId, DDM_CALLER);
  List* role_ids = nullptr;
  List* user_ids = nullptr;
  switch (grantee.type) {
    case kGranteeRole: {
      role_ids = lappend_int(role_ids, grantee.id);
      break;
    }
    case kGranteeUser:
    case kGranteePublic: {
      user_ids = lappend_int(user_ids, grantee.id);
      break;
    }
  }

  // Grant permissions to users and roles on the relation.
  DoGrant(true /* is_grant */, false /* grant_option */, permid, GetUserId(),
          GetUserId(), user_ids, role_ids, false /* is_clp */,
          InvalidOid /* rel_permid */, InvalidAclId /* privid */,
          nullptr /* qualified_col_name */);

  return std::make_pair(permid, existing_attachment.has_value());
}

std::unordered_set<Oid> DetachDDMPolicyOnRel(
    PermissionEntry const& perm_entry, std::string pol_name,
    attribute_info_t const& in_attribute_info,
    attribute_info_t const& out_attribute_info, std::string rel_name,
    PolicyGrantee const& grantee) {
  ddm::CheckDDMPolicyOnRel(perm_entry, pol_name, in_attribute_info,
                           out_attribute_info, rel_name, grantee,
                           true /* isDetach */);

  std::unordered_set<Oid> permIds =
      GetAttachedPermsForRelOutput(perm_entry, grantee);
  if (permIds.empty()) {
    const char* columnText =
        (out_attribute_info.size() > 1) ? "columns" : "column";
    const char* granteeName = grantee.name == nullptr ? "PUBLIC" : grantee.name;
    ereport(
        ERROR,
        (errcode(ERRCODE_INVALID_GRANT_OPERATION),
         errmsg("DDM policy \"%s\" is not attached to \"%s\" on specified %s",
                pol_name.c_str(), granteeName, columnText)));
  }

  for (Oid permId : permIds) {
    if (OidIsValid(permId)) {
      // Revoke permissions from users and roles on the relation.
      ddm::RevokeDDMPermissions(permId, grantee);
    }
  }
  // Make all changes visible before processing the next table.
  CommandCounterIncrement();

  return permIds;
}

void ReportIdenticalPolicyAttachedToGranteeError(
    std::string const& error_policy, std::string const& rel_name,
    PolicyGrantee const& grantee) {
  switch (grantee.type) {
    case kGranteeRole: {
      ereport(ERROR,
              (errcode(ERRCODE_INVALID_GRANT_OPERATION),
               errmsg("DDM policy \"%s\" is already attached on relation "
                      "\"%s\" for given column with same priority "
                      "to role \"%s\"",
                      error_policy.c_str(), rel_name.c_str(), grantee.name)));
      break;
    }
    case kGranteeUser: {
      ereport(ERROR,
              (errcode(ERRCODE_INVALID_GRANT_OPERATION),
               errmsg("DDM policy \"%s\" is already attached on relation "
                      "\"%s\" for given column with same priority "
                      "to user \"%s\"",
                      error_policy.c_str(), rel_name.c_str(), grantee.name)));
      break;
    }
    case kGranteePublic: {
      ereport(ERROR,
              (errcode(ERRCODE_INVALID_GRANT_OPERATION),
               errmsg("DDM policy \"%s\" is already attached on relation "
                      "\"%s\" for given column with same priority "
                      "to public",
                      error_policy.c_str(), rel_name.c_str())));
      break;
    }
  }
}
}  // namespace ddm

Oid GetDDMPolicyIdByPermId(Oid permission_id) {
  HeapTuple tuple = SearchSysCache(PERMID, permission_id, 0, 0, 0);
  if (!HeapTupleIsValid(tuple)) {
    elog(ERROR, "could not find tuple for permission %u", permission_id);
  }
  SCOPE_EXIT_GUARD(ReleaseSysCache(tuple));
  Immutable_pg_permission pg_permission_form =
      reinterpret_cast<Immutable_pg_permission>(GETSTRUCT(tuple));
  XPGCHECK(pg_permission_form->privtype == DDM_POLICY_PRIV_TYPE,
           policy::tag_for_policy_type(kPolicyDDM));
  XPGCHECK(OidIsValid(pg_permission_form->privid),
           policy::tag_for_policy_type(kPolicyDDM));
  return pg_permission_form->privid;
}

bool CheckMaskingOnForDatashares(Oid relid) {
  // When the GUC gconf_ddm_enable_for_datashares is not enabled, masking is
  // default on for datashares.
  if (!gconf_ddm_enable_for_datashares) {
    return true;
  }

  ddm::DDMFlagState ddm_flag_state;
  int64 ddm_datashare_enabled = 0;

  if (!pg::GetClassExtendedEntryInt(relid, PG_CLASS_ENABLE_DDM_RLS_DATASHARES,
                                    &ddm_datashare_enabled)) {
    // DDM datasharing flag is unset. If unset(kFlagUnset), the default behavior
    // is DDM datasharing is enabled.
    return true;
  }

  switch (ddm_datashare_enabled) {
    case DDM_DS_UNSET_RLS_DS_OFF:
    case DDM_DS_UNSET_RLS_DS_ON:
      ddm_flag_state = ddm::DDMFlagState::kFlagUnset;
      break;

    case DDM_DS_OFF_RLS_DS_ON:
    case DDM_DS_OFF_RLS_DS_OFF:
    case DDM_DS_OFF_RLS_DS_UNSET:
      ddm_flag_state = ddm::DDMFlagState::kFlagOff;
      break;

    case DDM_DS_ON_RLS_DS_OFF:
    case DDM_DS_ON_RLS_DS_ON:
    case DDM_DS_ON_RLS_DS_UNSET:
      ddm_flag_state = ddm::DDMFlagState::kFlagOn;
      break;

    default:
      XCHECK_UNREACHABLE("Invalid DDM RLS datasharing column value");
  }

  // DDM datashare is not enabled only when the DDM datashare is explicitly
  // set to off.
  return ddm_flag_state != ddm::DDMFlagState::kFlagOff;
}
