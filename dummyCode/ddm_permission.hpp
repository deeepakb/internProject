/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#pragma once

#include "ddm/ddm_permission.h"
#include "ddm/ddm_policy.hpp"
#include "utils/acl.h"
#include "utils/rel.h"

#include "pg/src/include/c.h"
#include "pg/src/include/nodes/parsenodes.h"
#include "pg/src/include/postgres_ext.h"
#include "pg/src/include/utils/acl.h"

#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

namespace ddm {

/// Container for tracking policy attachment grantee.
struct PolicyGrantee {
  /// Type of grantee i.e. user/role/public.
  GranteeType type;
  /// AclId of the grantee.
  AclId id;
  /// Name of the grantee.
  char* name;
};

/// Container for creating DDM permission subId or pg_permission.
struct PermissionKey {
  /// Policy application priority. Priority is included in the permission entry
  /// along with the output attribute attnums to uniquely identify attachments.
  int4 priority = 0;
  /// Output column attnums for the permission entry.
  std::string attrsOut = "";
};

/// Container for creating DDM permission.
struct PermissionEntry {
  /// OID of the policy for permission entry.
  Oid polId = 0;
  /// Oid of the relation for which the permission entry will be created.
  Oid relId = 0;
  /// PermissionKey for creating the permission subID.
  PermissionKey key;
};

/// @brief a tuple to hold the following values in order of an attached policy
///   on a given relation's input columns and output columns for specified
///   priority:
///   Oid: Attached policyID if an identical attachment or an attachment with a
///   common output attnum/attname found, otherwise InvalidOid.
///   Oid: PermID of the attachment if attachment found, otherwise InvalidOid.
///   std::optional<std::string>: Output column name which matches with an
///   existing attachment's column name.
///   bool: True if current attachment is identical (same relation, input
///   attinfo, output attinfo, priority) to an existing attachment.
using attached_policy_info_t =
    std::tuple<Oid, Oid, std::optional<ddm::AttachedAttribute>, bool>;

/// Flag states used for DDM. Such as MASKING ON / OFF FOR DATASHARES flag.
enum class DDMFlagState { kFlagOff, kFlagOn, kFlagUnset };

/// Resolve grantee as user/role/PUBLIC.
///
/// @param grantee PolicyPrivGrantee information.
/// @return PolicyGrantee object containing resolved grantee.
/// @throw ereport on unknown or unsupported grantee i.e. superuser or
/// reserved role.
PolicyGrantee ResolvePolicyGrantee(const PolicyPrivGrantee* grantee);

/// Attach a DDM policy on relation to user/role/PUBLIC.
/// DDM policy attachment is implemented by creating a permission in RBAC table
/// pg_permission for the requested relation and granting it to users and roles
/// using RBAC tables pg_role_users and pg_user_roles.
/// @param perm_entry PermissionEntry of the policy.
/// @param pol_name Name of the DDM policy.
/// @param in_attribute_info attribute_info_t for the input columns of the
/// policy.
/// @param out_attribute_info attribute_info_t for the output columns of the
/// policy.
/// @param rel_name Name of the relation.
/// @param grantee PolicyGrantee for attaching DDM policy.
/// @return a pair containing Permission ID of the granted DDM privilege over
/// the relation for the specified policy and true if a duplicate policy(same
/// policy name, inattrs, outattrs and priority) is already attached to the
/// relation.
std::pair<Oid, bool> AttachPolicyOnRel(
    PermissionEntry const& perm_entry, std::string pol_name,
    attribute_info_t const& in_attribute_info,
    attribute_info_t const& out_attribute_info, std::string rel_name,
    PolicyGrantee const& grantee);

/// Detach a DDM policy on relation from user, role or PUBLIC.
/// DDM permission created in RBAC table pg_permission for the requested
/// relation is dropped and revoked from user and role using RBAC tables
/// pg_role_users and pg_user_roles. Moreover, appropriate pg_permission_mask
/// entry is deleted as well.
/// @param perm_entry PermissionEntry of the policy.
/// @param pol_name Name of the DDM policy.
/// @param in_attribute_info attribute_info_t for the input columns of the
/// policy.
/// @param out_attribute_info attribute_info_t for the output columns of the
/// policy.
/// @param rel_name Name of the relation.
/// @param grantee PolicyGrantee for detaching DDM policy.
/// @return unordered_set::set<Oid> the OIDs of the deleted permissions
/// (attachments).
std::unordered_set<Oid> DetachDDMPolicyOnRel(
    PermissionEntry const& perm_entry, std::string pol_name,
    attribute_info_t const& in_attribute_info,
    attribute_info_t const& out_attribute_info, std::string rel_name,
    PolicyGrantee const& grantee);

/// Get a hashed valued from PermissionKey to be used as subObjId for
/// pg_permission.
/// @param key PermissionKey for generating the hashed value.
/// @return Generated hashed value from the combination of priority and
/// output column attnums.
int perm_hash_value(const PermissionKey& key);

/// Search if any DDM policy is attached on the given relation input columns and
/// output columns for specified priority.
/// @param polid_to_attach Policy ID against which search for attached policies.
/// @param relid Oid of the relation.
/// @param in_attrs attribute_info_t of input column attrs(attnum / attname).
/// @param out_attrs attribute_info_t of output column attrs(attnum / attname).
/// @param priority Priority value to check the attachment.
/// @return attached_policy_info_t containing information about the attached
/// policy for the given relation's input attributes, output attributes and
/// priority.
attached_policy_info_t GetAttachedPolicyInfo(Oid polid_to_attach, Oid relid,
                                             attribute_info_t const& in_attrs,
                                             attribute_info_t const& out_attrs,
                                             int4 priority);

/// @brief A helper to compare a tuple of pg_permission_mask with the given
/// input attinfo and output attinfo to get if they are identical
/// (same input and output attinfo) or they have a common output attrbute
/// (attnum / attname).
/// @param Relation The relation pg_permission_mask.
/// @param HeapTuple Tuple entry of the relation pg_permission_mask to compare
/// with.
/// @param polid_to_attach Policy ID against which search for attached policies.
/// @param relid The relid for which we are comparing existing attachments.
/// @param in_attinfo std::vector of AttachedAttributes of input attributes.
/// @param out_attinfo std::vector of AttachedAttributes of output attributes.
/// @param out_colname_to_pathset_map For each column name(key) from the output
/// AttachedAttribute(s), stores the pathset(value) which has the corresponding
/// path components.
/// @param matched_outatt Stores the common output attribute if a common output
/// attribute is found.
/// @param matched_polid Stores the policy id of the tuple if the input tuple is
/// identical to the given input and output columns or the have a common output
/// attribute.
/// @param matched_permid Stores the permission id of the tuple if the input
/// tuple is identical to the given input and output columns or have a common
/// output attribute.
/// @return bool True if the input tuple is identical to the given input attinfo
/// and output attinfo(same input and output attinfo).
bool CompareWithAttachment(
    Relation pg_permission_mask, HeapTuple tup, Oid polid_to_attach, Oid relid,
    attribute_info_t const& in_attinfo, attribute_info_t const& out_attinfo,
    std::unordered_map<std::string, PathSet>& out_colname_to_pathset_map,
    std::optional<ddm::AttachedAttribute>& matched_outatt, Oid& matched_polid,
    Oid& matched_permid);

/// @brief Checks if the given attached_attribute conflicts with any
/// column in the given colname_to_pathset_map. An AttachedAttribute without
/// having a path value conflicts with a colname_to_pathset_map entry if they
/// share the same column name. For an AttachedAttribute with path value, along
/// with checking the column name, we also need to check if the path value of
/// the AttachedAttribute is contained (fully / partially) in the matched
/// column's PathSet.
/// @param attached_attribute the given AttachedAttribute.
/// @param colname_to_pathset_map Map containing column name and
/// corresponding PathSet.
/// @param relid Relation id to get the column name of the input
/// attached_attribute.
/// @return bool True if attached_attribute conflicts with any column in the
/// given column name_to_pathset_map.
bool IsConflictingAttachedAttribute(
    const ddm::AttachedAttribute& attached_attribute,
    const std::unordered_map<std::string, PathSet>& colname_to_pathset_map,
    Oid relid);

/// Search and retrieve any DDM policy attachments on the given relation output
/// columns from specified policy grantee in DETACH statement.
/// @param permEntry PermissionEntry of the policy.
/// @param grantee PolicyGrantee for deleting DDM permission entries.
/// @return A unordered set of retrieved permission Oids.
std::unordered_set<Oid> GetAttachedPermsForRelOutput(
    PermissionEntry const& permEntry, PolicyGrantee const& grantee);

/// Check if the specified permission ID is attached to the grantee.
/// @param permId Oid of the permission.
/// @param grantee PolicyGrantee to check for.
/// @return True if the DDM permission is attached.
bool IsPermissionAttached(Oid permId, PolicyGrantee const& grantee);

/// Check if the specified permission ID is attached to any user or role.
/// The purpose of this function is to identify the fact that a permission
/// exists with no grantees and then having it subsequently deleted by some
/// other code.
/// @param permId Oid of the permission.
/// @return True if the DDM permission is attached to any user or role.
bool IsPermissionAttached(Oid permId);

/// Find if any DDM policy is attached to given relation with specified key.
/// @param permEntry PermissionEntry of the policy.
/// @return Oid of the attached policy if found else InvalidOid.
Oid GetPolicyAttachedToRel(PermissionEntry const& permEntry);

///
/// @brief Returns the names of the DDM policies (if they exist) in the current
/// database that would be applied to a relation, user/role and column on that
/// relation.
///
/// @note
/// 1. Multiple DDM polices can be attached to (relation, user/role, column)
/// tuple, and only the highest priority policy will actually be applied (and
/// hence returned by this function).
/// 2. One and only one of user_id and role_id must be set (will be XCHECKed).
/// 3. Multiple policies can be attached to a column if those attachments are to
/// the nested paths.
///
/// @param relation_id the OID of the relation to which the policy is attached.
/// @param user_id optional AclId of the user to whom the policy is attached
/// (cannot be set simultaneously with role_id).
/// @param role_id optional AclId of the role to which the policy is attached
/// (cannot be set simultaneously with user_id).
/// @param column_name the name of the column on the relation (by relation_id)
/// to which the policy is attached). Note, the policy does not have to apply
/// only to this one column.
/// @return std::unordered_set<std::string> names of the DDM polices (can be
/// empty).
///
/// @throws PGCHECK if both or none of user_id and role_id must be set.
std::unordered_set<std::string> GetDDMPolicyNamesForColumn(
    Oid relation_id, std::optional<AclId> user_id, std::optional<AclId> role_id,
    const std::string column_name);

/// A helper function to ereport that an identical policy(same relation,
/// input attnums, output attnums, priority) is already attached to the
/// grantee.
/// @param error_policy name of the identical policy already attached to
/// the same grantee.
/// @param rel_name name of the relation policy attached on.
/// @param grantee PolicyGrantee the identical policy attached to.
void ReportIdenticalPolicyAttachedToGranteeError(
    std::string const& error_policy, std::string const& rel_name,
    PolicyGrantee const& grantee);
}  // namespace ddm

/// Checks if MASKING FOR DATASHARES is set to ON for the given relation.
/// @param [in] relid Oid of the relation.
/// @return true if MASKING FOR DATASHARES is Set to ON.
bool CheckMaskingOnForDatashares(Oid relid);
