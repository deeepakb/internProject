/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#include "ddm/ddm_dependency.h"

#include "policy/policy_dependency.hpp"
#include "xen_utils/scope_exit_guard.hpp"

#include "pg/src/backend/catalog/permission.hpp"
#include "pg/src/include/access/heapam.h"
#include "pg/src/include/access/xact.h"
#include "pg/src/include/catalog/catname.h"
#include "pg/src/include/catalog/dependency.h"
#include "pg/src/include/catalog/pg_class.h"
#include "pg/src/include/postgres_ext.h"
#include "pg/src/include/storage/lmgr.h"
#include "pg/src/include/utils/rel.h"

#include <optional>

bool IsDDMLookupObject(const ObjectAddress* object_reference) {
  Relation pg_depend = heap_openr(DependRelationName, AccessShareLock);

  policy::ObjectAddressSet visited;

  bool is_lookup_obj = policy::IsReferencedByClassId(
      pg_depend, object_reference, RelOid_pg_policy_mask, true /* recurse */,
      std::nullopt /* deptype */, visited, true /* skip_stale_permissions */);
  heap_close(pg_depend, AccessShareLock);
  return is_lookup_obj;
}

bool IsDDMTargetObject(const ObjectAddress* object_reference) {
  Relation pg_depend = heap_openr(DependRelationName, AccessShareLock);

  // Target dependencies are defined as direct dependencies of pg_permission.
  // Note that every target dependency is an indirect dependency of pg_policy.
  policy::ObjectAddressSet visited;

  bool is_target_obj = policy::IsReferencedByClassId(
      pg_depend, object_reference, RelOid_pg_permission, false /* recurse */,
      DEPENDENCY_DDM, visited, true /* skip_stale_permissions */);
  heap_close(pg_depend, AccessShareLock);
  return is_target_obj;
}

bool IsDDMLookupRel(Oid relid) {
  ObjectAddress obj = {RelOid_pg_class, relid, 0 /* objectSubId */};
  return IsDDMLookupObject(&obj);
}

bool IsDDMTargetRel(Oid relid) {
  ObjectAddress obj = {RelOid_pg_class, relid, 0 /* objectSubId */};
  Relation pg_depend = heap_openr(DependRelationName, AccessShareLock);

  // Target dependencies are defined as direct dependencies of pg_permission.
  // Note that every target dependency is an indirect dependency of pg_policy.
  policy::ObjectAddressSet visited;

  // In case of target relation dependency checks, we don't want a stale
  // permission dependency. It will help in blocking the usage of stale target
  // relations in CREATE POLICY statements.
  bool is_target_obj = policy::IsReferencedByClassId(
      pg_depend, &obj, RelOid_pg_permission, false /* recurse */,
      DEPENDENCY_DDM, visited, false /* skip_stale_permissions */);
  heap_close(pg_depend, AccessShareLock);
  return is_target_obj;
}

void UndoDDMGrant(const Oid permission_id) {
  Relation pg_permission_users =
      heap_openr(PermissionUsersRelationName, RowExclusiveLock);
  SCOPE_EXIT_GUARD(relation_close(pg_permission_users, RowExclusiveLock));

  Relation pg_user_permission =
      heap_openr(UserPermissionRelationName, RowExclusiveLock);
  SCOPE_EXIT_GUARD(relation_close(pg_user_permission, RowExclusiveLock));

  Relation pg_permission_roles =
      heap_openr(PermissionRolesRelationName, RowExclusiveLock);
  SCOPE_EXIT_GUARD(relation_close(pg_permission_roles, RowExclusiveLock));

  Relation pg_role_permission =
      heap_openr(RolePermissionRelationName, RowExclusiveLock);
  SCOPE_EXIT_GUARD(relation_close(pg_role_permission, RowExclusiveLock));

  // Revoke each permission from all the assigned users and roles.
  RevokePermRoles(permission_id, pg_permission_roles, pg_role_permission);
  RevokePermUsers(permission_id, pg_permission_users, pg_user_permission);

  // Apply the changes to those catalog tables.
  CommandCounterIncrement();
}
