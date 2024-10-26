/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#pragma once

#include "pg/src/include/catalog/dependency.h"
#include "pg/src/include/postgres_ext.h"

#ifdef __cplusplus
extern "C" {
#endif

/// @brief Check if an object is referenced as lookup in CREATE MASKING POLICY.
///
/// Internally, the function traverses dependency graph in pg_depend.
///
/// @param object_reference the dependency-style object description in a form of
/// {classId, objectId, objectSubId}.
///
/// @return whether the object is referenced by the policy.
bool IsDDMLookupObject(const ObjectAddress* object_reference);

/// @brief Check if an object is a target of ATTACH MASKING POLICY.
///
/// Internally, the function traverses dependency graph in pg_depend.
///
/// @param object_reference object_reference the dependency-style object
/// description in a form of {classId, objectId, objectSubId}.
///
/// @return whether the object is referenced by the policy attachment.
bool IsDDMTargetObject(const ObjectAddress* object_reference);

/// Check if a relation is referenced as lookup in an existing masking policy
/// definition.
/// @param relid Oid of the referenced lookup table
/// @return True if given relid is indeed a DDM lookup table.
bool IsDDMLookupRel(Oid relid);

/// Check if a relation is protected by a masking policy.
///
/// Note: IsDDMTargetRel does not detect DDM protected LBVs.
///
/// @param relid Oid of the target table
/// @return True if given relid is indeed a DDM lookup table.
bool IsDDMTargetRel(Oid relid);

/// @brief For a given permission OID, revokes it from all assigned users and
/// roles. Note this function does not check that the permission is a DDM
/// permission (it would require an extra catalog lookup).
///
/// @param permission_id the permission OID to revoke.
void UndoDDMGrant(const Oid permission_id);

#ifdef __cplusplus
} /* extern "C" */
#endif
