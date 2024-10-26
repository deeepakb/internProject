/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#pragma once

#include "nodes/parsenodes.h"

#include "pg/src/include/postgres_ext.h"

#ifdef __cplusplus
extern "C" {
#endif
/// Test if a DDM policy is attached to the given RTE (check by its relid).
/// Returns true if at least one policy is attached.
/// @param[in] Oid relid - relation that will be tested for attached policies.
/// @return bool - True if there is an attached rewrite.
/// @throws - elog Error if an invalid tuple is found. This will only occur with
///  corrupted data in the catalog.
bool HasDDMRewrite(RangeTblEntry* rte);

/// Test if a DDM policy is attached to the given relid. This fuction is
/// different than HasDDMRewrite() because it only checks if a policy is
/// attached, does not check the user type.
/// Returns true if at least one policy is attached.
/// @param[in] Oid relid - relation that will be tested for attached policies.
/// @return bool - True if there is an policy attached to the relation.
bool HasDDMAttachmentForRel(Oid relid);

/// Returns true if DDM is enabled over super paths.
bool DDMEnabledOverSuperPaths();
#ifdef __cplusplus
}
#endif
