/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#pragma once

#include "pg/src/include/postgres_ext.h"

#ifdef __cplusplus
extern "C" {
#endif

///
/// @brief Returns a DDM policy OID by the permission OID.
///
/// @param permission_id the OID of the permission tuple in pg_permission.
/// @return Oid the OID of the DDM policy in pg_policy_mask.
///
/// @throws XPGCHECK if the permission is not found or is not a DDM one.
Oid GetDDMPolicyIdByPermId(Oid permission_id);

#ifdef __cplusplus
} /* extern "C" */
#endif
