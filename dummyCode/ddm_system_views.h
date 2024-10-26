/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#pragma once

#include "pg/src/include/c.h"
#include "pg/src/include/postgres_ext.h"

#ifdef __cplusplus
extern "C" {
#endif

///
/// @brief Generates a human readable JSON representation of the DDM policy
/// expression.
///
/// @param input The JSON of DDM policy expression as it appears in the catalog.
/// This function should not be used over anything other than DDM expressions
/// read from the catalog. It will work (or gracefully) erro, but the result may
/// not be useful.
///
/// @return VarChar* The JSON of the DDM policy expression with type OID and
/// mode replaced with the human-friendly type name (i.e, integer).
///
VarChar* policy_expression_out_internal(const VarChar* input);

///
/// @brief Generates a human readable JSON representation of the attributes for
/// input and output columns for a DDM policy attachment.
///
/// We store the columns to which a DDM policy is attached as JSON arrays of
/// numbers. The numbers correspond to attribute (column) numbers for the
/// relation as it appears in the pg_class. This function transforms the JSON
/// array into column names for human readability.
///
/// @example
/// Input: [1, 3, 5]
/// Output ["sale_id", "price", "column_name"]
///
/// @param relation_id the OID of the relation on which to look for column names
/// (by design it comes from a DDM attachment).
/// @param input JSON array on numbers corresponding to attribute numbers (by
/// design it comes from a DDM attachment).
///
/// @return VarChar* the JSON array of strings - column names.
///
/// @throws XCHECK or PGCHECK if
/// 1. As with all parsing functions, if JSON is malformed.
/// 2. Relation does not exist.
/// 3. An attribute number from input is out of bounds with respect to an array
/// of columns for the relation.
///
VarChar* mask_policy_attached_atts_out_internal(Oid relation_id,
                                                const VarChar* input);
/**
 * @brief Returns the name of DDM policy in the current database that is
 * applicable to given relation (specified by relation schema and name),
 * user/role and column on the relation.
 *
 * @note One and only one of user_name and role_name must be set.
 */

///
/// @brief Returns the name of DDM policy in the current database that is
/// applicable to given relation (specified by relation schema and name),
/// user/role and column on the relation.
///
/// @param schema_name The schema name filter.
/// @param relation_name The relation name filter (in the given schema).
/// @param column_name The column name filter (in the given relation).
/// @param user_name An optional user name filter.
/// @param role_name An optional role name filter.
///
/// @return
/// 1. The name (not an OID) of the DDM policy that is applicable to the
/// given set of objects, if such policy exists and is single (i.e.,
/// column-level attachment).
/// 2. NULL if not such policy does not exists.
/// 3. A list of policy names in a form of JSON array of strings if there are
/// multiple policies satisfying the criteria (i.e., nested attachments).

/// The applicable policy is determined by the DDM attachment to the given
/// column (in then given relation and schema) to the given user/role with the
/// highest priority.
///
/// @note One and only one of user_name and role_name must be set.
VarChar* get_ddm_policy_name_for_column(VarChar* schema_name,
                                        VarChar* relation_name,
                                        VarChar* column_name,
                                        VarChar* user_name, VarChar* role_name);

#ifdef __cplusplus
}
#endif
