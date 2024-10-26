/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#pragma once

#include "pg/src/include/nodes/parsenodes.h"
#include "pg/src/include/nodes/pg_list.h"
#ifdef __cplusplus

namespace ddm {

/// Struct containing the results of a DDM rewrite.
struct RewriteResult {
  /// Whether ddm was applied to this query.
  bool ddm_applied = false;
  /// Rewritten query, queries are rewritten in-place.
  Query* rewrite = nullptr;
  /// Whether query was reparsed by OMNI.
  bool omni_reparsed = false;
};

/// Rewrites Query with any associated masking policies. Modifies the Query Tree
/// in-place, without duplicating memory.
/// @param[in] Query* q - Query to be rewritten
/// @param[in] bool log_applied_policy - If true, log applied DDM policies.
/// @return RewriteResult - Result object from DDM rewriter.
/// @throw ereport when associated masking attachments + policies cannot
/// be built (foreign key violation in catalog)
/// @throw PGCheck on policies/attachments which fail to parse.
RewriteResult RewriteQuery(Query* q, bool log_applied_policy);
}  // namespace ddm

#endif

#ifdef __cplusplus
extern "C" {
#endif

/// In-place rewrites a list of Query nodes with DDM policies. Will return
/// immediately if ddm is disabled.
/// @param List* queries - input queries to be rewritten.
/// @param bool log_applied_policy - Log applied DDM policies.
/// @return bool - Whether the query was rewritten by DDM.
bool RewriteDDM(List* queries, bool log_applied_policy);

/// In-place marks any DDM protected relations as having has_ddm_rewrite=true.
/// This function is primarily intended to be called from the PG Rewriter. The
/// PG Rewriter is responsible for view unfolding using stored query trees.
/// These stored query trees *Do Not* include the has_ddm_rewrite boolean.
/// Storing an additional boolean in the serialized Query Tree has significant
/// backwards compatibility concerns. Current thinking as of 10/2022 is that
/// these query trees should be replaced with serialized Queries to simplify
/// Backwards Compatabilitiy.
/// @param List* queries - input queries to be rewritten.
void MarkDDMRelations(Query* q);
#ifdef __cplusplus
}
#endif
