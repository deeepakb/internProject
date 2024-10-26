/// Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#pragma once

#include "external_catalog/model/object_model.hpp"
#include "federation/FederatedSnapshotManager.hpp"
#include "nodes/parsenodes.h"
#include "optimizer/clauses.h"

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace ddm {

// Alias for LF-ALP policy in DDM module.
using LFALPPolicy = fsmutils::authorized_nested_paths_t;
// Forward declaration.
struct MergeEntry;

/// @brief Checks whether given RTE has an LF policy attached to it.
///
/// In order for the check to return true, (1) the RTE has to be for a relation
/// managed by LF, (2) there needs to be an LF-ALP policy for the RTE. Note,
/// this call does not fully parse the policy (it has non-negligible cost), it
/// merely checks that there is an LF-ALP policy for RTE. An LF policy may or
/// may not have an ALP component.
///
/// @note This function has the following performance characteristics:
/// 1. It will short-circuit if gconf_enable_lf_alp is OFF or if the RTE is not
///    LF-managed (this check does not do network call).
/// 2. It will access LF policy (JSON) for the RTE and this policy is cached.
/// 3. It will not parse ALP paths, it will short-circuit is there at least one
///    such path exists.
bool HasLakeFormationALPForRTE(const RangeTblEntry* rte);

/// @brief Returns the parsed LF-ALP policy (if exists) for the RTE.
///
/// If the RTE is for a relation not managed by LF, or if LF policy is not found
/// for such relation, returns std::nullopt. Note, the existence of return value
/// for this function may be used as a more precise alternative to
/// HasLakeFormationALPForRTE, but it does parsing of ALP paths, which may be
/// expensive.
std::optional<LFALPPolicy> GetLakeFormationALPForRTE(const RangeTblEntry* rte);

struct ObjTransformAlpColumnsCtx {
  /// The cache of LF-ALP policies per RTE OID. The value can be std::nullopt
  /// meaning that this OID is known not to have an LF-ALP policy.
  std::optional<LFALPPolicy> lf_alp_policy;
  /// A set of columns that are known to need object transform.
  std::unordered_set<std::string_view> obj_transform_alp_columns;
};

/// A map of RTE OIDs to an object transform construct that contains LF-ALP
/// policies and columns that are known to need object transform.
using ObjTransformAlpColumnsRelationMap =
    std::unordered_map<Oid, ObjTransformAlpColumnsCtx>;

/// @brief Modifies given DDM masks map to include the entries resulting for
/// LF-ALP policy.
///
/// Note, nothing is returned by this function, instead the masks map is being
/// modified. Also note that since LF and DDM are currently mutually exclusive,
/// there cannot be a case when the same columns in the map has a DDM entry and
/// an LF-ALP entry.
///
/// @param masks The DDM masks is map that is primary input to the DDM rewriter.
/// The map is per relation and maps column names to essentially a parsed
/// expression masking the column.
/// @param policy LF-ALP policy is itself a similar map: it maps the SUPER
/// column names to sets of authorized paths for the column. This routine
/// therefore converts the set of paths into an expression (OBJECT_TRANSFORM)
/// that modifies the SUPER value to retain only those paths.
/// @param alp_obj_transform_columns a set of columns for the policy that are
/// known to need object transform. This is a part of var level optimization.
/// Assuming the optimization is ON, we know that all columns not in the set are
/// known to have only the allowed navigation paths.
void AddLakeFormationALPToDDMMap(
    std::unordered_map<std::string, std::vector<MergeEntry>>* masks,
    LFALPPolicy policy,
    const std::unordered_set<std::string_view>& alp_obj_transform_columns);

struct VarLevelALPWalkerCtx {
  /// Top-level original query.
  Query* query;
  /// Stack of rtables so that any Var * can be tracked to its RTE from any
  /// subquery.
  std::vector<List*> rtables;
  /// For Lf-ALP, a map of relations to LF-ALP policies on all nested paths, as
  /// well as columns for the policy that are known to need object transform.
  ObjTransformAlpColumnsRelationMap obj_transform_alp_columns_rel_map;
};

/// @brief Helper routine of VarLevelALPWalker that specifically processes Var
/// references. It verifies that the given Var is LF-ALP protected and if so:
/// 1. Skips it (no-op) if the navigation path is authorized.
/// 2. Errors the query if the navigation path is unauthorized.
/// 3. Adds the Var (by column name) to ctx.obj_transform_alp_columns.
bool WalkNestedVar(Var* var, VarLevelALPWalkerCtx* ctx);

/// @brief A walker callback that traverses the query looking for all Var's
/// subject to LF-ALP and for each such Var erroring or adding it to
/// ctx.obj_transform_alp_columns.
bool VarLevelALPWalker(Node* node, void* context);

/// @brief An entry point for VarLevelALPWalker that sets up the context.
/// Returns the a relation map of relations to LF-ALP policies on all nested
/// paths, as well as columns for the policy that are known to need object
/// transform.
ObjTransformAlpColumnsRelationMap WalkVarLevelALP(Query* query);

/// @brief Retrieve the original nested column for an external nested range
/// table entry. The caller needs to ensure that provided `var` belongs and
/// refers to the given RTE.
/// Returns the original nested external column for the corresponding target
/// list entry.
const external_catalog::Column& OriginalNestedColumnForExternalNestedRTEVar(
    const RangeTblEntry* rte, const Var* var);

}  // namespace ddm
