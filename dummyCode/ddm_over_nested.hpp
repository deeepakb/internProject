/// Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#pragma once

#include "ddm/ddm_policy.hpp"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace ddm {

// Forward declarations.
struct MergeEntry;
struct RewriteMutatorCtx;

// Context for the InPlaceNestedVarsWalker that looks for SUPER Vars for
// OBJECT_TRANSFORM.
struct InPlaceNestedVarsWalkerCtx {
  static const int kFlag = QTW_WALK_RELATIONS;

  /// Stack of rtables so that any Var can be tracked to its RTE from any
  /// subquery.
  std::vector<List*> rtables;
  /// The collection of Vars that are subject to OBJECT_TRANSFORM. The map is a
  /// set of column name per RTE (by OID).
  NestedVarMap obj_transform_vars;
  /// A cache of mask maps per RTE so that we don't recompute it and reload for
  /// each Var (instead, once per RTE).
  MaskMapsCache mask_maps_cache;
};

/// @brief Constructs an OBJECT_TRANSFORM parse node (in a form of MinMaxExpr)
/// from merge entries.
///
/// @param entries the merge entries for the mask map with a condition that each
/// entry has a nested path.
/// @param input the column name of the input SUPER.
MinMaxExpr* MakeObjectTransform(std::vector<MergeEntry> entries,
                                std::string input);

/// @brief Wrap the given node into layers of navigation components (object
/// paths or array indices) given by Var's partiql_path.
/// @example for node `udf( a + 10 )` and path `a.b[2]`, we produce:
///
/// GET_ARRAY_PATH(
///   GET_OBJECT_PATH(
///     GET_OBJECT_PATH(
///       udf( a + 10 ),
///       'a'
///     ),
///     'b'
///   ),
///   2
/// )
Node* MakePartiQLNavigationPath(const Var* var, Node* node);

/// @brief A helper for the in-place walker that processes a Var node. If this
/// is a subject to DDM-nested-induced OBJECT_TRANSFORM, will add it to the
/// context.
void WalkInPlaceNestedVar(Var* var, InPlaceNestedVarsWalkerCtx* context);

/// @brief A walker function that looks for Vars that will need OBJECT_TRANSFORM
/// due to DDM over nested. Returns always false (i.e., does not short-circuit).
// TODO(bogatov): should be possible to reuse with LF-ALP similar function.
bool InPlaceNestedVarsWalker(Node* node, void* context);

/// @brief An entry point for the in-place OBJECT_TRANSFORM walker. Returns a
/// collection of Vars for which we will do OBJECT_TRANSFORM on the RTE (i.e.,
/// subquery) level.
NestedVarMap WalkInPlaceNestedVars(Query* query);

/// @brief A helper that mutates (i.e., returns a new node) the Var replacing it
/// with the masking expression. Will do so only if required by DDM (including
/// checking if this Var has not been OBJECT_TRANSFORM'd).
Node* MutateNestedVar(Var* var, RewriteMutatorCtx* context);

}  // namespace ddm
