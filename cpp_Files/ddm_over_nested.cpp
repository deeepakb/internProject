/// Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#include "ddm/ddm_over_nested.hpp"

#include "ddm/constants.hpp"
#include "ddm/ddm_policy.hpp"
#include "fmgroids.h"  // NOLINT(build/include_subdir)
#include "nodes/makefuncs.h"
#include "omnisql/omnisql_view.h"
#include "parser/analyze.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "partiql/partiql.h"
#include "policy/policy_utils.hpp"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"

#include <algorithm>
#include <set>

extern bool gconf_ddm_enable_over_super_paths;
extern bool gconf_ddm_enable_perm_check;
extern bool g_pgtest_ddm;

namespace ddm {

/// @brief Does a cache lookup (including loading to cache if not found) for the
/// mask map for RTE (by it's rel ID).
MaskMap GetMasksFromCache(MaskMapsCache* mask_maps_cache, Oid rte_relid);

/// @brief A helper mutator for the Var that looks for Var that are policy
/// arguments and sets their varno and varlevelsup (i.e., their RTable pointers)
/// to the proper RTable pointed to by `var` argument (the original Var).
bool VarAdjustMutator(Node* node, void* context);

/// @brief A helper to build an expression from ResTarget mask for the given RTE
/// and the nested variable. The reason we can't plug the mask node "as is" is
/// because ResTarget is a parse node, while we need query node expresion. This
/// function is trimmed down version of BuildSubQueryForRel. Resulting
/// expression is plugable instead of the Var to be masked.
Expr* BuildExpressionForNestedPolicy(RangeTblEntry* rte, std::string varname,
                                     Var* var, ResTarget* mask);

MinMaxExpr* MakeObjectTransform(std::vector<MergeEntry> entries,
                                std::string input) {
  if (g_pgtest_ddm) {
    // Enforce unambiguous ordering for determinism and testing.
    std::sort(entries.begin(), entries.end(), [](MergeEntry a, MergeEntry b) {
      return *a.nested_path < *b.nested_path;
    });
  }

  ColumnRef* input_node = makeNode(ColumnRef);
  input_node->oraclePlus = false;
  input_node->fields = list_make1(makeString(pstrdup(input.c_str())));

  List* set_args = NIL;
  for (const MergeEntry& entry : entries) {
    Type const_type = typeidType(VARCHAROID);
    XCHECK(entry.nested_path->size() <
           std::numeric_limits<int32>::max() - VARHDRSZ);
    int32 typmod = entry.nested_path->size() + VARHDRSZ;
    Datum path_datum = DirectFunctionCall3(varcharin,
                          CStringGetDatum(pstrdup(entry.nested_path->c_str())),
                          ObjectIdGetDatum(InvalidOid),
                          typmod);
    Const* path = makeConst(VARCHAROID, typeLen(const_type), path_datum,
                            false /* constisnull */, typeByVal(const_type));
    set_args = lappend(set_args, path);
    set_args = lappend(set_args, copyObject(entry.rtgt->val));
  }

  MinMaxExpr* ot_node = makeNode(MinMaxExpr);
  ot_node->op = IS_OBJECT_TRANSFORM;
  ot_node->args = list_make3(input_node, NIL /* KEEP args */, set_args);

  return ot_node;
}

Node* MakePartiQLNavigationPath(const Var* var, Node* node) {
  if (var->vartype != PARTIQLOID || var->partiql_path == NIL) return node;

  // Here we wrap resulting expressions into calls to get_object_path and
  // get_array_path, because we don't support partiql_path over non-Var.
  ListCell* lc = nullptr;
  foreach(lc, var->partiql_path) {
    PartiQLPathElement* path_el =
        reinterpret_cast<PartiQLPathElement*>(lfirst(lc));
    switch (path_el->path_step_type) {
      case PARTIQL_PATH_STEP_OBJECT: {
        XCHECK(IsA(path_el->element, String));
        const char* component = resolveStrValForSuperAttribute(
            reinterpret_cast<Value*>(path_el->element),
            true /* ispartiqlcolumn */);
        Type const_type = typeidType(VARCHAROID);
        Datum path_datum =
            DirectFunctionCall1(varcharin, CStringGetDatum(pstrdup(component)));
        Const* path = makeConst(VARCHAROID, typeLen(const_type), path_datum,
                                false /* constisnull */, typeByVal(const_type));
        node = reinterpret_cast<Node*>(
            makeFuncExpr(F_GET_OBJECT_PATH, PARTIQLOID, list_make2(node, path),
                         COERCE_EXPLICIT_CALL));
        break;
      }
      case PARTIQL_PATH_STEP_ARRAY: {
        // In array notation, index can be any node. If it's already in the
        // partiql navigation list, we know it's safe to inject it in the
        // function call.
        node = reinterpret_cast<Node*>(
            makeFuncExpr(F_GET_ARRAY_PATH, PARTIQLOID,
                         list_make2(node, copyObject(path_el->element)),
                         COERCE_EXPLICIT_CALL));
        break;
      }
      default:
        XCHECK_UNREACHABLE();
        break;
    }
  }
  return node;
}

MaskMap GetMasksFromCache(MaskMapsCache* mask_maps_cache, Oid rte_relid) {
  auto masks_it = mask_maps_cache->find(rte_relid);
  if (masks_it != mask_maps_cache->end()) return masks_it->second;

  std::set<Attachment> attachments =
      FetchAttachmentsForRel(rte_relid, GetUserId(), std::nullopt /* roleid */);

  std::set<ParsedPolicyAttachment> parsed_attachments;
  std::transform(attachments.begin(), attachments.end(),
                 std::inserter(parsed_attachments, parsed_attachments.begin()),
                 [](Attachment attachment) -> ParsedPolicyAttachment {
                   return ParseAttachment(attachment);
                 });

  MaskMap mask_map =
      BuildMaskMap(parsed_attachments, {} /* obj_transform_alp_ctx */);

  mask_maps_cache->insert({rte_relid, mask_map});
  return mask_map;
}

void WalkInPlaceNestedVar(Var* var, InPlaceNestedVarsWalkerCtx* context) {
  XCHECK(IsA(var, Var), DDM_CALLER);
  XCHECK(context != nullptr, DDM_CALLER);
  XCHECK(var != nullptr, DDM_CALLER);

  // The variables that are not SUPER are taken care of in MutateRTE /
  // MutateVar.
  if (var->vartype != PARTIQLOID) return;

  // Ensure RTable is stored in the stack for the variable.
  XCHECK(var->varlevelsup < context->rtables.size(), DDM_CALLER);
  RangeTblEntry* rte = rt_fetch(
      var->varno,
      context->rtables[context->rtables.size() - var->varlevelsup - 1]);
  Oid rte_relid = GetRelationRelid(rte);
  // This can happen for non-view subqueries, JOIN subqueries, etc. In such case
  // we know such variable is not a column of DDM-protected relation.
  if (!OidIsValid(rte_relid)) return;

  // We need a real name of the column to look it up in the map.
  std::string_view varname = policy::ColumnNameForRTEVar(rte, var);
  // A name can be empty in case of dropped column.
  if (varname.size() == 0) return;

  MaskMap mask_map = GetMasksFromCache(&context->mask_maps_cache, rte_relid);
  // If no attachments for variable, return.
  auto mask_map_it = mask_map.find(std::string(varname));
  if (mask_map_it == mask_map.end()) return;

  std::vector<MergeEntry> entries = mask_map_it->second;
  // If attachment is not nested, return.
  if (entries.size() == 1 && !entries[0].nested_path) return;

  // Now the fun part: determine if given var requires OBJECT_TRANSFORM given
  // current DDM attachments. As of now, we let the variable avoid the
  // OBJECT_TRANSFORM iff this the navigation path exactly matches the
  // attachment. For @example:
  //
  // 1. attach a.b AND nav ref a.b   MEANS NO  obj transform
  // 2. attach a.b AND nav ref a.b.c MEANS YES obj transform
  // 3. attach a.b AND nav ref a     MEANS YES obj transform

  // Collect navigation path component by component.
  std::vector<std::string_view> components;
  ListCell* lc = nullptr;
  foreach(lc, var->partiql_path) {
    PartiQLPathElement* path_el =
        reinterpret_cast<PartiQLPathElement*>(lfirst(lc));
    if (path_el->path_step_type == PARTIQL_PATH_STEP_OBJECT) {
      XCHECK(IsA(path_el->element, String));
      components.push_back(strVal(path_el->element));
    } else {
      // Short circuit: any array reference in navigation path requires
      // OBJECT_TRANSFORM.
      context->obj_transform_vars[rte_relid].insert(std::string(varname));
      return;
    }
  }

  for (auto&& entry : entries) {
    XCHECK(entry.nested_path);
    RawPath entry_path(entry.nested_path->data(), entry.nested_path->size());

    RawPath::Iterator entry_path_it = entry_path.begin();
    std::vector<std::string_view>::iterator components_it = components.begin();

    while (entry_path_it != entry_path.end() &&
           components_it != components.end()) {
      // If at any step the path components are different, paths diverge.
      if (*entry_path_it++ != *components_it++) break;
    }
    if (entry_path_it != entry_path.end() ||
        components_it != components.end()) {
      // That's the case we are looking for: either attachment path is a perfect
      // prefix of this var path, or the other way around.
      context->obj_transform_vars[rte_relid].insert(std::string(varname));
      break;
    }
  }

  return;
}

bool InPlaceNestedVarsWalker(Node* node, void* context) {
  if (node == nullptr) return false;

  InPlaceNestedVarsWalkerCtx* ctx =
      reinterpret_cast<InPlaceNestedVarsWalkerCtx*>(context);

  // Don't use query_tree_walker for walking into subqueries because we need
  // to set and restore context upon descend into subquery.
  if (IsA(node, RangeTblEntry)) {
    RangeTblEntry* rte = reinterpret_cast<RangeTblEntry*>(node);
    // Don't care what RTE type it is. As long as it has subquery, we descend
    // into it.
    if (rte->subquery != nullptr) {
      ctx->rtables.push_back(rte->subquery->rtable);
      query_tree_walker(reinterpret_cast<Query*>(rte->subquery),
                        InPlaceNestedVarsWalker, context,
                        InPlaceNestedVarsWalkerCtx::kFlag);
      ctx->rtables.pop_back();
    }
    return false;
  }

  // Query node is specifically not handled by the expression_tree_walker.
  if (IsA(node, Query)) {
    ctx->rtables.push_back(reinterpret_cast<Query*>(node)->rtable);
    query_tree_walker(reinterpret_cast<Query*>(node), InPlaceNestedVarsWalker,
                      context, InPlaceNestedVarsWalkerCtx::kFlag);
    ctx->rtables.pop_back();
    return false;
  }

  if (IsA(node, Var)) {
    WalkInPlaceNestedVar(reinterpret_cast<Var*>(node), ctx);
    return false;
  }
  expression_tree_walker(node, InPlaceNestedVarsWalker, ctx);
  return false;
}

NestedVarMap WalkInPlaceNestedVars(Query* query) {
  InPlaceNestedVarsWalkerCtx context;

  bool supported_cmd =
      query->commandType != CMD_UPDATE && query->commandType != CMD_DELETE;

  if (gconf_ddm_enable_over_super_paths && supported_cmd) {
    context.rtables.push_back(query->rtable);
    query_tree_walker(query, InPlaceNestedVarsWalker, &context,
                      InPlaceNestedVarsWalkerCtx::kFlag);
  }
  return context.obj_transform_vars;
}

/// Context for VarAdjustMutator mutator. Keeps track of depth to be able to
/// find the Var that is policy arg. Also keeps the RTable pointers to set.
struct VarAdjustMutatorContext {
  static const int kFlags = QTW_WALK_RTE_SUBQUERY;

  const Index correct_varno;
  const Index correct_varlevelsup;
  size_t depth = 0;
};

bool VarAdjustMutator(Node* node, void* context) {
  auto ctx = reinterpret_cast<VarAdjustMutatorContext*>(context);

  if (node == nullptr) return false;

  if (IsA(node, Query)) {
    ctx->depth++;
    query_tree_walker(reinterpret_cast<Query*>(node), VarAdjustMutator, context,
                      VarAdjustMutatorContext::kFlags);
    ctx->depth--;
    return false;
  }

  if (IsA(node, RangeTblEntry)) {
    RangeTblEntry* rte = reinterpret_cast<RangeTblEntry*>(node);

    if (rte->subquery != nullptr) {
      ctx->depth++;
      query_tree_walker(reinterpret_cast<Query*>(rte->subquery),
                        VarAdjustMutator, context,
                        VarAdjustMutatorContext::kFlags);
      ctx->depth--;
    }
    return false;
  }

  if (IsA(node, Var)) {
    auto var = reinterpret_cast<Var*>(node);
    /// Only update the variables that are arguments of the masking expressions.
    /// check that the Var belongs to "masked" RTE using varno and varlevelsup.
    if (var->varno == 1 && var->varlevelsup == ctx->depth) {
      var->varno = ctx->correct_varno;
      var->varlevelsup = ctx->correct_varlevelsup;
    }
    return false;
  }

  return expression_tree_walker(node, VarAdjustMutator, context);
}

Expr* BuildExpressionForNestedPolicy(RangeTblEntry* rte, std::string varname,
                                     Var* var, ResTarget* mask) {
  // Compose an SQL SELECT query.
  std::string select_sql =
      "SELECT " + varname + " FROM " + GetRTEQueryTargetName(rte);

  SelectStmt* select_q = reinterpret_cast<SelectStmt*>(
      linitial(ParseQuery(select_sql.c_str(), LOG_TAG_DDM)));

  // Replace target with masked target.
  ResTarget* masked = copyObject(mask);
  masked->name = pstrdup(varname.c_str());

  // Note, from the user perspective, she queries a SUPER navigation path which
  // is always SUPER (even if she later wraps it in CAST). Therefore, if were to
  // change type of the "variable" to scalar policy expression return type, this
  // immediately gives away the application of DDM and may break user
  // expectation.
  TypeCast* cast = makeNode(TypeCast);
  cast->arg = reinterpret_cast<Node*>(masked->val);
  cast->typname = makeTypeNameFromOid(PARTIQLOID, max_partiql_size());
  masked->val = reinterpret_cast<Node*>(cast);

  XCHECK_EQ(1, list_length(select_q->targetList));
  void* old_val = replace_pointer(list_head(select_q->targetList), masked);
  safe_pfree(old_val);

  // Parse-analyze the masked query (ie, from parse tree to query tree).
  List* parsed = parse_analyze_int_rw(
      reinterpret_cast<Node*>(select_q), nullptr /* paramTypes */,
      0 /* numParams */, ParseTypeDDM, true /* post_ds_perm_checks */,
      NIL /* external_rtables */, false /* is_prepared_statement */,
      false /* allow_localized_temps */, NIL /* active_lbvs */,
      false /* only_metadata */, InvalidAclId /* lbv_owner */,
      InvalidOid /* skip_rls_relid */, NIL /* rls_reparse_lbvs */,
      nullptr /* bastion_external_schema */);

  if (gconf_ddm_enable_perm_check) {
    // Permissions are already verified in ddm::VerifyObjectPermissions.
    DisableRTEPermCheck(parsed);
  }

  // Extract target entry from the query tree.
  XCHECK_EQ(1, list_length(parsed), DDM_CALLER);
  Query* q = reinterpret_cast<Query*>(linitial(parsed));
  DisableDdmOnFgacTarget(q, rte);
  XCHECK(q->jointree != nullptr, DDM_CALLER);
  XCHECK(q->jointree->fromlist != nullptr, DDM_CALLER);
  XCHECK_EQ(1, list_length(q->rtable), DDM_CALLER);
  auto rewritten_rte = reinterpret_cast<RangeTblEntry*>(linitial(q->rtable));
  rewritten_rte->has_ddm_rewrite = false;

  XCHECK_EQ(1, list_length(q->targetList), DDM_CALLER);
  TargetEntry* mask_tgt =
      copyObject(reinterpret_cast<TargetEntry*>(linitial(q->targetList)));

  // Arguments of masking expressions (the Vars) assume that their RTable is 1,
  // which may not hold when the expression is substituted. So we set varno of
  // masking arguments to the varno of the variable which we substitute,
  XCHECK(var != nullptr, DDM_CALLER);
  VarAdjustMutatorContext varShiftCtx{var->varno, var->varlevelsup};
  expression_tree_walker(reinterpret_cast<Node*>(mask_tgt), VarAdjustMutator,
                         &varShiftCtx);

  return mask_tgt->expr;
}

Node* MutateNestedVar(Var* var, RewriteMutatorCtx* context) {
  XCHECK(var != nullptr, DDM_CALLER);
  XCHECK(context != nullptr, DDM_CALLER);

  Node* unchanged = reinterpret_cast<Node*>(var);

  if (!gconf_ddm_enable_over_super_paths) return unchanged;

  // Here, we proceed only for SUPER with nested path, since only such variable
  // can possibly be subject to in-place policy application.
  if (var->vartype != PARTIQLOID || var->partiql_path == NIL) return unchanged;

  // Ensure RTable is stored in the stack for the variable.
  XCHECK(var->varlevelsup < context->rtables.size(), DDM_CALLER);
  RangeTblEntry* rte = rt_fetch(
      var->varno,
      context->rtables[context->rtables.size() - var->varlevelsup - 1]);
  Oid rte_relid = GetRelationRelid(rte);
  // This can happen for non-view subqueries, JOIN subqueries, etc. In such case
  // we know such variable is not a column of DDM-protected relation.
  if (!OidIsValid(rte_relid)) return unchanged;

  // We need a real name of the column to look it up in the map.
  std::string varname = std::string(policy::ColumnNameForRTEVar(rte, var));
  // A name can be empty in case of dropped column.
  if (varname.size() == 0) return unchanged;

  MaskMap mask_map = GetMasksFromCache(&context->mask_maps_cache, rte_relid);
  // If no attachments for variable, return.
  auto mask_map_it = mask_map.find(varname);
  if (mask_map_it == mask_map.end()) return unchanged;

  std::vector<MergeEntry> entries = mask_map_it->second;
  // If attachment is not nested, return.
  if (entries.size() == 1 && !entries[0].nested_path) return unchanged;

  // If we have marked this variable for OBJECT_TRANSFORM, then it has been
  // applied in MutateRTE, and we return.
  auto obj_transform_vars_it = context->obj_transform_vars.find(rte_relid);
  if (obj_transform_vars_it != context->obj_transform_vars.end()) {
    if (obj_transform_vars_it->second.find(varname) !=
        obj_transform_vars_it->second.end()) {
      // Found in a list for OBJECT_TRANSFORM. Therefore, will NOT mutate
      // in-place.
      return unchanged;
    }
  }

  // Construct this Var's path components.
  std::vector<std::string_view> components;
  ListCell* lc = nullptr;
  foreach(lc, var->partiql_path) {
    PartiQLPathElement* path_el =
        reinterpret_cast<PartiQLPathElement*>(lfirst(lc));
    XCHECK(path_el->path_step_type == PARTIQL_PATH_STEP_OBJECT,
           "DDM: SUPER with array path was not added to obj transform");
    XCHECK(IsA(path_el->element, String));
    components.push_back(strVal(path_el->element));
  }

  // Find the correct merge entry (if exists).
  MergeEntry* entry = nullptr;
  for (auto&& possible_entry : entries) {
    XCHECK(possible_entry.nested_path);
    RawPath entry_path(possible_entry.nested_path->data(),
                       possible_entry.nested_path->size());

    if (std::equal(entry_path.begin(), entry_path.end(), components.begin())) {
      entry = &possible_entry;
      break;
    }
  }

  if (entry == nullptr) return unchanged;

  // Finally, if we are at his point we know that: (1) variable is a SUPER with
  // nested path, (2) there is a nested attachment for this exact path of this
  // variable, and (3) the masking policy can be applied in-place.
  Expr* mutated =
      BuildExpressionForNestedPolicy(rte, varname, var, entry->rtgt.get());

  // Note, no need to wrap in nav path since we have checked that the masked
  // path and input paths are the same.
  return reinterpret_cast<Node*>(mutated);
}

}  // namespace ddm
