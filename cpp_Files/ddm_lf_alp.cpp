/// Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#include "ddm/ddm_lf_alp.hpp"

#include "ddm/constants.hpp"
#include "ddm/ddm_policy.hpp"
#include "external_catalog/table_registry.hpp"
#include "federation/FederatedSnapshotManager.hpp"
#include "nested_fe/nested_query_api.h"
#include "nodes/makefuncs.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "policy/policy_utils.hpp"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "xen_utils/xen_except.h"

#include <boost/algorithm/string.hpp>

#include <algorithm>
#include <iomanip>
#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

extern bool gconf_enable_lf_alp;
extern bool gconf_enable_lf_alp_var_optimization;
extern bool gconf_spectrum_enable_lf_data_filters;

namespace ddm {

// Flags used for the VarLevelALPWalker. We specifically do not let the walker
// walk into non-RLS subqueries since we need to adjust the context for it.
const unsigned int kVarLevelALPWalkerFlag =
    QTW_WALK_RELATIONS | QTW_WALK_RLS_SUBQUERIES;

bool HasLakeFormationALPForRTE(const RangeTblEntry* rte) {
  if (!gconf_enable_lf_alp) return false;

  char* db_name = nullptr;
  char* ns_name = nullptr;
  char* rel_name = nullptr;

  if (!IsLfManagedRelation(rte, &db_name, &ns_name, &rel_name)) {
    // Not a datasharing relation accessed via LF.
    return false;
  }

  XCHECK(db_name != nullptr, LF_ALP_CALLER);
  XCHECK(ns_name != nullptr, LF_ALP_CALLER);
  XCHECK(rel_name != nullptr, LF_ALP_CALLER);

  return fsmutils::GetAuthorizedNestedPaths(db_name, ns_name, rel_name,
                                            true /* found_only */)
      .first;
}

std::optional<LFALPPolicy> GetLakeFormationALPForRTE(const RangeTblEntry* rte) {
  if (!gconf_enable_lf_alp) return std::nullopt;

  char* db_name = nullptr;
  char* ns_name = nullptr;
  char* rel_name = nullptr;

  if (!IsLfManagedRelation(rte, &db_name, &ns_name, &rel_name)) {
    // Not a datasharing relation accessed via LF.
    return std::nullopt;
  }

  XCHECK(db_name != nullptr, LF_ALP_CALLER);
  XCHECK(ns_name != nullptr, LF_ALP_CALLER);
  XCHECK(rel_name != nullptr, LF_ALP_CALLER);

  auto [found, lf_alp_policy] =
      fsmutils::GetAuthorizedNestedPaths(db_name, ns_name, rel_name);

  return found ? lf_alp_policy
               : static_cast<std::optional<LFALPPolicy>>(std::nullopt);
}

void AddLakeFormationALPToDDMMap(
    MaskMap* masks, LFALPPolicy policy,
    const std::unordered_set<std::string_view>& alp_obj_transform_columns) {
  for (auto&& [column, path_set] : policy) {
    // XCHECK column format (surrounding quotes).
    XCHECK(column[0] == '\"' && column[column.size() - 1] == '\"',
           LF_ALP_CALLER);
    // Extract the values dropping quotes.
    std::string column_unquoted = column.substr(1, column.size() - 2);

    if (gconf_enable_lf_alp_var_optimization) {
      // As an optimization we have visited all SUPER variables that have LF-ALP
      // on them before DDM rewrite. We have collected the variables here whose
      // references (navigation paths) could not be evaluated into ALLOW or DENY
      // (i.e., complex projections). It's only for them that we need to apply
      // OBJECT_TRANSFORM.
      if (alp_obj_transform_columns.find(column_unquoted) ==
          alp_obj_transform_columns.end())
        continue;
    }

    std::vector<std::string> paths;
    std::vector<std::string_view> stack;
    PathSetView(path_set).Export(&paths, &stack);

    // Keeping rewrites deterministic.
    std::sort(paths.begin(), paths.end());

    // Don't forget to escape. We trust the LF... to an extent.
    std::stringstream ss;
    ss << "SELECT "
       << "OBJECT_TRANSFORM(\"" << MASK_PSEUDO_TBL << "\"."
       << std::quoted(column_unquoted, '\"', '\"') << " KEEP ";
    for (size_t i = 0; i < paths.size(); i++) {
      ss << std::quoted(paths[i], '\'', '\\');
      if (i != paths.size() - 1) ss << ", ";
    }
    ss << ")";

    List* parsetree_list = ParseQuery(ss.str().c_str(), LOG_TAG_LF_ALP);
    XCHECK(list_length(parsetree_list) == 1, LF_ALP_CALLER);
    SelectStmt* q = reinterpret_cast<SelectStmt*>(linitial(parsetree_list));

    XCHECK(q->targetList != nullptr, DDM_CALLER);
    XCHECK(list_length(q->targetList) == 1, DDM_CALLER);
    ResTarget* rtgt =
        copyObject(reinterpret_cast<ResTarget*>(linitial(q->targetList)));
    std::shared_ptr<ResTarget> rtgt_ptr(rtgt, PAlloc_deleter());
    safe_pfree(q);

    // Safe to insert since LF-ALP and DDM are mutually exclusive per RTE.
    masks->insert({column_unquoted,
                   {MergeEntry{rtgt_ptr, MAX_INT4 /* priority */,
                               nullptr /* attachment */, column_unquoted,
                               std::nullopt /* nested_path */}}});
  }
}

const external_catalog::Column& OriginalNestedColumnForExternalNestedRTEVar(
    const RangeTblEntry* rte, const Var* var) {
  XCHECK(var->vartype == PARTIQLOID);
  XCHECK(var->varoattno > 0);
  XCHECK(rte->nested_subquery != NULL);
  const external_catalog::Table& table =
      external_catalog::TableRegistry::GetTable(rte->external_table_id);
  const external_catalog::Column& column =
      table.getColumnForIntPathElement(var->varoattno - 1);
  // PartiQL OIDs over external tables can only reference complex types
  // (struct/array/map).
  XCHECK(column.original_type().IsComplexType());
  return column;
}

bool WalkNestedVar(Var* var, VarLevelALPWalkerCtx* context) {
  // Note here there may or may not be a navigation path. Even if there is not
  // (complex projection), we may need to apply ALP.
  if (var->vartype != PARTIQLOID) return false;

  // Ensure RTable is stored in the stack for the variable.
  XCHECK(var->varlevelsup < context->rtables.size(), LF_ALP_CALLER);
  RangeTblEntry* rte = rt_fetch(
      var->varno,
      context->rtables[context->rtables.size() - var->varlevelsup - 1]);

  const Oid relid = GetRelationRelid(rte);
  // This can happen for non-view subqueries, JOIN subqueries, etc. In such case
  // we know such variable is not a column of a protected relation.
  if (!OidIsValid(relid)) return false;

  // Use cache in context to lookup LF-ALP possible for the RTE (by its OID).
  auto obj_transform_alp_columns_rel_map_it =
      context->obj_transform_alp_columns_rel_map.find(relid);
  if (obj_transform_alp_columns_rel_map_it ==
      context->obj_transform_alp_columns_rel_map.end()) {
    context->obj_transform_alp_columns_rel_map[relid] = {
        GetLakeFormationALPForRTE(rte), /* obj_transform_alp_columns */ {}};
  }
  // Note that the object is not allocated, but merely referenced.
  ObjTransformAlpColumnsCtx* obj_transform_alp_columns_ctx =
      &context->obj_transform_alp_columns_rel_map[relid];

  // No policy, no problem.
  if (!obj_transform_alp_columns_ctx->lf_alp_policy) return false;

  // We need a real name of the column to look it up in the policy.
  std::string_view varname = policy::ColumnNameForRTEVar(rte, var);
  // A name can be empty in case of dropped column.
  if (varname.size() == 0) return false;

  const bool is_nested_spectrum_isolated_relation =
      external_catalog::TableRegistry::HasTable(rte->external_table_id) &&
      (rte->nested_subquery != NULL);
  std::stringstream localized_ss_name;
  localized_ss_name << std::quoted(std::string(varname), '\"', '\"');
  // If this refers to an external isolated nested table. In this case, we
  // lookup the original column names from the external table registry, as the
  // relation's attribute attname refers to a temporary localized one.
  if (is_nested_spectrum_isolated_relation) {
    const external_catalog::Column& original_ext_col =
        OriginalNestedColumnForExternalNestedRTEVar(rte, var);
    if (original_ext_col.original_type().IsCollectionType()) {
      // LF-ALP is only supported over complex STRUCT types. In case of
      // collection types (array/map), we short circuit here. Authorized paths
      // within collection types are guaranteed to fail when parsing the
      // authorized paths.
      return false;
    }
    // Lake Formation is guaranteed to escape special characters. This string
    // is not used or injected in the rewrite logic. It is only used to map the
    // appropriate localized column name (in the isolated localized nested
    // table), to the corresponding LF ALP policy.
    std::string original_name = "\"" + original_ext_col.name() + "\"";
    // In this case, we need to reconstruct the `lf_alp_policy_opt` with the
    // modified keys referring to the original column name.
    LFALPPolicy& lf_alp_policy = *obj_transform_alp_columns_ctx->lf_alp_policy;
    if (lf_alp_policy.find(original_name) == lf_alp_policy.end()) {
      return false;
    }
    PathSet& pathset = lf_alp_policy[original_name];
    lf_alp_policy.insert({localized_ss_name.str(), std::move(pathset)});
  }
  // Exit if this column is not ALPed in this RTE.
  auto pathsets_it = obj_transform_alp_columns_ctx->lf_alp_policy->find(
      localized_ss_name.str());
  if (pathsets_it == obj_transform_alp_columns_ctx->lf_alp_policy->end()) {
    return false;
  }

  if (gconf_enable_lf_alp_var_optimization) {
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
        // Treat PARTIQL_PATH_STEP_ARRAY as complex projection.
        break;
      }
    }

    // Decide what to do for the navigation path.
    PathSet::PathMembership membership =
        pathsets_it->second.CheckPathMembership(components);
    switch (membership) {
      case PathSet::PathMembership::kContainedFully:
        // Navigation path is authorized.
        break;
      case PathSet::PathMembership::kNotContained: {
        // Navigation path is unauthorized. Despite knowing the failed
        // navigation path we report a more generic message to exactly match the
        // NRW. For now the only RTE that can be here is a localized Spectrum
        // table, but for future proofness, if it's not, we derive it's name
        // normally.
        char* relation_name = nullptr;
        if (rte->nested_subquery) {
          relation_name =
              const_cast<char*>(getNestedTableName(rte->external_table_id));
        } else {
          relation_name = rte->external_tablename != nullptr
                              ? rte->external_tablename
                              : get_rel_name(relid);
        }
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_CLASS, relation_name);
      } break;
      case PathSet::PathMembership::kContainedPartially: {
        // Navigation path results in a complex project where there are
        // authorized (and therefore possibly unauthorized paths). Mark for
        // OBJECT_TRANSFORM.
        obj_transform_alp_columns_ctx->obj_transform_alp_columns.insert(
            varname);
      } break;
    }
  }
  return false;
}

bool VarLevelALPWalker(Node* node, void* context) {
  if (node == nullptr) return false;

  VarLevelALPWalkerCtx* ctx = reinterpret_cast<VarLevelALPWalkerCtx*>(context);

  // Don't use query_tree_walker for walking into subqueries because we need to
  // set and restore context upon descend into subquery.
  if (IsA(node, RangeTblEntry)) {
    RangeTblEntry* rte = reinterpret_cast<RangeTblEntry*>(node);
    // Don't care what RTE type it is. As long as it has subquery, we descend
    // into it.
    if (rte->subquery != nullptr) {
      ctx->rtables.push_back(rte->subquery->rtable);
      query_tree_walker(reinterpret_cast<Query*>(rte->subquery),
                        VarLevelALPWalker, context, kVarLevelALPWalkerFlag);
      ctx->rtables.pop_back();
    }
    return false;
  }

  // Query node is specifically not handled by the expression_tree_walker.
  if (IsA(node, Query)) {
    ctx->rtables.push_back(reinterpret_cast<Query*>(node)->rtable);
    query_tree_walker(reinterpret_cast<Query*>(node), VarLevelALPWalker,
                      context, kVarLevelALPWalkerFlag);
    ctx->rtables.pop_back();
    return false;
  }

  if (IsA(node, Var)) {
    return WalkNestedVar(reinterpret_cast<Var*>(node), ctx);
  }
  return expression_tree_walker(node, VarLevelALPWalker, ctx);
}

ObjTransformAlpColumnsRelationMap WalkVarLevelALP(Query* query) {
  // We only walk through the ALP Var walker, if LF-ALP over Spectrum or LF-ALP
  // optimizations are enabled. If LF-ALP is disabled, or none of the above are
  // enabled, then return an empty map for the object transformation context.
  const bool alp_walker_eligibility =
      gconf_enable_lf_alp && (gconf_spectrum_enable_lf_data_filters ||
                              gconf_enable_lf_alp_var_optimization);
  if (!alp_walker_eligibility) return ObjTransformAlpColumnsRelationMap();

  VarLevelALPWalkerCtx context;
  context.query = query;
  context.rtables.push_back(query->rtable);
  query_tree_walker(query, VarLevelALPWalker, &context, kVarLevelALPWalkerFlag);
  return context.obj_transform_alp_columns_rel_map;
}

}  // namespace ddm
