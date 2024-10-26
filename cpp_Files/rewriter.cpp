/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#include "ddm/rewriter.h"

#include "catalog/pg_policy_mask.h"
#include "ddm/constants.hpp"
#include "ddm/ddm_lf_alp.hpp"
#include "ddm/ddm_over_nested.hpp"
#include "ddm/ddm_policy.hpp"
#include "ddm/policy_access.h"
#include "monitoring/user_query/user_query_info.h"
#include "monitoring/user_query/user_query_info.hpp"
#include "omnisql/omnisql_print_sql.hpp"
#include "omnisql/omnisql_reparse_sql.hpp"
#include "omnisql/omnisql_translate_query.hpp"
#include "omnisql/omnisql_view.h"
#include "parser/parsetree.h"
#include "pg2volt/QueryWalker.h"
#include "policy/policy_utils.h"
#include "policy/policy_utils.hpp"
#include "rewrite/rewriteHandler.h"
#include "seclog/seclog_masking.hpp"
#include "sys/xen_backend_pids_api.h"
#include "systables/stl_applied_masking_policy.hpp"
#include "xen_utils/exception/xcheck.hpp"
#include "xen_utils/smart_ptr.hpp"

#include "pg/src/include/access/xact.h"
#include "pg/src/include/c.h"
#include "pg/src/include/miscadmin.h"
#include "pg/src/include/monitoring_pg_wrapper.h"
#include "pg/src/include/nodes/nodes.h"
#include "pg/src/include/nodes/parsenodes.h"
#include "pg/src/include/nodes/pg_list.h"
#include "pg/src/include/nodes/primnodes.h"
#include "pg/src/include/optimizer/clauses.h"
#include "pg/src/include/postgres_ext.h"
#include "pg/src/include/rewrite/rewriteSupport.h"
#include "pg/src/include/utils/elog.h"
#include "pg/src/include/utils/elog_sqlstate.h"
#include "pg/src/include/utils/lsyscache.h"

#include <iomanip>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <stddef.h>

extern bool gconf_ddm_enable_omni_reparse_on_updates;
extern bool gconf_enable_ddm;
extern bool gconf_enable_ddm_case_sensitive_guc_check;
extern bool gconf_enable_lf_alp;
extern bool gconf_enable_logging_for_ddm_applied_masking_policy;

namespace ddm {

// Flags used for the query_tree_mutator for both initial traversal,
// and traversing subqueries.
const unsigned int kDDMMutatorFlags =
    (QTW_WALK_RELATIONS ^ QTW_DONT_COPY_NODE ^ QTW_WALK_RTE_SUBQUERY);

// Flags used for the query_tree_walker when marking relations as having ddm.
const unsigned int kMarkWalkerFlag =
    (QTW_WALK_RELATIONS ^ QTW_WALK_RTE_SUBQUERY);

/// Struct to manage context for RTE permission push down.
struct RTEPermPushDownContext {
  /// source_rte who's permissions are pushed down.
  RangeTblEntry* source_rte = nullptr;
};

/// Struct representing the context for view definition push down.
struct ViewPushDownContext {
  /// The source_rte representing rewritten view.
  RangeTblEntry* view_rte;
  /// view_rte original relid.
  Oid view_rte_original_relid;
  /// Rewritten view query.
  Query* view_query;
  /// Count of view subquery push down.
  int push_down_count = 0;
};

namespace {

/// Rewrites the provided node. Leverages caches to avoid repeating work.
/// Will rewrite any node which has the following.
///
/// 1. A RangeTblEntry*
/// 2. rte->has_ddm_rewrite is true (set during query parsing).
///
/// The resulting RTE will preserve Alias's and other RTE information.
/// @param[in] Node* node - node to be rewritten.
/// @param[in] void* context - RewriteMutatorContext which is used to represent
/// cached data.
/// @return Node - The node to include in the mutated tree.
/// @throws ereport - If a masking policy cannot be built for the associated
/// query
Node* RewriteMutator(Node* node, void* context);

/// Deparses the given query q and reparses again using OMNI.
/// @param[in] q query to reparse using OMNI.
/// @return Query* reparsed using OMNI.
Query* ReparseOmniQuery(Query* q);

/// Checks if there exist such case where a joinaliasvar entry from the given
/// rte_join_list of RTE_JOINs and an entry in result_relation_indexes refer
/// to the same target relation / target relation alias.
/// @param[in] rte_join_list Vector containing RangeTblEntry* entries of kind
/// RTE_JOIN.
/// @param[in] result_relation_indexes Set containing the indexes of RTEs that
/// refer to result relation / result result relation alias.
/// @return true if a joinaliasvar entry refers to a target relation / target
/// relation alias.
bool JoinaliasvarHasTargetRelation(
    std::vector<RangeTblEntry*> rte_join_list,
    std::unordered_set<Index> result_relation_indexes);

/// Private Walker for pushing down permissions to nested RTEs.
bool RTEPermissionsPushdownWalker(Node* node, void* context) {
  if (node == nullptr) {
    return false;
  }

  if (IsA(node, RangeTblEntry)) {
    XCHECK(context != nullptr);
    auto ctx = reinterpret_cast<RTEPermPushDownContext*>(context);
    RangeTblEntry* rte = reinterpret_cast<RangeTblEntry*>(node);
    if (GetRelationRelid(rte) == GetRelationRelid(ctx->source_rte)) {
      RangeTblEntry* source_rte = ctx->source_rte;
      rte->requiredPerms = source_rte->requiredPerms;
      rte->checkAsUser = source_rte->checkAsUser;
      rte->selectedCols = bms_copy(source_rte->selectedCols);
      rte->updatedCols = bms_copy(source_rte->updatedCols);
    }
    return false;

  } else if (IsA(node, Query)) {
    return query_tree_walker(reinterpret_cast<Query*>(node),
                             RTEPermissionsPushdownWalker, context,
                             QTW_WALK_RELATIONS);
  }

  return expression_tree_walker(node, RTEPermissionsPushdownWalker, context);
}

/// Walker responsible for pushing down unfolded view subquery. In
/// the case of a view, we first rewrite the query as SELECT user_mask FROM
/// view_def followed by a request SELECT user_mask FROM (unfolded_view).
/// This walker takes care of pushing the unfolded view definition into any
/// nested view reference.
bool ViewPushdownWalker(Node* node, void* context) {
  if (node == nullptr) {
    return false;
  }

  if (IsA(node, RangeTblEntry)) {
    XCHECK(context != nullptr);
    auto ctx = reinterpret_cast<ViewPushDownContext*>(context);
    RangeTblEntry* rte = reinterpret_cast<RangeTblEntry*>(node);
    Oid rte_relid = GetRelationRelid(rte);
    if (OidIsValid(rte_relid) && rte_relid == ctx->view_rte_original_relid) {
      XCHECK(ctx->view_query != nullptr);
      rte->subquery = copyObject(ctx->view_query);
      // If rte kind is RTE_RELATION, we need to set the kind to RTE_SUBQUERY
      // as we are setting the view_query as rte's subquery.
      if (rte->rtekind == RTE_RELATION) {
        rte->rtekind = RTE_SUBQUERY;
      }
      rte->has_ddm_rewrite = false;
      ctx->push_down_count = ctx->push_down_count + 1;
    }
    return false;

  } else if (IsA(node, Query)) {
    return query_tree_walker(reinterpret_cast<Query*>(node), ViewPushdownWalker,
                             context, QTW_WALK_RELATIONS);
  }

  return expression_tree_walker(node, ViewPushdownWalker, context);
}

/// For all relation types, distinct applied policies and SQL command types,
/// this method logs an entry into stl_applied_masking_policy table.
/// @param RelAttachments attachments - Cached Attachments for the current query
/// which consists all the applied attachments/policies.
/// @return Logs an entry into stl_applied_masking_policy table.
void LogDDMAppliedPolicies(RelAttachments attachments) {
  int64 recordtime = current_micro_time();
  int64_t query_id = get_current_user_query_id();
  const UserQueryInfo& qinfo = GetUserQueryInfo(query_id);
  std::set<Oid> polids;
  for (auto attachment : attachments) {
    MaskMap masks =
        BuildMaskMap(attachment.second, {} /* obj_transform_alp_ctx */);
    for (auto merge_entries : masks) {
      for (auto merge_entry : merge_entries.second) {
        polids.insert(merge_entry.attachment->pol.pol.polid);
      }
    }
    const char* ddm_relkind = nullptr;
    char relkind = get_rel_relkind(attachment.first);
    if (relkind == RELKIND_RELATION) {
      // Local table.
      ddm_relkind = GetRelationKindName(RELKIND_RELATION);
    } else if (relkind == RELKIND_VIEW) {
      // Local view.
      ddm_relkind = GetRelationKindName(get_view_kind(attachment.first));
    }
    PGCHECK(ddm_relkind != nullptr, "Relation is invalid.");
    for (auto polid : polids) {
      // Log the information in system table.
      stl_applied_masking_policy_rec::log(
          polid, GetUserId(), recordtime, GetMyExposedPid(),
          GetCurrentTransactionIdIfAny(), qinfo.query_id, MyDatabaseId,
          attachment.first /* relid */, ddm_relkind);
    }
  }
}

/// TODO(bogatov): VarAdjustMutator in ddm_over_nested file is a more robust
/// version of this.
/// Mutator which adjusts varlevelsup to be equal to the depth
/// of the context and, updates the varno and varnoold. This change ensures that
/// vars on the cherry picked masking expressions for UPDATE and DELETE queries
/// points to the correct rtable in the top level query.
/// @param Node* node - Node which is being traversed.
/// @param RewriteMutatorCtx* context - Mutator context containing depth field.
/// @return bool - Whether to recurse into the node.
///                For use in expression_tree_walker
bool VarLevelsUpMutator(Node* node, void* context) {
  if (node == NULL) return false;

  if (IsA(node, Var)) {
    auto var = reinterpret_cast<Var*>(node);
    auto ctx = reinterpret_cast<RewriteMutatorCtx*>(context);
    /// Only update the mutator when it is the first RTIndex in the masking expr
    /// subquery.
    if (var->varno == 1) {
      var->varlevelsup = ctx->rtables.size() - 1;
      var->varno = ctx->target_varno;
      var->varnoold = ctx->target_varno;
    }
    return false;
  }

  return expression_tree_walker(node, VarLevelsUpMutator, context);
}

/// Builds a Masking policy query from cached attachments.
/// Stores resulting query in the provided context's queries map.
/// @param[in] RangeTblEntry* rte - RangeTable to build a masking query for.
/// @param[in] RewriteMutatorCtx* ctx - Context object tracking cached data.
/// @return bool - True if Query was built.
bool PopulateQueryFromAttachments(const RangeTblEntry* rte,
                                  RewriteMutatorCtx* ctx) {
  std::unordered_map<Oid, std::set<ParsedPolicyAttachment>>::iterator
      cached_attachments = ctx->attachments.find(GetRelationRelid(rte));

  if (cached_attachments != ctx->attachments.end()) {
    if (!cached_attachments->second.empty()) {
      Query* q = MergePolsForRTE(
          rte, ctx->top_query, cached_attachments->second,
          ctx->obj_transform_vars,
          ctx->obj_transform_alp_columns_rel_map[GetRelationRelid(rte)]);
      ctx->queries.insert(
          {GetRelationRelid(rte), std::shared_ptr<Query>(q, PAlloc_deleter())});
    } else {
      ctx->queries.insert({GetRelationRelid(rte), std::nullopt});
    }
    return true;
  }

  return false;
}

/// Builds a Masking policy query from source Catalog tables.
/// Guarantees that each associated policy ID is parsed at most once.
/// @param[in] RangeTblEntry* rte - RangeTable to build a masking query for.
/// @param[in] RewriteMutatorCtx* ctx - Context object tracking cached data.
/// @return bool - True if Query was built.
/// @throws ereport - If a masking policy cannot be found for the masked
/// attachment.
bool PopulateQueryFromCatalog(const RangeTblEntry* rte,
                              RewriteMutatorCtx* ctx) {
  const std::set<Attachment>& attachments = FetchAttachmentsForRel(
      GetRelationRelid(rte), GetUserId(), std::nullopt /* roleid */);
  std::set<ParsedPolicyAttachment> parsed_attachments;
  for (auto attach : attachments) {
    const std::unordered_map<Oid, ParsedPolicy>::iterator& cached_pol =
        ctx->policies.find(attach.polid);
    ParsedPolicy parsed_pol;
    if (cached_pol != ctx->policies.end()) {
      parsed_pol = cached_pol->second;
      attach.pol = cached_pol->second.pol;
    } else {
      // First check seclog attachments. They are precompiled.
      std::optional<Policy> seclog_policy = seclog::GetPolicy(attach.relid);
      if (seclog_policy) {
        XCHECK(attach.pol);
      } else {
        attach.pol = FetchPolicy(attach.polid);
      }
      if (!attach.pol.has_value()) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                        "Could not find policy for mask attachment"));
      }
      parsed_pol = ParsePolicy(attach.pol.value());
      if (!attach.pol->is_seclog) {
        // Do not cache SecLog attachments since (1) they are constant lookup,
        // (2) we cache by Policy ID which is not set for SecLog policies, and
        // SecLog policy generation does not involve parser.
        ctx->policies.insert({parsed_pol.pol.polid, parsed_pol});
      }
    }
    parsed_attachments.insert(ParseAttachment(attach, parsed_pol));
  }
  ctx->attachments.insert({GetRelationRelid(rte), parsed_attachments});
  return PopulateQueryFromAttachments(rte, ctx);
}

/// Builds Subquery for the given relation. Leverages caches stored in ctx
/// to build Masking queries. Subquery is built using policies associated
/// to GetUserId(), and each query, policy, and attachment is parsed once.
/// @param[in] RangeTblEntry* rte - RangeTable to build a masking query for.
/// @param[in] RewriteMutatorCtx* ctx - Context object tracking cached data.
/// @return optional<shared_ptr<Query>> - built query. Empty if there were
///                                       no attachments.
/// @throws ereport - If a masking policy cannot be built.
std::optional<std::shared_ptr<Query>> BuildSubQueryForRel(
    const RangeTblEntry* rte, RewriteMutatorCtx* ctx) {
  Oid relid = GetRelationRelid(rte);
  XCHECK(OidIsValid(relid));

  // If LF-ALP is defined for RTE, short circuit since LF-ALP and DDM are
  // currently mutually exclusive.
  if (gconf_enable_lf_alp && ddm::HasLakeFormationALPForRTE(rte)) {
    return std::shared_ptr<Query>(
        MergePolsForRTE(rte, ctx->top_query, {} /* attachments */,
                        ctx->obj_transform_vars,
                        ctx->obj_transform_alp_columns_rel_map[relid]),
        PAlloc_deleter());
  }

  // First, look for already built queries for the given relid in the local
  // ctx->queries directly in the context memory.
  std::unordered_map<Oid, std::optional<std::shared_ptr<Query>>>::iterator
      cached_query = ctx->queries.find(relid);
  if (cached_query != ctx->queries.end()) {
    return cached_query->second;
  } else if (PopulateQueryFromAttachments(rte, ctx)) {
    // If queries not found in the context, try to build them from fetched
    // attachments.
    cached_query = ctx->queries.find(relid);
  } else if (PopulateQueryFromCatalog(rte, ctx)) {
    // Otherwise, fetch the attachment and build the queries from them.
    cached_query = ctx->queries.find(relid);
  } else {
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    "Could not build masking policies for query"));
  }

  return cached_query->second;
}

/// Convert DDM protected RTEs into RTE_SUBQUERY. Mutates the node in place.
/// If the DDM portected RTE already has a subquery, we unfold the DDM inside of
/// view, store the view subquery and push down the view subquery.
/// Permission Checks are performed, and the resulting Node has permission
/// checks disabled. The rational for this approach is described below.
///
/// Currently, Redshift uses the following procedure for performing
/// permission checks.
///  1. Evaluate permissions in-line QueryRewriteAllWithOptions.
///  2. (Optional) During RLS Rewriting.
///  3. At Query Planning time.
/// As OmniSQL reparses the query, it's impossible to carry definer
/// permissions for views, RLS, or DDM policies permissions past OmniSQL.
/// The current solution to this problem is for each component to call
/// CheckRTEPermissions to check permissions,
/// followed either by a call to DisableRTEPermCheck or a boolean passthrough on
/// disable_permission_checks in QueryRewriteAll. This approach is secure,
/// however caution must be exercised to ensure that permissions cannot
/// leak. In DDM, we return a boolean to QueryRewriteAll and treat DDM
/// permissions in the same manner as view permissions.
///
/// @param[in] Node* node - RTE to be replaced with SubQuery.
/// @param[in] RewriteMutatorCtx* ctx - Context for the rewrite operation.
/// @return Node* - rewritten RTE.
Node* MutateRTE(Node* node, RewriteMutatorCtx* ctx) {
  XCHECK(ctx != nullptr, DDM_CALLER);
  XCHECK(node != nullptr, DDM_CALLER);
  XCHECK(IsA(node, RangeTblEntry), DDM_CALLER);

  auto rte = reinterpret_cast<RangeTblEntry*>(node);
  std::optional<Query*> view_sub_query;

  /// Ignore mutating placeholder RTE entries added during view rewrite.
  if (rte->eref != NULL && (strcmp(rte->eref->aliasname, "*OLD*") == 0 ||
                            strcmp(rte->eref->aliasname, "*NEW*") == 0)) {
    return reinterpret_cast<Node*>(rte);
  }

  if (rte->subquery != nullptr) {
    /// This is unfolding DDM inside of views. We are storing the view subquery
    /// because BuildSubQueryForRel below will build a DDM subquery that
    /// contains a view reference instead of the view's subquery.
    ctx->rtables.push_back(rte->subquery->rtable);
    rte->subquery = reinterpret_cast<Query*>(query_tree_mutator(
        rte->subquery, RewriteMutator, ctx, kDDMMutatorFlags));
    ctx->rtables.pop_back();
    view_sub_query = rte->subquery;
  }

  if (rte->has_ddm_rewrite) {
    if (gconf_enable_ddm_case_sensitive_guc_check &&
        !seclog::HasAttachment(GetRelationRelid(rte)) &&
        CheckCaseSensitiveGucsChanged()) {
      // Note, seclog::HasAttachment means RTE is SecLog-governed and thus
      // cannot have regular DDM policies.
      ereport(
          ERROR,
          (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
           errmsg(
               "DDM protected relation does not support session level config "
               "on case sensitivity being different from its default value.")));
    }

    // When parsing we set has_ddm_rewrite if there is any attachment on the
    // rel. This is necessary to enforce Executor privilege views as
    // currently all views are unfolded with view owner as the current user
    // id. When we do permission checks on a per-user basis, this can lead to
    // an empty attachment set for the RTE which we model as an optional sub
    // query.
    std::optional<std::shared_ptr<Query>> q_opt = BuildSubQueryForRel(rte, ctx);
    if (q_opt) {
      Query* q = copyObject(q_opt->get());
      // Check Permissions on DDM subquery. First push down permissions from
      // parent RTE, then apply CheckRTEPermissions. This approach avoids any
      // risk that the query contains objects that the user should not have
      // access to - such as the case when a lookup table's permissions are
      // revoked from the policy.

      RTEPermPushDownContext push_down_ctx = {rte /* source_rte */};
      query_tree_walker(q, RTEPermissionsPushdownWalker, &push_down_ctx,
                        QTW_WALK_RELATIONS);
      CheckRTEPermissions(q);
      ctx->ddm_applied = true;
      XCHECK(q != NULL, DDM_CALLER);
      // After we set view RTE Subquery with masking query, we may not have
      // the placeholder view old rte for getting view relid. We get the
      // RTE relid before setting the masking query as subquery.
      Oid rte_relid = GetRelationRelid(rte);
      rte->subquery = q;
      rte->rtekind = RTE_SUBQUERY;
      rte->has_ddm_rewrite = false;

      // Push down view subquery.
      if (view_sub_query) {
        // To push down unfolded view, when view kind is regular view,
        // traversing of QTW_WALK_RELATIONS is required. When view kind is LBV
        // traversing of QTW_WALK_RTE_SUBQUERY and ignoring
        // QTW_IGNORE_RT_SUBQUERIES is required.
        ViewPushDownContext view_pushdown_ctx = {rte, rte_relid,
                                                 *view_sub_query};
        query_tree_walker(rte->subquery, ViewPushdownWalker, &view_pushdown_ctx,
                          QTW_WALK_RELATIONS ^ QTW_WALK_RTE_SUBQUERY ^
                              QTW_IGNORE_RT_SUBQUERIES);
        XCHECK(view_pushdown_ctx.push_down_count == 1, DDM_CALLER);
      }
    }
    rte->has_ddm_rewrite = false;
  }

  return reinterpret_cast<Node*>(rte);
}

/// To avoid a nested loop join. We do not rewrite rtable when it is the
/// target of a Delete or update statement. Instead we cherry pick the
/// expressions from the masking policy and attach them to each var. We
/// validate that we have the correct number of varlevelsup vs. depth to
/// ensure we are only rewriting references to the result ref.
/// @param[in] Node* node - node to be rewritten.
/// @param[in] RewriteMutatorCtx* ctx - context to use for rewrite.
/// return Node* - rewritten node.
Node* MutateVar(Node* node, RewriteMutatorCtx* ctx) {
  XCHECK(IsA(node, Var), DDM_CALLER);
  XCHECK(ctx != nullptr, DDM_CALLER);
  XCHECK(node != nullptr, DDM_CALLER);

  auto var = reinterpret_cast<Var*>(node);

  // Since MutateVar is only meant to be used in UPDATE and DELETE queries, we
  // ensure that the following processing logic applies only to variables of
  // result relation or result relation alias.
  if (ctx->result_relation_indexes.count(var->varno - 1) &&
      ctx->rewrite_result_refs &&
      (ctx->rtables.size() - 1) == var->varlevelsup) {
    RangeTblEntry* rte = rt_fetch(var->varno, ctx->rtables[0]);
    ctx->target_varno = var->varno;

    std::optional<std::shared_ptr<Query>> sub_query_opt =
        BuildSubQueryForRel(rte, ctx);
    if (sub_query_opt) {
      Query* sub_query = copyObject(sub_query_opt->get());
      TargetEntry* mask_tgt = copyObject(reinterpret_cast<TargetEntry*>(
          lfirst(list_nth_cell(sub_query->targetList, var->varattno - 1))));
      /// We walk the TargetEntry and replace result rtable references with
      /// the correct depth.
      expression_tree_walker(reinterpret_cast<Node*>(mask_tgt),
                             VarLevelsUpMutator, ctx);

      // Will apply SUPER navigation only if applicable.
      return MakePartiQLNavigationPath(var,
                                       reinterpret_cast<Node*>(mask_tgt->expr));
    }
  }
  return node;
}

/// Rewrites the provided node. Leverages caches to avoid repeating work.
/// Will rewrite any node which has the following.
///
/// 1. A RangeTblEntry*
/// 2. rte->has_ddm_rewrite is true (set during query parsing).
/// 3. rte->rtekind is not RTE_SUBQUERY.
///
/// The resulting RTE will preserve Alias's and other RTE information.
/// @param[in] Node* node - node to be rewritten.
/// @param[in] void* context - RewriteMutatorContext which is used to represent
/// cached data.
/// @return Node - The node to include in the mutated tree.
/// @throws ereport - If a masking policy cannot be built for the associated
/// query
Node* RewriteMutator(Node* node, void* context) {
  auto ctx = reinterpret_cast<RewriteMutatorCtx*>(context);

  if (node == nullptr) return node;

  if (IsA(node, RangeTblEntry)) return MutateRTE(node, ctx);

  if (IsA(node, Query)) {
    auto input_q = reinterpret_cast<Query*>(node);
    ctx->rtables.push_back(input_q->rtable);
    Query* result =
        query_tree_mutator(input_q, RewriteMutator, context, kDDMMutatorFlags);
    ctx->rtables.pop_back();
    // This invariant is required by omni.
    XCHECK(input_q == result, DDM_CALLER);
    return reinterpret_cast<Node*>(result);
  }

  if (IsA(node, Var)) {
    if (ctx->cmd_type == CMD_UPDATE || ctx->cmd_type == CMD_DELETE) {
      return MutateVar(node, ctx);
    } else {
      return MutateNestedVar(reinterpret_cast<Var*>(node), ctx);
    }
  }

  return expression_tree_mutator_with_flags(node, RewriteMutator, context,
                                            kDDMMutatorFlags);
}

Query* ReparseOmniQuery(Query* q) {
  std::string sql_omni = omnisql::PrintSQL(omnisql::TranslateQuery(q));
  List* reparsed_list = omnisql::ReparseSQL(
      sql_omni.c_str(), const_cast<Query*>(q), LOG_TAG_DDM, ParseTypeDDM);

  XCHECK(list_length(reparsed_list) == 1, DDM_CALLER);
  Query* omni_reparsed = reinterpret_cast<Query*>(linitial(reparsed_list));
  omnisql::RestoreQuery(q, omni_reparsed);
  return omni_reparsed;
}

bool JoinaliasvarHasTargetRelation(
    std::vector<RangeTblEntry*> rte_join_list,
    std::unordered_set<Index> result_relation_indexes) {
  for (RangeTblEntry* rte_join : rte_join_list) {
    XCHECK(rte_join != nullptr, DDM_CALLER);
    XCHECK(rte_join->rtekind == RTE_JOIN, DDM_CALLER);
    if (rte_join->joinaliasvars == nullptr) continue;

    ListCell* lc;
    foreach(lc, rte_join->joinaliasvars) {
      Var* var = reinterpret_cast<Var*>(lfirst(lc));
      XCHECK(var != nullptr, DDM_CALLER);
      XCHECK(IsA(var, Var), DDM_CALLER);

      // Result relation index value 'x' and var with varno 'x-1' refer to
      // same target relation / target relation alias.
      if (result_relation_indexes.count(var->varno - 1)) return true;
    }
  }

  return false;
}
}  // namespace

RewriteResult RewriteQuery(Query* q, bool log_applied_policy) {
  // We operate on the same command types across DDM and RLS for now.
  if (!gconf_enable_ddm ||
      !policy::IsSecureRewriteEnabledCommand(q->commandType)) {
    return {false /* ddm_applied */, q /* rewrite */};
  }
  RewriteMutatorCtx ctx;
  std::vector<RangeTblEntry*> rte_join_list;
  bool omni_reparse_required = false;
  ctx.top_query = q;

  ctx.cmd_type = q->commandType;
  ctx.rtables.push_back(q->rtable);
  ctx.log_applied_policy = log_applied_policy;
  ctx.result_relation_indexes.insert(q->resultRelation - 1);

  if (ctx.cmd_type == CMD_UPDATE || ctx.cmd_type == CMD_DELETE ||
      ctx.cmd_type == CMD_INSERT) {
    RangeTblEntry* rte = rt_fetch(q->resultRelation - 1, q->rtable);

    if (rte->has_ddm_rewrite) {
      if (gconf_enable_ddm_case_sensitive_guc_check &&
          CheckCaseSensitiveGucsChanged()) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("DDM protected relation does not support "
                               "session level config on case sensitivity being "
                               "different from its default value.")));
      }

      ctx.rewrite_result_refs = true;
      // if we are rewriting the result ref, we must apply the permissions check
      // to the result ref and then inform QueryRewriteAll that we should
      // disable checks post-omni.
      ExecCheckRTEPerms(rte, false /* recursive */, false /* clp_only */,
                        false /* is_materialized_view */,
                        false /* check_schema_priv_as_view_owner */,
                        nullptr /* data_sharing_temp_views */);
      ctx.ddm_applied = true;

      Oid result_rte_relid = GetRelationRelid(rte);
      XCHECK(OidIsValid(result_rte_relid));

      // We identify all the rtes that are result relation / result relation
      // alias. We keep track of these rte indexes to only mutate vars that
      // belong to these rtes during UPDATE.
      ListCell* rt;
      Index index = 0;
      foreach(rt, q->rtable) {
        RangeTblEntry* rte_ptr = reinterpret_cast<RangeTblEntry*>(lfirst(rt));

        if (GetRelationRelid(rte_ptr) == result_rte_relid) {
          ctx.result_relation_indexes.insert(index);
          rte_ptr->has_ddm_rewrite = false;
        }

        if (rte_ptr->rtekind == RTE_JOIN) rte_join_list.push_back(rte_ptr);
        ++index;
      }

      // ON UPDATE queries, we perform in place mutation on those vars which
      // refer to target relation / target relation alias. If UPDATE query
      // has JOIN condition, MutateVar() also mutates those vars in the
      // joinaliasvars list of RTE_JOIN that refer to target relation / target
      // relation alias. In place mutation of such vars results in nested loop
      // join error in planner. To avoid this,
      // 1. We mark the query as omni_reparse_required which:
      //  (a) is an UPDATE statement.
      //  (b) has DDM protected target relation.
      //  (c) has joinaliasvar that refers to target relation / target relation
      //      alias.
      // 2. Once DDM rewrite is performed, we deparse the marked query and
      // reparse again using OMNI. Omni avoids nested loop joins by flattening
      // the query:
      // (a) OMNI has a query normalization to remove use of join alias vars.
      // (b) In join tree quals, instead of referring joinaliasvars, OMNI
      //     directly references relations from the top query along with
      //     applying the corresponding masking expressions it found from
      //     the deparsed query.
      if (gconf_ddm_enable_omni_reparse_on_updates && !rte_join_list.empty() &&
          ctx.cmd_type == CMD_UPDATE &&
          JoinaliasvarHasTargetRelation(rte_join_list,
                                        ctx.result_relation_indexes)) {
        omni_reparse_required = true;
      }
    }
  }
  ctx.obj_transform_alp_columns_rel_map = WalkVarLevelALP(q);

  ctx.obj_transform_vars = WalkInPlaceNestedVars(q);

  Query* rewrite =
      query_tree_mutator(q, RewriteMutator, &ctx, kDDMMutatorFlags);

  // Logging of applied masking policy.
  if (gconf_enable_logging_for_ddm_applied_masking_policy &&
      ctx.log_applied_policy) {
    LogDDMAppliedPolicies(ctx.attachments);
  }

  if (omni_reparse_required) {
    return {ctx.ddm_applied /* ddm_applied */,
            ReparseOmniQuery(rewrite) /*rewrite */, true /*omni_reparsed*/};
  }

  return {ctx.ddm_applied /* ddm_applied */, rewrite /*rewrite */,
          false /*omni_reparsed*/};
}

namespace {
bool MarkDDMWalker(Node* node, void* context) {
  if (node == nullptr) {
    return false;
  }

  if (IsA(node, RangeTblEntry)) {
    RangeTblEntry* rte = reinterpret_cast<RangeTblEntry*>(node);
    if (GetRelationRelid(rte) != InvalidOid && HasDDMRewrite(rte)) {
      rte->has_ddm_rewrite = true;
    }
    return false;
  } else if (IsA(node, Query)) {
    return query_tree_walker(reinterpret_cast<Query*>(node), MarkDDMWalker,
                             context, kMarkWalkerFlag);
  }

  return expression_tree_walker(node, MarkDDMWalker, context);
}

void MarkDDMRelations(Query* q) {
  query_tree_walker(q, MarkDDMWalker, nullptr, kMarkWalkerFlag);
}
}  // namespace
}  // namespace ddm

bool RewriteDDM(List* queries, bool log_applied_policy) {
  if (!gconf_enable_ddm) return false;
  xen_spin("ddm_rewriter");
  bool ddm_applied = false;
  ListCell* query = NULL;
  foreach(query, queries) {
    Query* q = reinterpret_cast<Query*>(lfirst(query));
    int64 query_id = q->queryId;
    struct HeapMemoryUsageStats entry_usage_stats = {};
    PG_GET_HEAP_MEM_USAGE_LOG_ENTRY(kRewriteDDMEntry, query_id,
                                    &entry_usage_stats);
    ddm::RewriteResult rewrite_result = ddm::RewriteQuery(
        reinterpret_cast<Query*>(lfirst(query)), log_applied_policy);

    if (rewrite_result.omni_reparsed) {
      // We are not freeing the old query here. Because the caller
      // QueryRewriteAllWithOptions() refers to the the original querytree
      // afterwards.
      replace_pointer(query, rewrite_result.rewrite);
    }

    ddm_applied |= rewrite_result.ddm_applied;
    PG_HEAP_MEM_USAGE_LOG_EXIT(kRewriteDDMExit, query_id, &entry_usage_stats);
  }
  return ddm_applied;
}

void MarkDDMRelations(Query* q) { ddm::MarkDDMRelations(q); }
