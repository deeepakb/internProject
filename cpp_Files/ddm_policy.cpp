/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#include "ddm/ddm_policy.hpp"

#include "access/heapam.h"
#include "catalog/catname.h"
#include "catalog/indexing.h"
#include "catalog/pg_datashare_objects.h"
#include "catalog/pg_permission.h"
#include "catalog/pg_permission_mask.h"
#include "catalog/pg_permission_policy.h"
#include "catalog/pg_policy_mask.h"
#include "commands/rlscmds.h"
#include "ddm/constants.hpp"
#include "ddm/ddm_lf_alp.hpp"
#include "ddm/ddm_over_nested.hpp"
#include "ddm/ddm_permission.hpp"
#include "ddm/ddm_serialization.hpp"
#include "ddm/unsupported.h"
#include "external_catalog/table_registry.hpp"
#include "nested_fe/nested_query_api.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "omnisql/omnisql.h"
#include "omnisql/omnisql_view.h"
#include "parser/analyze.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"
#include "policy/policy_permission.h"
#include "policy/policy_utils.h"
#include "policy/policy_utils.hpp"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteSupport.h"
#include "seclog/seclog_masking.hpp"
#include "storage/lmgr.h"
#include "sys/pg_defs.hpp"
#include "sys/query.hpp"
#include "sys/stats_indicator.hpp"
#include "tcop/tcopprot.h"
#include "util/get_correct_typmod.hpp"
#include "util/stack_guard.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/lsyscache.h"
#include "xen_utils/scope_exit_guard.hpp"

#include "pg/src/include/c.h"
#include "pg/src/include/postgres_ext.h"

#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

extern bool gconf_ddm_enable_grant_revoke_for_functions;
extern bool gconf_ddm_enable_perm_check;
extern bool gconf_ddm_enable_over_super_paths;
extern bool gconf_enable_lf_alp;
extern bool gconf_enable_session_context_variable;
extern bool g_pgtest_ddm;
extern bool gconf_spectrum_enable_lf_data_filters;
extern bool gconf_ddm_enable_lbv_caching;

namespace ddm {

const int RESERVE_POL_SIZE = 50;
const char* EXPRESSION_DELIM = ", ";
/// Default value for typmod as 260.
const int32 defaultTypmod = 260;

/// @brief Checks (and ereport's if check fails) that the DDM attachment is
/// supported in (in this version of padb given GIC settings).
///
/// Currently, only checks that the attachment is not to a SUPER path.
///
/// @param attachment the DDM attachment to check.
/// @throws ereport if check fails.
void CheckAttachmentSupported(Attachment attachment);

/// Construct a select query for the given rte. Resulting query is of the form
/// `select col1, col2, col3... from table_name`
SelectStmt* BuildQueryForRTE(const RangeTblEntry* rte,
                             std::optional<std::vector<std::string>> colnames);

/// @brief A helper for BuildMaskMap to build inital mask map from attachments
/// (includes logic for nested attachments). This does not include LF-ALP and
/// processing of ColumnRef's.
void LoadMaskMapFromAttachments(
    const std::set<ParsedPolicyAttachment>& attachments, MaskMap* masks);

Node* parse_tree_mutator(Node* node, mutator_fn_t mutator, void* context) {
  if (node == nullptr) return nullptr;

  // Guard against stack overflow due to overly complex expressions
  CheckStackDepth();
  // mutate node
  node = mutator(node, context);
  switch (nodeTag(node)) {
    case T_A_Const:
    case T_ColumnRef:
    case T_String:
    case T_SelectStmt:
    case T_ParamRef: {
      return node;
    }
    case T_A_Indirection: {
      A_Indirection* ind = reinterpret_cast<A_Indirection*>(node);
      ind->arg = parse_tree_mutator(ind->arg, mutator, context);
      ListCell* lc = nullptr;
      foreach(lc, ind->indirection) {
        replace_pointer(
            lc, parse_tree_mutator(reinterpret_cast<Node*>(lfirst(lc)), mutator,
                                   context));
      }
      return reinterpret_cast<Node*>(ind);
    }
    case T_A_Indices: {
      A_Indices* idx = reinterpret_cast<A_Indices*>(node);
      idx->lidx = parse_tree_mutator(idx->lidx, mutator, context);
      idx->uidx = parse_tree_mutator(idx->uidx, mutator, context);
      return reinterpret_cast<Node*>(idx);
    }
    case T_TypeCast: {
      TypeCast* tc = reinterpret_cast<TypeCast*>(node);
      tc->arg = parse_tree_mutator(tc->arg, mutator, context);
      return reinterpret_cast<Node*>(tc);
    }
    case T_TryCast: {
      TryCast* tc = reinterpret_cast<TryCast*>(node);
      tc->arg = parse_tree_mutator(tc->arg, mutator, context);
      return reinterpret_cast<Node*>(tc);
    }
    case T_A_Expr: {
      A_Expr* a = reinterpret_cast<A_Expr*>(node);
      a->lexpr = parse_tree_mutator(a->lexpr, mutator, context);
      a->rexpr = parse_tree_mutator(a->rexpr, mutator, context);
      return reinterpret_cast<Node*>(a);
    }
    case T_FuncCall: {
      FuncCall* fn = reinterpret_cast<FuncCall*>(node);
      ListCell* arg = nullptr;
      foreach(arg, fn->args) {
        replace_pointer(arg,
                        parse_tree_mutator(reinterpret_cast<Node*>(lfirst(arg)),
                                           mutator, context));
      }
      return reinterpret_cast<Node*>(fn);
    }
    case T_SubLink: {
      SubLink* sl = reinterpret_cast<SubLink*>(node);
      if (sl->subLinkType != EXISTS_SUBLINK && sl->subLinkType != ANY_SUBLINK) {
        ereport(
            ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("Query type not supported in data masking expression.")));
      }
      ListCell* lc = nullptr;
      foreach(lc, sl->lefthand) {
        replace_pointer(
            lc, parse_tree_mutator(reinterpret_cast<Node*>(lfirst(lc)), mutator,
                                   context));
      }
      foreach(lc, sl->operName) {
        replace_pointer(
            lc, parse_tree_mutator(reinterpret_cast<Node*>(lfirst(lc)), mutator,
                                   context));
      }
      foreach(lc, sl->operOids) {
        replace_pointer(
            lc, parse_tree_mutator(reinterpret_cast<Node*>(lfirst(lc)), mutator,
                                   context));
      }
      sl->subselect = parse_tree_mutator(sl->subselect, mutator, context);
      return reinterpret_cast<Node*>(sl);
    }
    case T_CaseExpr: {
      CaseExpr* c = reinterpret_cast<CaseExpr*>(node);
      c->arg = reinterpret_cast<Expr*>(parse_tree_mutator(
          reinterpret_cast<Node*>(c->arg), mutator, context));
      c->defresult = reinterpret_cast<Expr*>(parse_tree_mutator(
          reinterpret_cast<Node*>(c->defresult), mutator, context));
      ListCell* arg = nullptr;
      foreach(arg, c->args) {
        replace_pointer(arg,
                        parse_tree_mutator(reinterpret_cast<Node*>(lfirst(arg)),
                                           mutator, context));
      }
      return reinterpret_cast<Node*>(c);
    }
    case T_CaseWhen: {
      CaseWhen* cw = reinterpret_cast<CaseWhen*>(node);
      cw->expr = reinterpret_cast<Expr*>(parse_tree_mutator(
          reinterpret_cast<Node*>(cw->expr), mutator, context));
      cw->result = reinterpret_cast<Expr*>(parse_tree_mutator(
          reinterpret_cast<Node*>(cw->result), mutator, context));
      return reinterpret_cast<Node*>(cw);
    }
    case T_NullTest: {
      NullTest* nt = reinterpret_cast<NullTest*>(node);
      nt->arg = reinterpret_cast<Expr*>(parse_tree_mutator(
          reinterpret_cast<Node*>(nt->arg), mutator, context));
      return reinterpret_cast<Node*>(nt);
    }
    case T_MinMaxExpr: {
      MinMaxExpr* mme = reinterpret_cast<MinMaxExpr*>(node);
      ListCell* arg = nullptr;
      foreach(arg, mme->args) {
        replace_pointer(
            arg, reinterpret_cast<Node*>(parse_tree_mutator(
                     reinterpret_cast<Node*>(lfirst(arg)), mutator, context)));
      }
      return reinterpret_cast<Node*>(mme);
    }
    case T_GroupingFunc: {
      GroupingFunc* g_func = reinterpret_cast<GroupingFunc*>(node);
      ListCell* arg = nullptr;
      foreach(arg, g_func->args) {
        replace_pointer(arg,
                        parse_tree_mutator(reinterpret_cast<Node*>(lfirst(arg)),
                                           mutator, context));
      }

      foreach(arg, g_func->refs) {
        replace_pointer(arg,
                        parse_tree_mutator(reinterpret_cast<Node*>(lfirst(arg)),
                                           mutator, context));
      }
      return reinterpret_cast<Node*>(g_func);
    }
    case T_BooleanTest: {
      BooleanTest* b_test = reinterpret_cast<BooleanTest*>(node);
      b_test->arg = reinterpret_cast<Expr*>(parse_tree_mutator(
          reinterpret_cast<Node*>(b_test->arg), mutator, context));
      return reinterpret_cast<Node*>(b_test);
    }
    case T_List: {
      // OBJECT_TRANSFORM arguments are T_Lists.
      List* list = reinterpret_cast<List*>(node);
      ListCell* arg = nullptr;
      foreach(arg, list) {
        replace_pointer(
            arg, reinterpret_cast<Node*>(parse_tree_mutator(
                     reinterpret_cast<Node*>(lfirst(arg)), mutator, context)));
      }
      return reinterpret_cast<Node*>(list);
    }
    default: {
      ereport(ERROR,
              (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
               errmsg("Query type not supported in data masking expression.")));
    }
  }
  // Unreachable statement.
  XCHECK_UNREACHABLE(false, DDM_CALLER);
}

std::vector<std::shared_ptr<ResTarget>> ParsePolExpr(const std::string& expr) {
  std::vector<ParsedPolicyExpression> parsed_exprs =
      DeserializeParsedPolicyExpressions(expr);

  std::string mask_expression;
  mask_expression.reserve(RESERVE_POL_SIZE);
  for (auto expr : parsed_exprs) {
    if (!mask_expression.empty()) {
      mask_expression.append(EXPRESSION_DELIM);
    }
    mask_expression.append(expr.expression);
  }

  std::string pol_query = "SELECT " + mask_expression + ";";
  List* parsetree_list = NIL;
  parsetree_list = ParseQuery(pol_query.c_str(), LOG_TAG_DDM);
  XCHECK(list_length(parsetree_list) == 1, DDM_CALLER);
  SelectStmt* q = reinterpret_cast<SelectStmt*>(linitial(parsetree_list));
  XCHECK(q->targetList != NIL, DDM_CALLER);
  XCHECK(list_length(q->targetList) >= 1, DDM_CALLER);

  std::vector<std::shared_ptr<ResTarget>> targets = {};
  ListCell* lc = nullptr;
  ResTarget* rtgt = nullptr;

  foreach(lc, q->targetList) {
    rtgt = copyObject(reinterpret_cast<ResTarget*>(lfirst(lc)));
    std::shared_ptr<ResTarget> rtgt_ptr(rtgt, PAlloc_deleter());
    targets.push_back(rtgt_ptr);
  }
  safe_pfree(q);

  return targets;
}

/// Entry point for Policy Parsing.
ParsedPolicy ParsePolicy(const Policy& pol) {
  const std::vector<std::shared_ptr<ResTarget>>& targets =
      ParsePolExpr(pol.polexpr);
  const std::vector<std::shared_ptr<ColumnDef>>& attrs =
      DeserializeColumnDefs(pol.polattrs);

  return {pol, attrs, targets};
}

ParsedPolicyAttachment ParseAttachment(
    const Attachment& attachment,
    const std::optional<ParsedPolicy> parsed_pol) {
  XCHECK(attachment.pol.has_value(), DDM_CALLER);

  // Lock the target relation so that it doesn't get altered or dropped.
  Relation rel = relation_open(attachment.relid, AccessShareLock);
  SCOPE_EXIT_GUARD(relation_close(rel, AccessShareLock));

  const ParsedPolicy& pol =
      parsed_pol.value_or(ParsePolicy(attachment.pol.value()));

  std::vector<AttachedAttribute> polattrs_in;
  std::vector<AttachedAttribute> polattrs_out;
  if (get_rel_relkind(attachment.relid) == RELKIND_VIEW &&
      get_view_kind(attachment.relid) == OBJTYPE_LBV) {
    polattrs_in = DeserializeAttachedAttributes(attachment.polattrsin);
    polattrs_out = DeserializeAttachedAttributes(attachment.polattrsout);
  } else {
    polattrs_in = AttributesToColumnDefs(
        rel, DeserializeAttachedAttributes(attachment.polattrsin));
    polattrs_out = AttributesToColumnDefs(
        rel, DeserializeAttachedAttributes(attachment.polattrsout));
  }

  ParsedPolicyAttachment parsed_attach = {attachment, pol, polattrs_in,
                                          polattrs_out};

  XCHECK(
      parsed_attach.polattrs_out.size() == parsed_attach.pol.parsed_pol.size(),
      DDM_CALLER);

  return parsed_attach;
}

Node* SwapColRefMutator(Node* node, void* context) {
  if (!node) return nullptr;

  if (IsA(node, ColumnRef)) {
    auto ctx = reinterpret_cast<SwapColDefMutatorCtx*>(context);
    ColumnRef* cref = reinterpret_cast<ColumnRef*>(node);

    // We expect two kinds of column references: references to masking
    // expression arguments in the form of
    // MASK_PSEUDO_TBL.column_name[.nested.attributes] and in [DP-46149]
    // reference to lookup tables of the form
    // lookup_table_name.column_name[.nested.attributes].
    XPGCHECK(list_length(cref->fields) >= 2,
             policy::tag_for_policy_type(kPolicyDDM));

    if (strcmp(strVal(linitial(cref->fields)), MASK_PSEUDO_TBL) != 0) {
      // All references to masking policy arguments are rewritten as ColumnRef
      // with alias MASK_PSEUDO_TBL. Thus, if the reference is not in the form
      // of MASK_PSEUDO_TBL.column_name[.nested.attributes] , we do not swap
      // anything. The examples include lookup table references with or without
      // schema / database prefix.
      return node;
    }
    char* name = strVal(lsecond(cref->fields));

    const auto& swap_it = ctx->swap_map.find(std::string(name));

    if (swap_it != ctx->swap_map.end()) {
      // The reference is expected to be of the form
      // MASK_PSEUDO_TBL.column_name[.<any number of subfields>] . In this
      // case we replace "column_name", remove "MASK_PSEUDO_TBL", and leave the
      // rest (the SUPER navigation path) as is. Note, the fields are ordered by
      // db, schema, table, column, nested path.
      List* old_fields = cref->fields;
      ListCell* field = nullptr;
      // The first element is the new (swapped) reference.
      cref->fields =
          list_make1(makeString(pstrdup(swap_it->second.column_name.c_str())));
      // Then comes the optional input nested path.
      if (swap_it->second.nested_path) {
        RawPath path(swap_it->second.nested_path->data(),
                     swap_it->second.nested_path->size());
        for (auto&& component : path) {
          cref->fields =
              lappend(cref->fields,
                      makeString(pstrdup(std::string(component).c_str())));
        }
      }

      if (list_length(old_fields) > 2) {
        // Skip the first two elements: MASK_PSEUDO_TBL and original
        // column_name to swap.
        for_each_cell(field, list_nth_cell(old_fields, 2)) {
          cref->fields = lappend(
              cref->fields, makeString(pstrdup(strVal(
                                reinterpret_cast<Value*>(lfirst(field))))));
        }
      }

      list_free(old_fields);

      // Add cast around the ref for nested paths.
      // TODO(bogatov): Consider doing this for all policies including
      // column-level SUPER.
      if (swap_it->second.nested_path) {
        TypeCast* cast = makeNode(TypeCast);
        cast->arg = reinterpret_cast<Node*>(cref);
        cast->typname = makeTypeNameFromOid(swap_it->second.policy_arg_typid,
                                            swap_it->second.policy_arg_typmod);
        return reinterpret_cast<Node*>(cast);
      }
    }
  }
  return node;
}

namespace {
/// Utility method for building swap context used by SwapColRefMutator
/// takes as input the attachment for the policy.
SwapColDefMutatorCtx BuildSwapDefCtx(
    std::shared_ptr<ParsedPolicyAttachment> attach) {
  std::vector<AttachedAttribute>& input_atts = attach->polattrs_in;
  std::vector<std::shared_ptr<ColumnDef>>& pol_atts = attach->pol.parsed_attrs;
  XCHECK(input_atts.size() == pol_atts.size(),
         policy::tag_for_policy_type(kPolicyDDM));
  std::unordered_map<std::string, SwapColDefMutatorCtx::Entry> swap_map = {};

  for (size_t i = 0; i < input_atts.size(); i++) {
    std::string input_att_colname;
    AttachedAttribute::Attribute attribute = input_atts[i].attribute;

    if (std::holds_alternative<std::shared_ptr<ColumnDef>>(attribute)) {
      input_att_colname = std::string(
          (std::get<std::shared_ptr<ColumnDef>>(attribute))->colname);
    } else {
      XCHECK(std::holds_alternative<std::string>(attribute), DDM_CALLER);
      input_att_colname = std::get<std::string>(attribute);
    }
    swap_map.insert({std::string(pol_atts[i]->colname),
                     SwapColDefMutatorCtx::Entry{
                         input_att_colname, pol_atts[i]->typname->typid,
                         pol_atts[i]->typname->typmod, input_atts[i].path}});
  }
  return {swap_map};
}
}  // namespace

void LoadMaskMapFromAttachments(
    const std::set<ParsedPolicyAttachment>& attachments, MaskMap* masks) {
  for (const ParsedPolicyAttachment& attachment : attachments) {
    for (size_t i = 0; i < attachment.polattrs_out.size(); i++) {
      // Create merge entry to be inserted, emplaced or ignored.
      ResTarget* rtgt_raw = copyObject(
          reinterpret_cast<ResTarget*>(attachment.pol.parsed_pol[i].get()));
      MergeEntry entry{
          std::shared_ptr<ResTarget>(rtgt_raw, PAlloc_deleter()),
          attachment.attachment.polpriority,
          std::make_shared<ParsedPolicyAttachment>(attachment),
          {} /* lf_alp_column_name */,
          attachment.polattrs_out[i].path,
      };

      // Get the column key for the map (different logic for LBV and non-LBV).
      AttachedAttribute::Attribute attribute =
          attachment.polattrs_out[i].attribute;
      std::string col_key;
      if (std::holds_alternative<std::shared_ptr<ColumnDef>>(attribute)) {
        col_key = std::string(
            (std::get<std::shared_ptr<ColumnDef>>(attribute))->colname);
      } else {
        XCHECK(std::holds_alternative<std::string>(attribute),
               policy::tag_for_policy_type(kPolicyDDM));
        col_key = std::get<std::string>(attribute);
      }

      // The logic of handling of nested paths is roughly the following:
      // 1. If no entries exist for the column, add this one and exit.
      // 2. If this entry is a column attachment, check the highest priority
      // among all entries for the column (naturally, either one other
      // column-level attachment or possibly multiple nested attachments). If
      // new entry is still of the highest priority, replace all existing
      // entries with the new one.
      // 3. At this point we know the entries exist for the column and our entry
      // is nested. So we go over these existing entries:
      // 3(a). If we see an existing entry that is column-level, we either
      // override it (we know and XCHECK it's a sole entry), or it wins and we
      // drop our entry.
      // 3(b). We know an existing entry is nested, so we check if our entry
      // collides with that one. If not, we go to the next entry (i.e., this
      // existing entry does not bother us).
      // 3(c). So our entry and existing one collide. We check if our entry is
      // higher, and if so we remember to delete this existing lower-priority
      // entry. If our entry is lower, we exit immediately dropping our entry.
      // 3(d). Lastly, we know our entry is to be inserted, and it needs to
      // override zero, one or more existing nested entries. So we remove those
      // very existing entries and insert the new one.

      const auto& target_it = masks->find(col_key);
      if (target_it == masks->end()) {
        // Insert or update the entry for the column.
        (*masks)[col_key] = {entry};
        continue;
      }

      if (!entry.nested_path) {
        // If this entry has no nested path then it will or will not override
        // the exiting entry based solely on the highest priority of the
        // existing entries. In other words, column-level attachment overrides
        // all nested attachments.
        int4 col_key_priority =
            std::max_element(target_it->second.begin(), target_it->second.end())
                ->priority;
        // If existing entry is of the same or higher priority, don't insert.
        if (col_key_priority >= entry.priority) continue;
        (*masks)[col_key] = {entry};
        continue;
      }

      // In case of nested we need to check deeper if this attachment can
      // override an existing one (based among other things on whether there
      // is conflicting path).
      RawPath path(entry.nested_path->data(), entry.nested_path->size());
      bool inserted_or_ignored = false;
      std::vector<std::string> to_remove;
      for (auto existing_entry_it = target_it->second.begin();
           existing_entry_it != target_it->second.end(); existing_entry_it++) {
        // Short-circuit if existing entry is column-level attachment.
        if (!existing_entry_it->nested_path) {
          // If current entry is for a column-level attachment, our new
          // nested-level attachment will either override it or get ignored
          // because these two cannot co-exist for same column.
          XCHECK(target_it->second.size() == 1);
          inserted_or_ignored = true;
          if (existing_entry_it->priority >= entry.priority) break;
          (*masks)[col_key] = {entry};
          break;
        }

        RawPath existing_path(existing_entry_it->nested_path->data(),
                              existing_entry_it->nested_path->size());
        // If these paths don't conflict, we don't need to check priorities.
        // We simply move on to rest of nested-attachments.
        if (RawPath::Diverge(path, existing_path)) continue;

        // If these paths do collide, then we either ignore our entry or collect
        // exiting entries to remove. In this case below, if there is a
        // conflicting path with higher priority, we know we cannot insert our
        // entry.
        if (existing_entry_it->priority >= entry.priority) {
          inserted_or_ignored = true;
          break;
        }
        // We have determined that our entry's nested path conflicts with this
        // existing entry's nested path, and our priority is higher, so we mark
        // the existing entry for removal.
        to_remove.push_back(*existing_entry_it->nested_path);
      }
      // Finally, if we have determined that our entry is eligible for
      // insertion, we remove those marked for removal and insert ours.
      if (!inserted_or_ignored) {
        // Vector resizes after each erase. It's safer to simply remember the
        // paths to erase than iterators or indices that may change.
        for (auto&& to_remove_path : to_remove) {
          target_it->second.erase(
              std::remove_if(target_it->second.begin(), target_it->second.end(),
                             [&to_remove_path](const MergeEntry& e) {
                               return e.nested_path == to_remove_path;
                             }),
              target_it->second.end());
        }
        target_it->second.push_back(entry);
      }
    }
  }
}

MaskMap BuildMaskMap(const std::set<ParsedPolicyAttachment>& attachments,
                     const ObjTransformAlpColumnsCtx& obj_transform_alp_ctx) {
  MaskMap masks;

  LoadMaskMapFromAttachments(attachments, &masks);

  // lf_alp_policy_opt that is std::nullopt is a valid state. This naturally
  // occurs if:
  // a) RTE is DDM-protected but not LF-ALP-protected.
  // b) In the case of Spectrum nested relations, that neither are enforced at
  //    DDM level.
  // these two cases are mutually exclusive, but we add this code in a way to
  // support these cases concurrently in the future.
  if (obj_transform_alp_ctx.lf_alp_policy) {
    AddLakeFormationALPToDDMMap(
        &masks, *obj_transform_alp_ctx.lf_alp_policy,
        obj_transform_alp_ctx.obj_transform_alp_columns);
  }

  // Loop through map and swap policy definitions.
  // We are guaranteed that only one policy owns any ResTarget.
  // All inputs to masking policies should be unmasked.
  for (auto& entries : masks) {
    for (MergeEntry& entry : entries.second) {
      SwapColDefMutatorCtx ctx =
          entry.attachment
              ? BuildSwapDefCtx(entry.attachment)
              // LF-ALP SwapColRefMutator is a no-op.
              : SwapColDefMutatorCtx{{{entry.lf_alp_column_name,
                                       SwapColDefMutatorCtx::Entry{
                                           entry.lf_alp_column_name,
                                           InvalidOid /* policy_arg_typid */,
                                           0 /* policy_arg_typmod */,
                                           std::nullopt /* nested path */}}}};
      entry.rtgt->val = parse_tree_mutator(
          reinterpret_cast<Node*>(entry.rtgt->val), SwapColRefMutator, &ctx);
    }
  }
  return masks;
}

namespace {
/// Get actual column names for the given RTE.
/// @param RangeTblEntry* rte
/// @return std::vector of std::string containing the column names.
std::vector<std::string> GetActualColumnNames(const RangeTblEntry* rte) {
  std::vector<std::string> colnames;

  if (rte->actual_colnames == nullptr) {
    // Note NoLock here because we only need column names here.
    Relation relation = relation_open(GetRelationRelid(rte), NoLock);
    SCOPE_EXIT_GUARD(relation_close(relation, NoLock));
    Copied_pg_attribute* relation_attributes = relation->rd_att->attrs;
    int maxattrs = relation->rd_att->natts;
    for (int varattno = 0; varattno < maxattrs; varattno++) {
      Immutable_pg_attribute attr = relation_attributes[varattno];
      if (attr->attisdropped) {
        colnames.push_back("");
      } else {
        colnames.push_back(std::string(NameStr(attr->attname)));
      }
    }
  } else {
    // Actual column names are already filled in.
    ListCell* lc = nullptr;
    foreach(lc, rte->actual_colnames) {
      char* name = reinterpret_cast<char*>(strVal(lfirst(lc)));
      XCHECK(name != nullptr, DDM_CALLER);
      colnames.push_back(std::string(name));
    }
  }

  return colnames;
}
}  // namespace

std::string GetRTEQueryTargetName(const RangeTblEntry* rte) {
  std::stringstream target;
  char* ext_db_name = nullptr;
  char* ext_ns_name = nullptr;
  char* ext_rel_name = nullptr;
  if (gconf_enable_lf_alp &&
      IsLfManagedRelation(rte, &ext_db_name, &ext_ns_name, &ext_rel_name)) {
    if (g_pgtest_ddm) {
      // TODO(bogatov): PG test has a weird behavior regarding schema lookup. In
      // particular, none of the schema names we tried, including
      // "pgtest_testcase_schema" works. So for unit tests we simply supply
      // relation name and it will find it.
      target << quote_identifier(ext_rel_name);
    } else if (rte->nested_subquery) {
      // This relation refers to an external nested table. In this case, we
      // access the namespace and relation name from the main RTE directly,
      // as opposed to the isolated query.
      if (IsExternalDbRTE(rte)) {
        // Fully qualify tables with external dbs set.
        target << quote_identifier(rte->database) << '.';
      }
      const char* external_tablename = rte->external_tablename;
      if (is_RTE_ds_spectrum_table_via_lf(rte)) {
        external_tablename = rte->datasharing_lf_spectrum_tablename;
      }

      target << quote_identifier(rte->external_namespace) << '.'
             << quote_identifier(external_tablename);
    } else {
      // LF-managed localized relation.
      XCHECK(ext_db_name != nullptr, LF_ALP_CALLER);
      XCHECK(ext_ns_name != nullptr, LF_ALP_CALLER);
      XCHECK(ext_rel_name != nullptr, LF_ALP_CALLER);
      target << quote_identifier(ext_db_name) << '.'
             << quote_identifier(ext_ns_name) << '.'
             << quote_identifier(ext_rel_name);
    }
  } else {
    char* ns_name =
        get_namespace_name(get_rel_namespace(GetRelationRelid(rte)));
    XCHECK(ns_name != nullptr, DDM_CALLER);
    char* rel_name = get_rel_name(GetRelationRelid(rte));
    XCHECK(rel_name != nullptr, DDM_CALLER);
    target << quote_identifier(ns_name) << '.' << quote_identifier(rel_name);
  }
  return target.str();
}

/// Construct a select query for the given rte. Resulting query is of the form
/// `select col1, col2, col3... from table_name`
/// \param RangeTblEntry* rte
/// \param colnames optional std::vector containing std::string of column names.
/// colnames should have entries when the relation type of the rte is lbv. For
/// lbv, column names cannot be found in pg_attribute.
/// \return SelectStmt*
SelectStmt* BuildQueryForRTE(const RangeTblEntry* rte,
                             std::optional<std::vector<std::string>> colnames) {
  std::string column_names_concat;
  bool first_col = true;

  if (!colnames) colnames = GetActualColumnNames(rte);
  XCHECK(colnames && colnames->size() != 0, DDM_CALLER);

  for (std::string& colname : *colnames) {
    XCHECK(colname.size() > 0, DDM_CALLER);
    if (first_col) {
      first_col = false;
    } else {
      column_names_concat += ", ";
    }
    column_names_concat += quote_identifier(colname.c_str());
  }

  XCHECK(column_names_concat.size() != 0, DDM_CALLER);

  std::string target = GetRTEQueryTargetName(rte);

  std::string sql = "SELECT " + column_names_concat + " FROM " + target;

  return reinterpret_cast<SelectStmt*>(
      linitial(ParseQuery(sql.c_str(), LOG_TAG_DDM)));
}

namespace {
/// Iterates through the provided SelectStmt's ResTargets and replaces any
/// matched columns with the Masking expression in the masks map.
SelectStmt* MaskColumns(
    SelectStmt* select_stmt, const MaskMap& masks,
    const std::optional<NestedVarNames>& obj_transform_vars_opt) {
  ListCell* lc = nullptr;
  ResTarget* rtgt = nullptr;
  ResTarget* masked = nullptr;
  std::string col;

  foreach(lc, select_stmt->targetList) {
    Node* node = reinterpret_cast<Node*>(lfirst(lc));
    XCHECK(IsA(node, ResTarget), DDM_CALLER);
    rtgt = reinterpret_cast<ResTarget*>(node);
    XCHECK(IsA(rtgt->val, ColumnRef), DDM_CALLER);
    ColumnRef* cref = reinterpret_cast<ColumnRef*>(rtgt->val);
    XCHECK(list_length(cref->fields) == 1, DDM_CALLER);
    col = std::string(strVal(reinterpret_cast<Value*>(linitial(cref->fields))));

    const auto& mask_it = masks.find(col);
    if (mask_it == masks.end()) continue;

    if (mask_it->second.size() == 1 && !mask_it->second[0].nested_path) {
      // This block is for column-level attachments only.
      masked = copyObject(mask_it->second[0].rtgt.get());
    } else {
      // This block is for nested path attachments only.
      if (obj_transform_vars_opt &&
          obj_transform_vars_opt->find(col) == obj_transform_vars_opt->end()) {
        // Optimization: if walker did not find any reference for the `col`
        // that must result in OBJECT_TRANSFORM, then don't use it. Instead, the
        // mask will be applied on variable level.
        break;
      }

      masked = makeNode(ResTarget);
      masked->val =
          reinterpret_cast<Node*>(MakeObjectTransform(mask_it->second, col));
    }
    masked->name = pstrdup(col.c_str());
    void* old_val = replace_pointer(lc, masked);
    safe_pfree(old_val);
  }

  return select_stmt;
}
}  // namespace

std::tuple<std::shared_ptr<Alias>, std::vector<std::shared_ptr<Node>>,
           std::shared_ptr<List>>
AnalyzePolicyStatement(
    const std::vector<std::shared_ptr<ColumnDef>>& column_definitions,
    const std::vector<std::shared_ptr<Node>>& masking_expressions) {
  ParseState* pstate = make_parsestate(NULL);
  // This is to ensure that when subqueries inside expressions are processed,
  // the parse state remembers that it is a DDM expression and correctly
  // validates that the references are allowed (like to the system catalogs).
  pstate->is_parsing_ddm_policy = true;

  std::shared_ptr<Query> subquery;
  std::shared_ptr<Alias> alias;
  std::tie(subquery, alias) = BuildSubqueryForAnalysis(column_definitions);

  // Add the subquery to the parse state's FROM clause.
  int rtindex = addRangeTableEntryForSubquery(pstate, subquery.get(),
                                              alias.get(), /*inFromCl=*/true)
                    .second;
  addRTEtoQuery(pstate, rtindex, /*addToJoinList*/ true,
                /*addToNameSpace*/ true);

  std::vector<std::shared_ptr<Node>> transformed_expressions;

  // Now that the parse state represents correct query, run transformation
  // through all expressions.
  for (std::shared_ptr<Node> expression : masking_expressions) {
    std::shared_ptr<Node> transformed_expression(
        transformExpr(pstate, copyObject(expression.get())), PAlloc_deleter());

    transformed_expressions.push_back(transformed_expression);
  }

  std::shared_ptr<List> range_table(pstate->p_rtable, PAlloc_deleter());

  return {alias, transformed_expressions, range_table};
}

std::pair<std::shared_ptr<Query>, std::shared_ptr<Alias>>
BuildSubqueryForAnalysis(
    const std::vector<std::shared_ptr<ColumnDef>>& column_definitions) {
  std::shared_ptr<Query> subquery(makeNode(Query), PAlloc_deleter());
  subquery->commandType = CMD_SELECT;
  subquery->canSetTag = false;
  RangeTblRef* rtr = makeRangeTblRef(list_length(subquery->rtable));
  subquery->jointree = makeFromExpr(list_make1(rtr), nullptr);

  List* column_names = NIL;

  // Add each policy column to the query's target list and to the list of column
  // names for alias.
  AttrNumber varattno = 0;
  for (std::shared_ptr<ColumnDef> column_definition : column_definitions) {
    XCHECK(column_definition);
    char* column_name = reinterpret_cast<char*>(column_definition->colname);
    TypeName* type_name =
        reinterpret_cast<TypeName*>(column_definition->typname);
    Oid type_oid = typenameTypeId(type_name);

    Var* var = makeNode(Var);
    var->varno = 1;
    var->varattno = ++varattno;
    var->vartype = type_oid;
    // Check if type_oid is VARCHAR and tymod is -1.
    // Set typmod to 260 as default value.
    if (type_oid == VARCHAROID && type_name->typmod == -1) {
      type_name->typmod = defaultTypmod;
    }
    var->vartypmod = type_name->typmod;
    var->varlevelsup = 0;
    Resdom* result_domain =
        makeResdom(var->varattno, var->vartype, var->vartypmod, column_name,
                   false /* resjunk */);
    TargetEntry* tle =
        makeTargetEntry(result_domain, reinterpret_cast<Expr*>(var));
    subquery->targetList = lappend(subquery->targetList, tle);

    column_names = lappend(column_names, makeString(pstrdup(column_name)));
  }

  // Use the column names from DDM policy's WITH clause.
  std::shared_ptr<Alias> alias(makeAlias(ddm::MASK_PSEUDO_TBL, column_names),
                               PAlloc_deleter());

  return {subquery, alias};
}

std::shared_ptr<Query> BuildQueryForDeparsing(
    std::shared_ptr<Alias> alias, std::shared_ptr<Node> expression) {
  std::string dummy = ddm::DDM_DUMMY_NAME;

  std::shared_ptr<Query> query(makeNode(Query), PAlloc_deleter());
  query->commandType = CMD_SELECT;
  RangeTblEntry* rte = makeNode(RangeTblEntry);
  rte->rtekind = RTE_RELATION;
  rte->eref = alias.get();
  rte->inh = false;
  rte->inFromCl = true;
  rte->external_table_id = -1;
  rte->file_registry_id = -1;
  // Here we set S3 table name so that the FROM clause looks to PG like a valid
  // external relation. A non-null s3_table_location means to the planner that
  // the table is external and therefore the planner will not try to validate it
  // until the runtime. This is what we need here, because there is no table
  // for this query, we only need the SELECT and ALIAS parts.
  rte->s3_table_location = pstrdup(dummy.c_str());
  rte->external_namespace = pstrdup(dummy.c_str());
  rte->external_tablename = pstrdup(dummy.c_str());
  query->rtable = lappend(query->rtable, rte);

  // Attach the expression itself.
  RangeTblRef* rtr = makeRangeTblRef(list_length(query->rtable));
  query->jointree = makeFromExpr(list_make1(rtr), nullptr);

  // Resdom here contains correct expression return type.
  Resdom* resdom =
      makeResdom(1, exprType(expression.get()), exprTypmod(expression.get()),
                 pstrdup(dummy.c_str()), false /* resjunk */);

  TargetEntry* tle = makeTargetEntry(
      resdom, std::reinterpret_pointer_cast<Expr>(expression).get());
  query->targetList = lappend(query->targetList, tle);

  return query;
}

void VerifyObjectPermissions(Oid polid) {
  if (!gconf_ddm_enable_perm_check) return;

  char* pol_att_str = nullptr;
  char* pol_expr = nullptr;
  GetPolicyAttrsAndExpr(polid, &pol_att_str, &pol_expr);

  std::vector<std::shared_ptr<ColumnDef>> parsed_attrs =
      ddm::DeserializeColumnDefs(pol_att_str);

  const std::vector<std::shared_ptr<ResTarget>>& parsed_exprs =
      ParsePolExpr(pol_expr);

  std::vector<std::shared_ptr<Node>> masking_expressions;
  for (auto tar : parsed_exprs) {
    Node* res_tar = copyObject(reinterpret_cast<Node*>(tar.get()->val));
    std::shared_ptr<Node> s_ptr(res_tar, PAlloc_deleter());
    masking_expressions.push_back(s_ptr);
  }

  std::shared_ptr<Alias> alias;
  std::vector<std::shared_ptr<Node>> expressions;
  std::shared_ptr<List> range_table;
  std::tie(alias, expressions, range_table) =
      ddm::AnalyzePolicyStatement(parsed_attrs, masking_expressions);

  // Convert transformed expressions to ParsedPolicyExpression, one by one.
  for (auto&& expression : expressions) {
    auto query = ddm::BuildQueryForDeparsing(alias, expression);
    policy::PolicyPermCheckContext perm_ctx = {
        polid, PolicyType::kPolicyDDM /* poltype */};
    CheckPermissionForPolicy(reinterpret_cast<Node*>(query.get()->targetList),
                             &perm_ctx);
  }
}

void DisableDdmOnFgacTarget(Query* q, const RangeTblEntry* rte) {
  XCHECK(q->jointree != nullptr, DDM_CALLER);
  XCHECK(q->jointree->fromlist != nullptr, DDM_CALLER);
  XCHECK_EQ(list_length(q->rtable), 1, DDM_CALLER);
  RangeTblEntry* fgac_target_rte =
      reinterpret_cast<RangeTblEntry*>(linitial(q->rtable));

  // THIS IS AN LF RTE, we check correct target based on name.
  char* rte_db_name = nullptr;
  char* rte_ns_name = nullptr;
  char* rte_rel_name = nullptr;
  const bool is_rel_via_lf =
      IsLfManagedRelation(rte, &rte_db_name, &rte_ns_name, &rte_rel_name);
  if (is_rel_via_lf) {
    char* fgac_db_name = nullptr;
    char* fgac_ns_name = nullptr;
    char* fgac_rel_name = nullptr;
    const bool is_fgac_rel_via_lf =
        IsLfManagedRelation(rte, &fgac_db_name, &fgac_ns_name, &fgac_rel_name);
    XCHECK(is_fgac_rel_via_lf, DDM_CALLER);
    XCHECK(rte_db_name != nullptr && fgac_db_name != nullptr &&
               strncmp(rte_db_name, fgac_db_name, NAMEDATALEN) == 0,
           DDM_CALLER);
    XCHECK(rte_ns_name != nullptr && fgac_ns_name != nullptr &&
               strncmp(rte_ns_name, fgac_ns_name, NAMEDATALEN) == 0,
           DDM_CALLER);
    XCHECK(rte_rel_name != nullptr && fgac_rel_name != nullptr &&
               strncmp(rte_rel_name, fgac_rel_name, NAMEDATALEN) == 0,
           DDM_CALLER);
  } else {
    // THIS IS A LOCAL RTE, WE CHECK TARGET BASED ON OID
    // Target can be a RTE_[RLS]_RELATION: table, regular view, or MV
    //            or a RTE_[RLS]_SUBQUERY: late binding view.
    XCHECK(fgac_target_rte->rtekind == RTE_RELATION ||
               fgac_target_rte->rtekind == RTE_SUBQUERY ||
               IsRlsRte(fgac_target_rte),
           DDM_CALLER);
    XCHECK(GetRelationRelid(fgac_target_rte) == GetRelationRelid(rte),
           DDM_CALLER);
  }

  fgac_target_rte->has_ddm_rewrite = false;
}

Query* MergePolsForRTE(const RangeTblEntry* rte, Query* top_query,
                       const std::set<ParsedPolicyAttachment>& attachments,
                       const NestedVarMap& ddm_obj_transform_vars,
                       const ObjTransformAlpColumnsCtx& obj_transform_alp_ctx) {
  const bool has_ddm_attachments = !attachments.empty();
  const bool has_lf_alp_policies =
      gconf_enable_lf_alp && obj_transform_alp_ctx.lf_alp_policy;

  const bool is_spectrum_relation =
      is_RTE_s3table(rte) || IsSpectrumNested(rte);  // See [DP-61135].
  const bool is_under_seclog = seclog::HasAttachment(GetRelationRelid(rte));
  // It is possible that ALP paths are not treated as PartiQL for Spectrum
  // nested tables. This is only known after walking the query tree's target
  // list entries in the ALP walkers. Hence, `has_lf_alp_policies` can return
  // false, if ALP policies are on top of flattened JOINed scalar attributes of
  // the Spectrum NestedRewriter. For these, the baseline CLP engine already
  // suffices. This is also the case for count(*), where we do not have an
  // explicit reference to columns. Subsequently, the ALP walker does not
  // capture and cache the ALP policies.
  if (gconf_enable_lf_alp && !has_lf_alp_policies && is_spectrum_relation &&
      !is_under_seclog) {
    // RTEs masked with SecLog are allowed to be Spectrum (case of STCS).
    XCHECK(gconf_spectrum_enable_lf_data_filters);
    // In case ALP policies are not present in a Spectrum relation, we still
    // enforce that the relation does not contain any DDM attachmenet, as these
    // are not supported on Spectrum tables.
    XCHECK(!has_ddm_attachments, "DDM over an external relation");
  } else {
    // One and only one type of attachments must be non-empty until we support
    // LF and native DDM over on relation.
    XCHECK(has_ddm_attachments ^ has_lf_alp_policies,
           "LF and DDM over on relation");
  }

  MaskMap masks = BuildMaskMap(attachments, obj_transform_alp_ctx);

  SelectStmt* select_q;
  Oid rte_relid = GetRelationRelid(rte);
  std::optional<std::vector<std::string>> lbv_colnames;
  List* ddm_reparse_lbvs = NIL;
  if (IsLbv(rte_relid)) {
    char* rel_name = get_rel_name(rte_relid);
    std::unordered_map<std::string, std::shared_ptr<ColumnDef>>
        parsed_lbv_columndefs;
    lbv_colnames = ParseLbvColumns(rte, parsed_lbv_columndefs);

    for (auto attachment : attachments) {
      CheckAttachmentForColumnsType(attachment, parsed_lbv_columndefs,
                                    rel_name);
    }

    if (gconf_ddm_enable_lbv_caching) {
      // Cache the LBV so it will be reused when parsing the DDM policy. LBVs
      // can reference data lake objects which are expensive to parse. Also, the
      // DDM rewriter will be called on each EXECUTE and there are implicit
      // assumptions that EXECUTE will not create localization objects (which
      // would happen if the DDM rewriter were to parse the LBV).
      //
      // WARNING: disabling GUC gconf_ddm_enable_lbv_caching will result in
      // query ERROR for DDM-protected LBVs that reference data sharing or
      // external relations.
      ddm_reparse_lbvs =
          list_make1(copyObject(const_cast<RangeTblEntry*>(rte)));
    }
  }
  select_q = BuildQueryForRTE(rte, lbv_colnames);

  for (auto& entries : masks) {
    // Do only for DDM attachments (not LF-ALP).
    for (MergeEntry& entry : entries.second) {
      if (entry.attachment) {
        // SecLog policies are maintained by Redshift.
        if (entry.attachment->pol.pol.is_seclog) continue;
        Oid polid = entry.attachment->pol.pol.polid;
        XCHECK(polid > 0, policy::tag_for_policy_type(kPolicyDDM));
        // Verify permissions for all objects in the policy.
        ddm::VerifyObjectPermissions(polid);
      }
    }
  }

  // If SQL command type is not SELECT, we leave opt as unset, which will cause
  // MaskColumns to OBJECT_TRANSFORM all columns. If rte_relid is not found, set
  // opt to empty set, meaning none of the columns will be OBJECT_TRANSFORM'd.
  // Else pass the set of columns to OBJECT_TRANSFORM.
  std::optional<NestedVarNames> obj_transform_vars_opt = std::nullopt;
  if (top_query->commandType != CMD_UPDATE &&
      top_query->commandType != CMD_DELETE) {
    auto obj_transform_vars_it = ddm_obj_transform_vars.find(rte_relid);
    obj_transform_vars_opt =
        obj_transform_vars_it != ddm_obj_transform_vars.end()
            ? obj_transform_vars_it->second
            : NestedVarNames();
  }
  select_q = MaskColumns(select_q, masks, obj_transform_vars_opt);

  XCHECK(top_query != nullptr);
  List* external_rtables =
      has_lf_alp_policies ? CollectDataSharingExternalRTEs(top_query) : NIL;
  if (is_external_table(rte)) {
    external_rtables =
        list_concat(external_rtables, CollectExternalTableRTEs(top_query));
  }

  List* parsed = parse_analyze_int_rw(
      reinterpret_cast<Node*>(select_q), nullptr /* paramTypes */,
      0 /* numParams */, ParseTypeDDM, true /* post_ds_perm_checks */,
      external_rtables /* external_rtables */,
      false /* is_prepared_statement */,
      has_lf_alp_policies /* allow_localized_temps */, NIL /* active_lbvs */,
      false /* only_metadata */, InvalidAclId /* lbv_owner */,
      InvalidOid /* skip_rls_relid */, ddm_reparse_lbvs,
      nullptr /* bastion_external_schema */);

  if (gconf_ddm_enable_perm_check) {
    // Disable Permsissions on the unfolded DDM SubQuery.
    // Permissions are already verified in ddm::VerifyObjectPermissions.
    DisableRTEPermCheck(parsed);
  }

  XCHECK_EQ(list_length(parsed), 1, DDM_CALLER);
  Query* q = reinterpret_cast<Query*>(linitial(parsed));
  DisableDdmOnFgacTarget(q, rte);
  XCHECK(q->jointree != nullptr, DDM_CALLER);
  XCHECK(q->jointree->fromlist != nullptr, DDM_CALLER);
  XCHECK_EQ(list_length(q->rtable), 1, DDM_CALLER);

  auto rewritten_rte = reinterpret_cast<RangeTblEntry*>(linitial(q->rtable));
  rewritten_rte->has_ddm_rewrite = false;

  if (gconf_enable_session_context_variable) {
    return convert_current_setting_func_to_const(q, NULL /* no param_list */,
                                                 false /* no folded view*/);
  }

  return q;
}

std::string FetchStringFromText(SysCacheIdentifier cache, HeapTuple tup,
                                int attnum, bool fail_on_null) {
  bool is_null = true;
  Datum text_datum = SysCacheGetAttr(cache, tup, attnum, &is_null);
  if (fail_on_null) {
    XCHECK(!is_null, DDM_CALLER);
  }
  char* toasted =
      pstrdup(DatumGetCString(DirectFunctionCall1(textout, text_datum)));

  return std::string(toasted);
}

Attachment AttachmentFromTuple(HeapTuple attachment_tuple) {
  XCHECK(HeapTupleIsValid(attachment_tuple), DDM_CALLER);

  Immutable_pg_permission_mask mask =
      reinterpret_cast<Immutable_pg_permission_mask>(
          GETSTRUCT(attachment_tuple));

  std::string attrs_out =
      FetchStringFromText(PERMISSIONMASKRELPRIORITY, attachment_tuple,
                          Anum_pg_permission_mask_polattrsout, true);
  std::string attrs_in =
      FetchStringFromText(PERMISSIONMASKRELPRIORITY, attachment_tuple,
                          Anum_pg_permission_mask_polattrsin, true);

  return Attachment{mask->polid,       mask->permid,        mask->relid,
                    mask->polpriority, mask->polmodifiedby, attrs_in,
                    attrs_out};
}

Policy PolicyFromTuple(HeapTuple pol_tuple) {
  XCHECK(HeapTupleIsValid(pol_tuple), DDM_CALLER);
  Immutable_pg_policy_mask pol =
      reinterpret_cast<Immutable_pg_policy_mask>(GETSTRUCT(pol_tuple));

  return Policy{pol->polid,
                pol->poldbid,
                false /* maskoningest */,
                pol->polname,
                pol->polmodifiedby,
                FetchStringFromText(POLICYMASKID, pol_tuple,
                                    Anum_pg_policy_mask_polattrs, true),
                FetchStringFromText(POLICYMASKID, pol_tuple,
                                    Anum_pg_policy_mask_polexpr, true)};
}

bool HasAttachmentsForRel(const Oid relid) {
  // First check seclog attachments. They are precompiled.
  if (seclog::HasAttachment(relid)) return true;

  CatCList* catlist = SearchSysCacheExtendedList(PERMISSIONMASKRELPRIORITY, 1,
                                                 relid, 0, 0, 0, 0, 0);
  XCHECK(catlist != nullptr, DDM_CALLER);

  bool has_attachments = false;
  for (int i = 0; i < catlist->n_members; ++i) {
    HeapTuple tup = &catlist->members[i]->tuple;
    if (!HeapTupleIsValid(tup)) {
      elog(ERROR, "could not find tuple for relation %u", relid);
    }
    has_attachments = true;
    break;
  }
  ReleaseSysCacheList(catlist);
  return has_attachments;
}

bool HasAttachmentsForPolicy(const Oid policy_oid) {
  Relation pg_permission_mask =
      heap_openr(PermissionMaskRelationName, AccessShareLock);
  SCOPE_EXIT_GUARD(heap_close(pg_permission_mask, AccessShareLock));

  HeapScanDesc scan_description =
      heap_beginscan(pg_permission_mask, SnapshotNow, 0, nullptr);
  SCOPE_EXIT_GUARD(heap_endscan(scan_description));

  HeapTuple tuple;

  while (HeapTupleIsValid(
      tuple = heap_getnext(scan_description, ForwardScanDirection))) {
    Immutable_pg_permission_mask permission_form =
        reinterpret_cast<Immutable_pg_permission_mask>(GETSTRUCT(tuple));

    if (permission_form->polid == policy_oid) {
      return true;
    }
  }
  return false;
}

std::set<Attachment> FetchAttachmentsForRel(const Oid relid,
                                            const std::optional<AclId> userid,
                                            const std::optional<AclId> roleid) {
  // First check seclog attachments. They are precompiled.
  std::optional<Attachment> seclog_attachment = seclog::GetAttachment(relid);
  if (seclog_attachment) return {*seclog_attachment};

  PGCHECK((userid.has_value()) ^ (roleid.has_value()), "%s",
          policy::tag_for_policy_type(kPolicyDDM).c_str());

  CatCList* catlist = SearchSysCacheExtendedList(PERMISSIONMASKRELPRIORITY, 1,
                                                 relid, 0, 0, 0, 0, 0);
  XCHECK(catlist != nullptr, DDM_CALLER);
  SCOPE_EXIT_GUARD(ReleaseSysCacheList(catlist));

  std::set<Attachment> attachments;

  for (int i = 0; i < catlist->n_members; ++i) {
    HeapTuple tup = &catlist->members[i]->tuple;
    if (!HeapTupleIsValid(tup)) {
      elog(ERROR, "could not find tuple for relation %u", relid);
    }

    auto attach = AttachmentFromTuple(tup);
    CheckAttachmentSupported(attach);

    AclId public_id = InvalidAclId;
    bool check_public_priv_dsw =
        check_default_privilege_on_public(RelOid_pg_class, relid);
    bool attached_to_public =
        CheckOnePerm(public_id, attach.permid, check_public_priv_dsw);

    if (!attached_to_public && userid.has_value() &&
        !CheckOnePerm(userid.value(), attach.permid, check_public_priv_dsw)) {
      continue;
    }

    if (!attached_to_public && roleid.has_value() &&
        !CheckOneRolePerm(roleid.value(), attach.permid)) {
      continue;
    }

    attach.pol = FetchPolicy(attach.polid);
    attachments.insert(attach);
  }

  return attachments;
}

Policy FetchPolicy(const Oid polid) {
  HeapTuple tup =
      SearchSysCache(POLICYMASKID, ObjectIdGetDatum(polid), 0, 0, 0);
  SCOPE_EXIT_GUARD(ReleaseSysCache(tup));

  if (!HeapTupleIsValid(tup)) {
    // foreign key violation
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    "could not find tuple for policy %u", polid));
  }
  return PolicyFromTuple(tup);
}

std::vector<int> ColumnNamesToAttributeNumbers(
    Oid rel_id, const std::vector<std::shared_ptr<Value>>& column_names) {
  std::vector<int> attribute_numbers;
  attribute_numbers.reserve(column_names.size());

  for (auto&& column_name : column_names) {
    const char* column_name_cstr = strVal(column_name.get());
    HeapTuple tuple = SearchSysCacheAttName(rel_id, column_name_cstr);
    if (!HeapTupleIsValid(tuple))
      ereport(ERROR,
              (errcode(ERRCODE_UNDEFINED_COLUMN),
               errmsg("column \"%s\" does not exist", column_name_cstr)));
    SCOPE_EXIT_GUARD(ReleaseSysCache(tuple));
    int attribute_number = ((Immutable_pg_attribute)GETSTRUCT(tuple))->attnum;
    XCHECK(attribute_number > 0, DDM_CALLER);

    attribute_numbers.push_back(attribute_number);
  }

  return attribute_numbers;
}

std::vector<AttachedAttribute> AttributesToColumnDefs(
    const Relation& relation,
    const std::vector<AttachedAttribute>& attributes) {
  std::vector<AttachedAttribute> column_definitions;
  column_definitions.reserve(attributes.size());

  for (AttachedAttribute attached_attribute : attributes) {
    XPGCHECK(std::holds_alternative<int>(attached_attribute.attribute));

    int attribute_number = std::get<int>(attached_attribute.attribute);

    XPGCHECK(attribute_number > 0);
    // Convert from cardinal to ordinal scale.
    attribute_number -= 1;
    Copied_pg_attribute* relatts = relation->rd_att->attrs;
    int n_atts = relation->rd_rel->relnatts;
    XPGCHECK(attribute_number < n_atts);

    std::shared_ptr<ColumnDef> column_definition(
        makeColumnDef(relatts[attribute_number]->attname.data,
                      relatts[attribute_number]->atttypid,
                      relatts[attribute_number]->atttypmod),
        PAlloc_deleter());

    column_definitions.push_back({column_definition, attached_attribute.path});
  }

  return column_definitions;
}

std::pair<attribute_info_t, bool> ColumnNamesToAttachedAttributes(
    Oid rel_id, List* colnames) {
  ddm::attribute_info_t attached_attributes;
  bool has_path = false;

  ListCell* lc = nullptr;
  foreach(lc, colnames) {
    List* column_components = reinterpret_cast<List*>(lfirst(lc));
    attached_attributes.push_back(ColumnComponentsToAttachedAttribute(
        column_components, rel_id, &has_path));
  }

  return {attached_attributes, has_path};
}

ddm::AttachedAttribute ColumnComponentsToAttachedAttribute(
    List* column_components, Oid rel_id, bool* has_path) {
  XCHECK(list_length(column_components) > 0, DDM_CALLER);
  std::string colname_str;
  std::optional<std::string> path;
  std::stringstream ss_path;
  int nested_depth = 0;

  ListCell* lc = nullptr;
  foreach(lc, column_components) {
    if (colname_str.empty()) {
      // First entry in the column components is the column name.
      colname_str = std::string(strVal(lfirst(lc)));
      continue;
    }

    *has_path = true;
    ++nested_depth;
    if (nested_depth > MAX_PARTIQL_NESTING_LEVELS) {
      ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("SUPER path exceeds maximum nesting levels of %d",
                             MAX_PARTIQL_NESTING_LEVELS)));
    }

    AddComponentToPath(ss_path, std::string(strVal(lfirst(lc))));
  }

  if (ss_path.tellp() != std::streampos(0)) path = ss_path.str();

  if (IsLbv(rel_id)) return {colname_str, path};

  // For relations other than LBV (table / regular view), we need to get
  // attribute number for the column name.
  const char* column_name_cstr = colname_str.c_str();
  HeapTuple tuple = SearchSysCacheAttName(rel_id, column_name_cstr);
  if (!HeapTupleIsValid(tuple))
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("column \"%s\" does not exist", column_name_cstr)));
  int attribute_number = ((Immutable_pg_attribute)GETSTRUCT(tuple))->attnum;
  XCHECK(attribute_number > 0, DDM_CALLER);
  ReleaseSysCache(tuple);

  return {attribute_number, path};
}

void AddComponentToPath(std::stringstream& ss_path,
                        std::string path_component) {
  if (ss_path.tellp() == std::streampos(0)) {
    // path_component is the first component of the path.
    ss_path << std::quoted(path_component, PathSetBase::kQuote);
  } else {
    ss_path << PathSetBase::kDelimiter
            << std::quoted(path_component, PathSetBase::kQuote);
  }
}

void CheckAttachmentSupported(Attachment attachment) {
  if (!gconf_ddm_enable_over_super_paths) {
    for (auto&& attribute_encoded :
         {attachment.polattrsin, attachment.polattrsout}) {
      std::vector<ddm::AttachedAttribute> attributes =
          DeserializeAttachedAttributes(attribute_encoded);

      bool path_found = std::any_of(attributes.begin(), attributes.end(),
                                    [](ddm::AttachedAttribute attribute) {
                                      return attribute.path.has_value();
                                    });
      if (path_found) UnsupportedSuperPathError();
    }
  }
}

bool IsCastable(TypeName* type_name, Oid typid, int32 typmod) {
  // Check if type is allowed for safecast.
  if (kSafeTypeId.find(type_name->typid) != kSafeTypeId.end()) {
    // Check if it is a safe cast.
    if (type_name->typid == typid && type_name->typmod >= typmod) {
      return true;
    }
  }
  // Check if it is a strict cast.
  return type_name->typid == typid && type_name->typmod == typmod;
}

bool IsSuperNestedCastable(Oid typid) {
  CATEGORY type_category = TypeCategory(typid);

  switch (type_category) {
    case BOOLEAN_TYPE:
    case STRING_TYPE:
    case NUMERIC_TYPE:
      return true;
    default:
      return typid == PARTIQLOID;
  }
}

std::vector<std::string> ParseLbvColumns(
    const RangeTblEntry* rte,
    std::unordered_map<std::string, std::shared_ptr<ColumnDef>>&
        parsed_lbv_columndefs) {
  std::vector<std::string> column_names;

  XCHECK(rte != nullptr, DDM_CALLER);
  XCHECK(rte->rtekind == RTE_SUBQUERY || rte->rtekind == RTE_RLS_SUBQUERY,
         DDM_CALLER);
  Oid rte_relid = GetRelationRelid(rte);
  XCHECK(OidIsValid(rte_relid), DDM_CALLER);
  XCHECK(IsLbv(rte_relid), DDM_CALLER);
  XCHECK(rte->subquery != nullptr, DDM_CALLER);
  XCHECK(rte->subquery->targetList != nullptr, DDM_CALLER);

  ListCell* lc;
  foreach(lc, rte->subquery->targetList) {
    XPGCHECK(IsA(lfirst(lc), TargetEntry), DDM_CALLER);
    Resdom* resdom = reinterpret_cast<TargetEntry*>(lfirst(lc))->resdom;
    std::shared_ptr<ColumnDef> column_definition(
        makeColumnDef(resdom->resname, resdom->restype, resdom->restypmod),
        PAlloc_deleter());
    parsed_lbv_columndefs.insert(
        {std::string(resdom->resname), column_definition});
    column_names.push_back(std::string(resdom->resname));
  }
  return column_names;
}

void CheckAttachmentForColumnsType(
    ParsedPolicyAttachment& attachment,
    std::unordered_map<std::string, std::shared_ptr<ColumnDef>> const&
        columndefs_map,
    const char* rel_name) {
  std::vector<std::shared_ptr<TypeName>> exprTargets =
      ddm::DeserializeTypeNames(attachment.pol.pol.polexpr);
  XCHECK(exprTargets.size() == attachment.polattrs_out.size(), DDM_CALLER);

  // TODO(bogatov): (1) reduce duplication, (2) clarify that this func must be
  // called only for LBV attachments.

  for (size_t i = 0; i < attachment.polattrs_out.size(); ++i) {
    XPGCHECK(std::holds_alternative<std::string>(
        attachment.polattrs_out[i].attribute));
    std::string col_name =
        std::get<std::string>(attachment.polattrs_out[i].attribute);
    const auto& columndef_it = columndefs_map.find(col_name);
    if (columndef_it == columndefs_map.end()) {
      IncompatibleDDMConfigurationError(rel_name);
    }

    TypeName* type = exprTargets[i].get();
    Oid expr_type = type->typid;
    XCHECK(OidIsValid(expr_type), DDM_CALLER);
    TypeName* columndef_typename = columndef_it->second->typname;
    XCHECK(columndef_typename != nullptr, DDM_CALLER);
    Oid typid = columndef_typename->typid;
    XCHECK(OidIsValid(typid), DDM_CALLER);

    bool is_type_compatible = false;
    if (attachment.polattrs_out[i].path) {
      // For nested attachments two conditions: (1) expression is
      // SUPER-castable, and (2) column is SUPER.
      is_type_compatible = ddm::IsSuperNestedCastable(expr_type);
      is_type_compatible &= (typid == PARTIQLOID);
    } else {
      is_type_compatible =
          can_coerce_type(1, &expr_type, &typid, COERCION_IMPLICIT);
    }

    if (!is_type_compatible) {
      IncompatibleDDMConfigurationError(rel_name);
    }
  }

  XCHECK(attachment.pol.parsed_attrs.size() == attachment.polattrs_in.size(),
         DDM_CALLER);
  for (size_t i = 0; i < attachment.polattrs_in.size(); ++i) {
    XPGCHECK(std::holds_alternative<std::string>(
        attachment.polattrs_in[i].attribute));
    std::string col_name =
        std::get<std::string>(attachment.polattrs_in[i].attribute);
    const auto& columndef_it = columndefs_map.find(col_name);
    if (columndef_it == columndefs_map.end()) {
      IncompatibleDDMConfigurationError(rel_name);
    }

    TypeName* parsed_pol_attrs_type_name =
        attachment.pol.parsed_attrs[i].get()->typname;
    TypeName* columndef_typename = columndef_it->second->typname;
    XCHECK(columndef_typename != nullptr, DDM_CALLER);
    Oid input_typid = columndef_typename->typid;
    XCHECK(OidIsValid(input_typid), DDM_CALLER);

    bool is_castable = false;
    if (attachment.polattrs_in[i].path) {
      // Same two conditions for nested attachments, see above.
      is_castable =
          ddm::IsSuperNestedCastable(parsed_pol_attrs_type_name->typid);
      is_castable &= (input_typid == PARTIQLOID);
    } else {
      int32 input_typmod = columndef_typename->typmod;
      input_typmod = GetCorrectTypmod(input_typid, input_typmod);
      is_castable = ddm::IsCastable(parsed_pol_attrs_type_name, input_typid,
                                    input_typmod);
    }

    if (!is_castable) {
      IncompatibleDDMConfigurationError(rel_name);
    }
  }
}
}  // namespace ddm
