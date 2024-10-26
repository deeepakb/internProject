/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#include "ddm/policy_access.h"

#include "ddm/constants.hpp"
#include "ddm/ddm_lf_alp.hpp"
#include "ddm/ddm_policy.hpp"
#include "xen_utils/compiler.h"

#include "pg/src/include/miscadmin.h"
// miscadmin must be imported prior to pg_datashare_objects.
#include "catalog/pg_datashare_objects.h"
#include "external_catalog/table_registry.hpp"
#include "nested_fe/nested_query_api.h"
#include "mv/mv_cinterface.h"

#include "pg/src/include/postgres_ext.h"
#include "pg/src/include/utils/elog.h"
#include "pg/src/include/utils/elog_sqlstate.h"
#include "pg/src/include/utils/lsyscache.h"
#include "pg/src/include/xen_cinterf.h"

extern bool gconf_ddm_enable_over_super_paths;
extern bool gconf_enable_ddm_on_views;
extern bool gconf_enable_ddm_on_mvs;
extern bool gconf_enable_ddm;
extern bool gconf_enable_lf_alp;
extern bool gconf_enable_object_transform;
extern bool gconf_spectrum_enable_complex_references;
extern bool gconf_spectrum_enable_lf_data_filters;

/// Enabled only during unit tests, backing catalog tables may not be created
/// during initdb (called on every PG test instantiation).
bool g_pgtest_ddm = false;


namespace {

/// Check if DDM is GUC enabled for the relation kind of a given range table
/// entry.
///
/// @param rte The range table entry to check.
/// @param rte_oid The oid of the range table entry.
/// @return true if DDM is enabled for the relation kind, otherwise false.
/// @throws XCHECK_UNREACHABLE if the relation kind is unknown.
bool IsDDMEnabledForRelkind(RangeTblEntry* rte, Oid rte_oid) {
  // The basic DDM GUC must be enabled to support any kind of relation.
  bool is_ddm_enabled = gconf_enable_ddm;
  // For views and S3 tables we need to check additional GUCs.
  char relkind = get_rel_relkind(rte_oid);
  switch (relkind) {
    case RELKIND_RELATION:
      break;
    case RELKIND_VIEW:
      if (IsMv(rte_oid)) {
        is_ddm_enabled &= gconf_enable_ddm_on_mvs;
      } else {
        is_ddm_enabled &= gconf_enable_ddm_on_views;
      }
      break;
    default:
      // An external RTE Oid refers to an InvalidOid relation, as it does not
      // exist in the local catalog. In these cases, if this RTE refers to a
      // nested Spectrum table (deferred localization), and if the external
      // table id is already registered in the external registry, handle it
      // differently.
      if ((rte_oid == InvalidOid) &&
          external_catalog::TableRegistry::HasTable(rte->external_table_id) &&
          IsSpectrumNested(rte)) {
        // Check if it's a spectrum relation. In such case, the localization is
        // deferred to post parsing. Hence, we refer to the RTE external table
        // id to infer the table in the external catalog registry.
        XCHECK(rte->rtekind == RTE_RELATION);
        is_ddm_enabled &= gconf_spectrum_enable_lf_data_filters;
        break;
      }

      XCHECK_UNREACHABLE("DDM was applied on unsupported relkind", relkind);
  }
  return is_ddm_enabled;
}

}  // namespace


#ifdef __cplusplus
extern "C" {
#endif
bool HasDDMRewrite(RangeTblEntry* rte) {
  if ((UNLIKELY(!is_xen_process())) && !g_pgtest_ddm) {
    return false;
  }

  // Determine if the relation has attached DDM policies either via native DDM
  // or via Lake Formation ALP (attribute level protection).
  Oid rte_oid = GetRelationRelid(rte);
  bool has_ddm_rewrite = ddm::HasAttachmentsForRel(rte_oid);
  if (gconf_enable_lf_alp) {
    bool has_lf_alp = ddm::HasLakeFormationALPForRTE(rte);
    // This invariant must hold until we support LF and DDM on one relation.
    XCHECK(!(has_ddm_rewrite && has_lf_alp), ddm::LF_ALP_CALLER);
    has_ddm_rewrite |= has_lf_alp;
  }

  // If there is an attached policy we raise an ERROR if DDM is not enabled for
  // the given relation kind. We do not raise this error if current user is a
  // super user. This enables super users to perform emergency administration
  // even when DDM is off.
  if (has_ddm_rewrite && !IsDDMEnabledForRelkind(rte, rte_oid) &&
      !superuser()) {
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("Relation \"%s\" is protected with Dynamic Data Masking "
                    "(DDM), however, DDM is unavailable. Please upgrade to a "
                    "version with DDM to query DDM-protected relations. "
                    "Alternatively, superuser can turn off DDM protection "
                    "using DETACH MASKING POLICY.",
                    get_rel_name(rte_oid))));
  }

  return has_ddm_rewrite;
}

bool HasDDMAttachmentForRel(Oid relid) {
  return ddm::HasAttachmentsForRel(relid);
}

bool DDMEnabledOverSuperPaths() {
  return gconf_ddm_enable_over_super_paths && gconf_enable_object_transform;
}
#ifdef __cplusplus
}
#endif
