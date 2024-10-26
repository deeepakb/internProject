/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#include "ddm/unsupported.h"

#include "rls/rls_unsupported.h"
#include "xen_utils/xen_except.hpp"

void LogPolicyUnsupportedFeature(const char* info, PolicyType polType) {
  // TODO(zabhi): Move the common policy API to a new module.
  switch (polType) {
    case PolicyType::kPolicyRLS: {
      LogRLSUnsupportedFeature(info);
      break;
    }
    case PolicyType::kPolicyDDM: {
      LogDDMUnsupportedFeature(info);
      break;
    }
  }
}

void LogDDMUnsupportedFeature(const char* info) {
  LOG_STL_ERROR_RECORD(DDMUnsupportedFeature, "", 0, "DDM", "%s", info);
}

void LogDDMUnsupportedAction(const char* info) {
  LOG_STL_ERROR_RECORD(DDMUnsupportedAction, "", 0, "DDM", "%s", info);
}

void UnsupportedSuperPathError() {
  ereport(
      ERROR,
      (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
       errmsg("One or more masking policies are attached to a SUPER path on a "
              "relation referenced in this query, however applying masking "
              "policies over nested paths is unavailable. Please upgrade to a "
              "newer version, or alternatively a superuser or a security admin "
              "can DROP ... CASCADE the policies that are attached to nested "
              "paths.")));
}

void IncompatibleDDMConfigurationError(const char* rel_name) {
  ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                  errmsg("Incompatible DDM configuration on relation \"%s\"",
                         rel_name)));
}
