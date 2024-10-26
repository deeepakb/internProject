/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#pragma once

#include "policy/policy_utils.h"

#ifdef __cplusplus
extern "C" {
#endif

/// A helper to log the error with an UnsupportedFeature and policy tags.
/// Will not ereport.
/// @param info Information to log.
/// @param polType Type of policy i.e. RLS or DDM.
void LogPolicyUnsupportedFeature(const char* info, PolicyType polType);

/// A helper to log the error with a DDMUnsupportedFeature and DDM tags.
/// Will not ereport.
/// @param info Information to log for DDM policy.
void LogDDMUnsupportedFeature(const char* info);

///
/// @brief A helper to log the error with a DDMUnsupportedAction and DDM tags.
///
/// Will not ereport.
///
/// @param info Information to log for DDM policy.
void LogDDMUnsupportedAction(const char* info);

/// @brief Aborts the query (with ereport ERROR) and prints an error message for
/// the case of trying to apply policies attached to SUPER paths in a version
/// that does not support this functionality yet.
void UnsupportedSuperPathError();

/// @brief Aborts the query (with ereport ERROR) and prints an error message
/// for the case of trying to apply policy(s) attached on relation that is
/// incompatible with the relation.
void IncompatibleDDMConfigurationError(const char* rel_name);

#ifdef __cplusplus
}
#endif
