/// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#pragma once

namespace ddm {
/// Caller identifiers for XCHECK invariants.
inline const char* DDM_CALLER = "DDM";
inline const char* LF_ALP_CALLER = "LF-ALP";
/// Alias for column references inside masking expression.
inline const char* MASK_PSEUDO_TBL = "masked_table";
/// DDM_DUMMY_NAME is modeled as an S3 external table to allow building query
/// for deparsing.
inline const char* DDM_DUMMY_NAME = "dummy";
}  // namespace ddm
