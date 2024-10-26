/// Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#pragma once

#include "ddm/ddm_policy.hpp"

#include "pg/test/pg_test.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <string>

namespace pgtest {
/// Helper to parse query trees. Duplicated from omni tests to avoid
/// dependencies between test cases.
/// @param &std::string sql - input SQL for parse/analysis.
/// @return Query* - Analyzed Query.
Query* ParseAnalyzeQuery(const std::string& sql);

/// Helper to get relation id from analyzed query.
/// The analyzed query is expected have a single
/// range table entry. Relation id of the range table
/// entry is returned.
/// @param Query* - Analyzed Query.
/// @return Oid* - Relation id.
Oid GetRelID(Query* q);

bool ColumnDefComparator(std::shared_ptr<ColumnDef>& l,
                         std::shared_ptr<ColumnDef>& r);

bool AttachedAttributeComparator(ddm::AttachedAttribute& l,
                                 ddm::AttachedAttribute& r);

}  // namespace pgtest
