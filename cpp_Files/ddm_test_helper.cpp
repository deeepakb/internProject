/// Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#include "pg/ddm/ddm_test_helper.hpp"

namespace pgtest {
Query* ParseAnalyzeQuery(const std::string& sql) {
  AutoValueRestorer<bool> postmaster(IsUnderPostmaster, true /*tmpval*/);
  List* parse_tree = pg::Parse(sql.c_str());
  List* query_tree = pg::Analyze(parse_tree);
  return reinterpret_cast<Query*>(linitial(query_tree));
}

Oid GetRelID(Query* q) {
  RangeTblEntry* rte = reinterpret_cast<RangeTblEntry*>(linitial(q->rtable));
  return rte->relid;
}

bool ColumnDefComparator(std::shared_ptr<ColumnDef>& l,
                         std::shared_ptr<ColumnDef>& r) {
  return strcmp(l->colname, r->colname) == 0 &&
         l->typname->typid == r->typname->typid &&
         l->typname->typmod == r->typname->typmod;
}

bool AttachedAttributeComparator(ddm::AttachedAttribute& l,
                                 ddm::AttachedAttribute& r) {
  bool path_equal = l.path == r.path;
  bool attribute_equal;
  if (std::holds_alternative<std::shared_ptr<ColumnDef>>(l.attribute)) {
    attribute_equal =
        ColumnDefComparator(std::get<std::shared_ptr<ColumnDef>>(l.attribute),
                            std::get<std::shared_ptr<ColumnDef>>(r.attribute));
  } else {
    // Note, for the rest of possible attributes (int, std::string) the
    // comparator is well-defined.
    attribute_equal = l.attribute == r.attribute;
  }

  return path_equal && attribute_equal;
}

}  // namespace pgtest
