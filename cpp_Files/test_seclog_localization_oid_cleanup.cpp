/// Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.

// These two includes must go first and in this order.
// clang-format off
#include <gtest/gtest.h>
#include "pg/test/pg_test.hpp"
// clang-format on

#include "seclog/seclog_access_control.h"
#include "seclog/seclog_masking.hpp"
#include "seclog/sensitive_objects_manager.hpp"

#include "pg/src/include/catalog/namespace.h"

#include <sstream>
#include <string>

extern bool AmLeaderProcess;

namespace pgtest {

class TestSecLogLocalizationOidCleanup : public pg::PgTest {
 public:
  static void SetUpTestCase() {
    // By default, the AmLeaderProcess flag is not set on the PGtest process.
    // SecLog asserts that SecLog functions are called on the LN - `EnforceLN`.
    original_am_leader_ = AmLeaderProcess;
    AmLeaderProcess = true;
  }

  static void TearDownTestCase() { AmLeaderProcess = original_am_leader_; }

  void SetUp() override {
    std::string create_table_sql =
        "create temp table " + test_tablename_ + " ( a int )";
    pg::RunQuery(create_table_sql);
    XCHECK_NOTNULL(Xen);
    Xen->sensitive_objects_manager = new SensitiveObjectsManager();
  }

  void TearDown() override {
    std::string drop_table_sql = "drop table " + test_tablename_;
    pg::RunQuery(drop_table_sql);
    XCHECK_NOTNULL(Xen);
    delete Xen->sensitive_objects_manager;
    Xen->sensitive_objects_manager = nullptr;
  }

  std::string test_tablename_{"seclog_localization_table"};

 private:
  static bool original_am_leader_;
};

bool TestSecLogLocalizationOidCleanup::original_am_leader_ = false;

PG_TEST_F(TestSecLogLocalizationOidCleanup, TestLocalizationOidCleanup) {
  // Test cleanup when temps are dropped individually and their corresponding
  // Oids are removed from SecLog tracking individually.
  Oid oid = RelnameGetRelid(test_tablename_.c_str());
  AddToSTVSensitiveLocalizationOIDs(oid);
  EXPECT_EQ(true,
            Xen->sensitive_objects_manager->IsSensitiveLocalizationOid(oid));
  RemoveFromSTVSensitiveLocalizationOIDs(oid, true /* verify */);
  EXPECT_EQ(false,
            Xen->sensitive_objects_manager->IsSensitiveLocalizationOid(oid));
}

PG_TEST_F(TestSecLogLocalizationOidCleanup, TestTempNamespaceCleanup) {
  // Test when the entire temp namespace is dropped and all Oids under the
  // namespace must be removed from SecLog tracking.
  Oid oid = RelnameGetRelid(test_tablename_.c_str());
  AddToSTVSensitiveLocalizationOIDs(oid);
  EXPECT_EQ(true,
            Xen->sensitive_objects_manager->IsSensitiveLocalizationOid(oid));
  seclog::RemoveCurrentSessionLocalizations();
  EXPECT_EQ(false,
            Xen->sensitive_objects_manager->IsSensitiveLocalizationOid(oid));
}

PG_TEST_F(TestSecLogLocalizationOidCleanup, TestMemoryConsumption) {
  Oid oid = RelnameGetRelid(test_tablename_.c_str());
  AddToSTVSensitiveLocalizationOIDs(oid);
  size_t add_memory = xen::allocator::a_size_alloced();
  RemoveFromSTVSensitiveLocalizationOIDs(oid, true /* verify */);
  size_t remove_memory = xen::allocator::a_size_alloced();
  // Memory consumed by the entry in shared memory.
  EXPECT_EQ((add_memory - remove_memory), 16);
}

PG_TEST_F(TestSecLogLocalizationOidCleanup, TestLocalizationNamespace) {
  // Test ensures that localization Oids of localized objects tracked by SecLog
  // belong to the session's only temporary namespace.
  EXPECT_EQ(SensitiveObjectsManager::GetCurrentSessionLocalizationNamespace(),
            getTempNamespace());
  EXPECT_TRUE(isTempNamespace(
      SensitiveObjectsManager::GetCurrentSessionLocalizationNamespace()));
}

}  // namespace pgtest
