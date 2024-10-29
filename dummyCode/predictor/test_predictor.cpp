// Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#include "test_predictor.hpp"

#include "dory/spectrum_features.hpp"
#include "predictor/balanced_collector.hpp"
#include "predictor/data_collector.hpp"
#include "predictor/predictor_main.hpp"
#include "predictor/predictor_util.hpp"
#include "predictor/predictor_worker.hpp"
#include "predictor/types.hpp"
#include "predictor/value_predictor.hpp"
#include "stats/xen_genstats.h"
#include "sys/pg_utils.hpp"
#include "sys/query.hpp"
#include "sys/queryid_manager.hpp"
#include "wlm/wlm_utils.hpp"
#include "xen_utils/auto_value_restorer.hpp"
#include "xen_utils/process_registry.hpp"
#include "xen_utils/xen_logmanager.hpp"

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <array>
#include <algorithm>
#include <ctime>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

extern bool gconf_enable_predictor;
extern bool gconf_enable_predictor_worker;
extern int gconf_predictor_default_data_window;
extern int gconf_predictor_exec_time_data_window;
extern int gconf_predictor_memory_data_window;
extern int gconf_predictor_copy_memory_data_window;
extern int gconf_predictor_local_query_scaling_data_window;
extern int gconf_predictor_local_copy_scaling_data_window;
extern int gconf_predictor_default_refresh_rate;
extern int gconf_predictor_burst_exec_time_refresh_rate;
extern int gconf_mv_refresh_time_predictor_refresh_rate;
extern int gconf_predictor_training_timeout_ms;
extern int gconf_vacuum_predictor_training_timeout_ms;
extern bool gconf_vacuum_predictor_enable;
extern int gconf_predictor_worker_frequency_s;
extern int gconf_predictor_default_test_data_percentage;
extern int gconf_predictor_default_error_test_data_percentage;
extern double gconf_predictor_worker_wlm_occupancy_allowance;
extern char* gconf_execution_time_predictor_ranges;
extern char* gconf_execution_without_compile_time_predictor_ranges;
extern char* gconf_memory_predictor_ranges;
extern char* gconf_mv_refresh_exec_time_predictor_ranges;
extern char* gconf_burst_exec_time_predictor_ranges;
extern char* gconf_query_label;
extern bool gconf_enable_wlm;
extern bool gconf_predictor_concat_bdc_ddc;
extern int gconf_predictor_max_repetition_count_in_ddc;
extern bool gconf_predictor_enable_moe;
extern bool gconf_predictor_enable_moe_memory;
extern char* gconf_predictor_memory_moe_param_override;
extern char* gconf_predictor_default_xgboost_param_override;
extern char* gconf_predictor_default_moe_xgboost_param_override;
extern int gconf_predictor_knn_default_refresh_rate;
extern double gconf_predictor_memory_prediction_rescaling_factor;
extern bool gconf_predictor_enable_backwards_feature_compatibility;
extern int gconf_predictor_ddc_fuzzy_match_mul;
extern bool gconf_local_query_scaling_predictor_enable;
extern bool gconf_local_query_scaling_predictor_enable_moe;
extern int gconf_predictor_fuzzy_match_copy_exec_time;
extern double gconf_predictor_scaling_ensemble_clip_min;
extern double gconf_predictor_scaling_ensemble_clip_max;
extern bool gconf_predictor_enable_moe_copy_exec_time;
extern bool gconf_copy_exec_time_predictor;
extern bool gconf_copy_query_memory_predictor_enable;
extern int gconf_predictor_copy_memory_refresh_rate;
extern double gconf_predictor_uncertainty_threshold;
extern bool gconf_predictor_async_load;
extern bool gconf_predictor_enable_s3_persistence;
extern bool gconf_predictor_ensemble_enable_overwriting;
namespace predictor {

CPPUNIT_TEST_SUITE_REGISTRATION(TestPredictor);

void TestPredictor::setUp() {
  gconf_query_label = "label";
  gconf_enable_wlm = true;

  // Copy the guc values to corresponding local variables.
  local_enable_predictor_ = gconf_enable_predictor;
  local_enable_predictor_worker_ = gconf_enable_predictor_worker;
  local_predictor_default_data_window_ = gconf_predictor_default_data_window;
  local_predictor_default_refresh_rate_ = gconf_predictor_default_refresh_rate;
  local_predictor_burst_exec_time_refresh_rate_ =
      gconf_predictor_burst_exec_time_refresh_rate;
  local_predictor_training_timeout_ms_ = gconf_predictor_training_timeout_ms;
  local_vacuum_predictor_training_timeout_ms_ =
      gconf_vacuum_predictor_training_timeout_ms;
  local_gconf_vacuum_predictor_enable_ = gconf_vacuum_predictor_enable;
  local_predictor_worker_frequency_s_ = gconf_predictor_worker_frequency_s;
  local_predictor_worker_wlm_occupancy_allowance_ =
      gconf_predictor_worker_wlm_occupancy_allowance;
  local_predictor_enable_backwards_feature_compatibility_ =
      gconf_predictor_enable_backwards_feature_compatibility;
  local_local_query_scaling_predictor_enable_moe =
      gconf_local_query_scaling_predictor_enable_moe;
  local_local_query_scaling_predictor_enable =
      gconf_local_query_scaling_predictor_enable;
  local_predictor_fuzzy_match_copy_exec_time =
      gconf_predictor_fuzzy_match_copy_exec_time;
  local_copy_exec_time_predictor = gconf_copy_exec_time_predictor;
  local_copy_query_memory_predictor_enable =
      gconf_copy_query_memory_predictor_enable;
  local_predictor_copy_memory_refresh_rate =
      gconf_predictor_copy_memory_refresh_rate;
  local_predictor_uncertainty_threshold = gconf_predictor_uncertainty_threshold;

  gconf_enable_predictor = true;
  gconf_enable_predictor_worker = true;
  gconf_predictor_default_data_window = 2000;
  gconf_predictor_exec_time_data_window = 2000;
  gconf_predictor_memory_data_window = 2000;
  gconf_predictor_copy_memory_data_window = 2000;
  gconf_predictor_local_query_scaling_data_window = 2000;
  gconf_predictor_local_copy_scaling_data_window = 2000;
  // These GUCs need a value since MoE for memory predictor because they are
  // needed for ValuePredictor constructor.
  gconf_predictor_memory_moe_param_override = "";
  gconf_predictor_default_xgboost_param_override = "";
  gconf_predictor_default_moe_xgboost_param_override = "";
  gconf_predictor_memory_prediction_rescaling_factor = 1.0;

  gconf_local_query_scaling_predictor_enable = true;

  gconf_predictor_enable_backwards_feature_compatibility = true;
  // We disable the rounding in this test, and will enable it only in
  // the dedicated test.
  gconf_predictor_ddc_fuzzy_match_mul = -1;

  // Refresh XGBoost and KNN asynchronously in MoE is tested seperately.
  // in TestAsyncXgboostKnnRefreshterminate();
  gconf_predictor_knn_default_refresh_rate = 0;

  gconf_copy_exec_time_predictor = true;

  gconf_copy_query_memory_predictor_enable = true;

  // TestEnsembleScalingPredictionWithoutOverwrite will cover
  // the false case specifically.
  gconf_predictor_ensemble_enable_overwriting = true;

  if (XenLogManager::get_logmgr() == nullptr) {
    LogManager* unittest_logmgr = new LogManager(false);
    unittest_logmgr->set_logmgr();
  }
  CPPUNIT_ASSERT(XenLogManager::get_logmgr() != nullptr);

  if (Xen->m_processes == nullptr) {
    Xen->m_processes = new Process_registry();
    MyXenPid = Xen->m_processes->add(getpid(), "CppTest", UnitTestProc);
    XenPidValue = MyXenPid.GetAtomic();
  }
  CPPUNIT_ASSERT(Xen->m_processes != nullptr);

  if (Xen->queryid_manager == nullptr) {
    Xen->queryid_manager = new queryid::QueryIdManager();
  }

  // To simulate creation of worker even with 100% wlm occupancy. Ideally in
  // prod, we want to set the guc value to be 100.0 so that predictor workers
  // are created even when wlm is fully occupied. This is because these
  // workers are leader only and usually finish quickly (631ms of P95
  // model building time).
  gconf_predictor_worker_wlm_occupancy_allowance = 100.0;
  // To simulate worker on every attempt.
  gconf_predictor_worker_frequency_s = 0;
  // To simulate model refresh even if there is no new data.
  gconf_predictor_default_refresh_rate = 0;
  gconf_predictor_burst_exec_time_refresh_rate = 0;
  gconf_predictor_copy_memory_refresh_rate = 0;
  // 100ms timeout for model training.
  gconf_predictor_training_timeout_ms = 1000;
  // 100ms timeout for model training
  gconf_vacuum_predictor_training_timeout_ms = 1000;
  // Enable vacuum predictor for testing.
  gconf_vacuum_predictor_enable = true;
  // Disable fuzzy match for copy predictor as a default.
  gconf_predictor_fuzzy_match_copy_exec_time = -1;
  Xen->predictors = new Predictors();

  small_query_features_[QueryPredictor::IsSelect] = 1;
  small_query_features_[QueryPredictor::Scan] = 1;
  small_query_features_[QueryPredictor::ScanRows] = kSmallInput;
  // Populate query feature for local query scaling predictor with
  // small number for testing.
  small_local_query_scaling_features_[LocalQueryScalingPredictor::IsSelect] = 1;
  small_local_query_scaling_features_[LocalQueryScalingPredictor::Scan] = 1;
  small_local_query_scaling_features_[LocalQueryScalingPredictor::ScanRows] =
      kSmallInput;
  small_local_query_scaling_features_[LocalQueryScalingPredictor::ClusterSize] =
      4;
  small_mv_features_[MvRefreshExecTimePredictor::Scan] = 1;
  small_mv_features_[MvRefreshExecTimePredictor::ScanRows] = kSmallInput;
  small_vac_features_[VacuumExecTimePredictor::IsVacuumDelete] = 1;
  small_vac_features_[VacuumExecTimePredictor::VacuumDeletedSoFar] = 1;
  small_vac_features_[VacuumExecTimePredictor::TotalActiveQueries] =
      kSmallInput;
  small_copy_features_[CopyExecTimePredictor::NumFiles] = kSmallInput;
  small_copy_features_[CopyExecTimePredictor::MaxFileSize] = kSmallInput;
  small_copy_features_[CopyExecTimePredictor::MinFileSize] = kSmallInput;
  small_copy_features_[CopyExecTimePredictor::P10FileSize] = kSmallInput;
  small_copy_features_[CopyExecTimePredictor::P50FileSize] = kSmallInput;
  small_copy_features_[CopyExecTimePredictor::P90FileSize] = kSmallInput;
  small_copy_memory_features_[CopyMemoryPredictor::NumFiles] = kSmallInput;
  small_copy_memory_features_[CopyMemoryPredictor::MaxFileSize] = kSmallInput;
  small_copy_memory_features_[CopyMemoryPredictor::MinFileSize] = kSmallInput;
  small_copy_memory_features_[CopyMemoryPredictor::P10FileSize] = kSmallInput;
  small_copy_memory_features_[CopyMemoryPredictor::P50FileSize] = kSmallInput;
  small_copy_memory_features_[CopyMemoryPredictor::P90FileSize] = kSmallInput;
  large_query_features_[QueryPredictor::IsSelect] = 1;
  large_query_features_[QueryPredictor::Scan] = 1;
  large_query_features_[QueryPredictor::ScanRows] = kLargeInput;
  large_local_query_scaling_features_[LocalQueryScalingPredictor::IsSelect] = 1;
  large_local_query_scaling_features_[LocalQueryScalingPredictor::Scan] = 1;
  large_local_query_scaling_features_[LocalQueryScalingPredictor::ScanRows] =
      kLargeInput;
  large_local_query_scaling_features_[LocalQueryScalingPredictor::ClusterSize] =
      4;
  large_mv_features_[MvRefreshExecTimePredictor::Scan] = 1;
  large_mv_features_[MvRefreshExecTimePredictor::ScanRows] = kLargeInput;
  large_vac_features_[VacuumExecTimePredictor::IsVacuumDelete] = 1;
  large_vac_features_[VacuumExecTimePredictor::VacuumDeletedSoFar] = 1;
  large_vac_features_[VacuumExecTimePredictor::TotalActiveQueries] =
      kLargeInput;
  large_copy_features_[CopyExecTimePredictor::NumFiles] = kLargeInput;
  large_copy_features_[CopyExecTimePredictor::MaxFileSize] = kLargeInput;
  large_copy_features_[CopyExecTimePredictor::MinFileSize] = kLargeInput;
  large_copy_features_[CopyExecTimePredictor::P10FileSize] = kLargeInput;
  large_copy_features_[CopyExecTimePredictor::P50FileSize] = kLargeInput;
  large_copy_features_[CopyExecTimePredictor::P90FileSize] = kLargeInput;
  large_copy_memory_features_[CopyMemoryPredictor::NumFiles] = kLargeInput;
  large_copy_memory_features_[CopyMemoryPredictor::MaxFileSize] = kLargeInput;
  large_copy_memory_features_[CopyMemoryPredictor::MinFileSize] = kLargeInput;
  large_copy_memory_features_[CopyMemoryPredictor::P10FileSize] = kLargeInput;
  large_copy_memory_features_[CopyMemoryPredictor::P50FileSize] = kLargeInput;
  large_copy_memory_features_[CopyMemoryPredictor::P90FileSize] = kLargeInput;
}

void TestPredictor::tearDown() {
  delete Xen->m_processes;
  Xen->m_processes = nullptr;
  MyPredictor.reset();
  delete Xen->predictors;
  Xen->predictors = nullptr;
  delete Xen->queryid_manager;
  Xen->queryid_manager = nullptr;

  // Destroy LogManager.
  LogManager* unittest_logmgr = XenLogManager::get_logmgr();
  unittest_logmgr->clean_logmgr();
  CPPUNIT_ASSERT(XenLogManager::get_logmgr() == NULL);

  // Restore the guc values.
  gconf_enable_predictor = local_enable_predictor_;
  gconf_enable_predictor_worker = local_enable_predictor_worker_;
  gconf_predictor_default_data_window = local_predictor_default_data_window_;
  gconf_predictor_default_refresh_rate = local_predictor_default_refresh_rate_;
  gconf_predictor_burst_exec_time_refresh_rate =
      local_predictor_burst_exec_time_refresh_rate_;
  gconf_predictor_training_timeout_ms = local_predictor_training_timeout_ms_;
  gconf_vacuum_predictor_training_timeout_ms =
      local_vacuum_predictor_training_timeout_ms_;
  gconf_vacuum_predictor_enable = local_gconf_vacuum_predictor_enable_;
  gconf_predictor_worker_frequency_s = local_predictor_worker_frequency_s_;
  gconf_predictor_worker_wlm_occupancy_allowance =
      local_predictor_worker_wlm_occupancy_allowance_;
  gconf_predictor_enable_backwards_feature_compatibility =
      local_predictor_enable_backwards_feature_compatibility_;
  gconf_local_query_scaling_predictor_enable_moe =
      local_local_query_scaling_predictor_enable_moe;
  gconf_local_query_scaling_predictor_enable =
      local_local_query_scaling_predictor_enable;
  gconf_predictor_fuzzy_match_copy_exec_time =
      local_predictor_fuzzy_match_copy_exec_time;
  gconf_copy_exec_time_predictor = local_copy_exec_time_predictor;
  gconf_copy_query_memory_predictor_enable =
      local_copy_query_memory_predictor_enable;
  gconf_predictor_copy_memory_refresh_rate =
      local_predictor_copy_memory_refresh_rate;
  gconf_predictor_uncertainty_threshold = local_predictor_uncertainty_threshold;

  gconf_query_label = nullptr;
  gconf_enable_wlm = false;
}

void TestPredictor::LoadData(std::string filename,
                             DataCollector* data_collector) {
  std::string delm(",");
  std::ifstream file(filename);
  std::string line("");
  // Iterate through each line and split the content using delimiter
  std::vector<std::vector<float>> raw_data;
  size_t i = 0;
  size_t j;
  while (getline(file, line)) {
    std::vector<std::string> vec;
    boost::algorithm::split(vec, line, boost::is_any_of(delm));
    raw_data.push_back(std::vector<float>(vec.size()));
    j = 0;
    for (std::string a : vec) {
      raw_data[i][j] = std::stod(a);
      ++j;
    }
    ++i;
  }
  // Close the File
  file.close();
  // Load all the data.
  std::vector<std::vector<float> >::iterator it;
  for (it = raw_data.begin(); it != raw_data.end(); ++it) {
    std::vector<float> input_v((*it).begin(), (*it).end() - 1);
    float output = (*it).back();
    data_collector->AddData(input_v, output);
  }
}

void TestPredictor::CheckHistogramUncertaintyResults(
    std::unique_ptr<QueryPredictorClient>& client,
    std::vector<int>& cluster_size, std::vector<double>& expected) {
  CPPUNIT_ASSERT_EQUAL(expected.size(), cluster_size.size());
  std::vector<double> results;
  int MAX_TEST_CLUSTER_SIZE = 200;
  for (int i = 0; i < MAX_TEST_CLUSTER_SIZE; i++) {
    results.push_back(client->GetXgbPredUncertainty(i));
  }
  // Check if uncertainty of cluster size provided matches with expectation.
  for (int i = 0; i < static_cast<int>(expected.size()); i++) {
    int cs = cluster_size[i];
    CPPUNIT_ASSERT_DOUBLES_EQUAL(expected[i], results[cs],
                                 std::numeric_limits<float>::epsilon());
  }
}

void TestPredictor::TestDataReset() {
  // Shrink the window so that not all data can fit.
  gconf_predictor_default_data_window = 1000;
  std::unique_ptr<DataCollector> data_collector(
      new DataCollector("TestDataResetCollector", 28,
          gconf_predictor_default_data_window));
  LoadData("test/cppunit/predictor/exec_time_data.csv", data_collector.get());

  data_collector->Reset();
  CPPUNIT_ASSERT_EQUAL(0, static_cast<int>(data_collector->GetInputSize()));

  // Let's load all the data again, to test that after reset, the data
  // collector can be filled with new data.
  LoadData("test/cppunit/predictor/exec_time_data.csv", data_collector.get());
  CPPUNIT_ASSERT_EQUAL(1000, static_cast<int>(data_collector->GetInputSize()));
}

void TestPredictor::TestDeserializationRead() {
  srand(time(NULL));
  // Predictors::ReadFromFile reads in 16KB chunk and in every chunk the
  // last byte is null character. So, the Predictors::ReadFromFile can read
  // at most 16MB - 1024 (kMaxFileReadSizeBytes) total bytes.
  const std::string filename("/tmp/cppunit_huge_persist_file");
  {
    std::ofstream ofs(filename);
    for (int i = 0; i < kMaxFileReadSizeBytes; ++i) {
      ofs << static_cast<char >(1 + std::rand() % 126);
    }
  }
  std::string read_str_1 = Predictors::ReadFromFile(filename.c_str());
  CPPUNIT_ASSERT_EQUAL(kMaxFileReadSizeBytes,
      static_cast<int >(read_str_1.size()));
  {
    // Let's append 10 more characters so that it's bigger than
    // kMaxFileReadSizeBytes. But since Predictors::ReadFromFil doesn't read
    // bigger than the above kMaxFileReadSizeBytes, the maximum read bytes
    // should still be kMaxFileReadSizeBytes.
    std::ofstream ofs(filename, std::ios_base::app);
    for (int i = 0; i < 10; ++i) {
      ofs << static_cast<char >(std::rand() % 126);
    }
  }
  std::string read_str_2 = Predictors::ReadFromFile(filename.c_str());
  // Make sure the size is still kMaxFileReadSizeBytes.
  CPPUNIT_ASSERT_EQUAL(kMaxFileReadSizeBytes,
      static_cast<int >(read_str_2.size()));
  CPPUNIT_ASSERT(read_str_1 == read_str_2);
}

bool TestPredictor::CreateNewWorker(PredictorWorker* worker,
    double occupancy, bool should_wait_for_new_worker) {
  pid_t last_pid = worker->GetWorkerContext().worker_pid_;
  worker->Start(occupancy);
  pid_t new_pid = worker->GetWorkerContext().worker_pid_;
  // In case we need to wait for the new worker to finish, let's try 10
  // times (~10s) to make sure the worker process finished.
  int trial = 10;
  Xen_proc_info::Ptr new_worker = nullptr;
  do {
    // We want to wait until the worker has started and finished.
    // When the worker finishes it removes itself from the process registry.
    // So, new_worker == nullptr means the worker started and finished.
    sleep(1);
    new_worker = Xen->m_processes->get_proc_info(new_pid, false);
    --trial;
  } while (should_wait_for_new_worker && trial > 0 && new_worker != nullptr);
  return last_pid != new_pid;
}

void TestPredictor::TestPredictorWorkerGucDisabled() {
  PredictorWorker worker;
  // Predictor worker created when gucs are enabled.
  gconf_enable_predictor = true;
  gconf_enable_predictor_worker = true;
  CPPUNIT_ASSERT(CreateNewWorker(&worker, 100.0, true));
  // Predictor workers are not created when any of the gucs is disabled.
  gconf_enable_predictor = false;
  gconf_enable_predictor_worker = true;
  CPPUNIT_ASSERT(!CreateNewWorker(&worker, 100.0, true));
  gconf_enable_predictor = true;
  gconf_enable_predictor_worker = false;
  CPPUNIT_ASSERT(!CreateNewWorker(&worker, 100.0, true));
  gconf_enable_predictor = false;
  gconf_enable_predictor_worker = false;
  CPPUNIT_ASSERT(!CreateNewWorker(&worker, 100.0, true));
  // Predictor worker created again when gucs are enabled.
  gconf_enable_predictor = true;
  gconf_enable_predictor_worker = true;
  CPPUNIT_ASSERT(CreateNewWorker(&worker, 100.0, true));
}

void TestPredictor::TestPredictorWorkerWithWlmOccupancy() {
  PredictorWorker worker;

  // Tests that predictor worker starts even if wlm is 100% occupied.
  gconf_predictor_worker_wlm_occupancy_allowance = 100.0;
  CPPUNIT_ASSERT(CreateNewWorker(&worker, 100.0, true));
  CPPUNIT_ASSERT(CreateNewWorker(&worker, 99.0, true));

  // Tests that predictor worker never starts even if wlm allowance is 0%,
  // except when the occupancy is 0%.
  gconf_predictor_worker_wlm_occupancy_allowance = 0;
  CPPUNIT_ASSERT(!CreateNewWorker(&worker, 100.0, true));
  CPPUNIT_ASSERT(!CreateNewWorker(&worker, 99.0, true));
  CPPUNIT_ASSERT(CreateNewWorker(&worker, 0.0, true));

  // Check the normal cases.
  gconf_predictor_worker_wlm_occupancy_allowance = 90.0;
  CPPUNIT_ASSERT(CreateNewWorker(&worker, 90.0, true));
  CPPUNIT_ASSERT(CreateNewWorker(&worker, 89.0, true));
  CPPUNIT_ASSERT(!CreateNewWorker(&worker, 90.1, true));
}

void TestPredictor::TestPredictorWorkerExists() {
  PredictorWorker worker;
  // Special value for testing, to simulate process existence.
  gconf_predictor_training_timeout_ms = 0;
  CPPUNIT_ASSERT(CreateNewWorker(&worker, 100.0,
      false /* Should not wait for the new worker to finish. */));
  // Record the last_run_time for later comparison.
  int64_t last_run_time = worker.GetWorkerContext().last_run_time_;
  // Above created worker will not remove itself from the process registry
  // due to the above special value for gconf_predictor_training_timeout_ms.
  // As a result, if we want to create another worker without removing the
  // existing worker first, it should not create another, and the
  // last_run_time_ should be updated.
  CPPUNIT_ASSERT(!CreateNewWorker(&worker, 100.0,
      false /* Should not wait for the new worker to finish. */));
  CPPUNIT_ASSERT(last_run_time < worker.GetWorkerContext().last_run_time_);
}

void TestPredictor::TestPredictorWorkerTooSoonToBeCreated() {
  PredictorWorker worker;

  CPPUNIT_ASSERT(CreateNewWorker(&worker, 100.0, true));
  int64_t last_run_time = worker.GetWorkerContext().last_run_time_;
  // Tests that predictor worker should not be created within 2s of the last
  // creation.
  gconf_predictor_worker_frequency_s = 2;
  // Too soon to create another worker.
  CPPUNIT_ASSERT(!CreateNewWorker(&worker, 100.0, true));
  // Wait now >2s so that it is started.
  int max_waiting_time = 20;
  bool is_worker_created = false;
  while (max_waiting_time > 0 && !is_worker_created) {
    sleep(1);
    is_worker_created = CreateNewWorker(&worker, 100.0, true);
    --max_waiting_time;
  }
  // Make sure that a new worker is created.
  CPPUNIT_ASSERT(is_worker_created);
  // Make sure that two workers didn't create too soon.
  CPPUNIT_ASSERT(worker.GetWorkerContext().last_run_time_ - last_run_time >=
      gconf_predictor_worker_frequency_s * US_PER_SEC);
}

void TestPredictor::TestPredictorWorkerNoRebuildNeeded() {
  // Predictor worker should not be created if there is no need to rebuild a
  // model. To simulate that we set the predictor refresh rate to be non zero.
  // Set the predictor refresh rate to every 10 queries.
  gconf_predictor_default_refresh_rate = 10;
  gconf_predictor_burst_exec_time_refresh_rate = 10;
  gconf_mv_refresh_time_predictor_refresh_rate = 10;
  gconf_predictor_copy_memory_refresh_rate = 10;

  MyPredictor.reset();
  delete Xen->predictors;
  Xen->predictors = new Predictors();

  // Get the reference to new data counter so that we can modify for the
  // purpose of the test.
  int* new_data_ref =
      Xen->predictors->execution_time_predictor_->GetNewDataCounter();
  int* new_mv_data_ref =
      Xen->predictors->mv_refresh_exec_time_predictor_->GetNewDataCounter();
  PredictorWorker worker;
  // Test <10 new data will not create a new worker.
  for (int i = 0; i < 10; ++i) {
    *new_data_ref = i;
    *new_mv_data_ref = i;
    // Worker should not be created.
    CPPUNIT_ASSERT(!CreateNewWorker(&worker, 100.0, true));
  }
  *new_data_ref = 10;
  *new_mv_data_ref = 10;
  // Should now create the worker.
  CPPUNIT_ASSERT(CreateNewWorker(&worker, 100.0, true));
  // Test again that <10 new data will not create a new worker.
  for (int i = 0; i < 10; ++i) {
    *new_data_ref = i;
    *new_mv_data_ref = i;
    // Worker should not be created.
    CPPUNIT_ASSERT(!CreateNewWorker(&worker, 100.0, true));
  }
  *new_data_ref = 20;
  *new_mv_data_ref = 20;
  // Should now create the worker.
  CPPUNIT_ASSERT(CreateNewWorker(&worker, 100.0, true));
}

void TestPredictor::TestPredictorWorkerDownloadFromS3() {
  AutoValueRestorer<bool> enable_predictor(gconf_enable_predictor, true);
  AutoValueRestorer<bool> enable_worker(gconf_enable_predictor_worker, true);
  AutoValueRestorer<double> worker_occupency(
      gconf_predictor_worker_wlm_occupancy_allowance, 100);
  AutoValueRestorer<int> worker_frequency(gconf_predictor_worker_frequency_s,
                                          1);
  AutoValueRestorer<bool> enable_s3(gconf_predictor_enable_s3_persistence,
                                    true);
  AutoValueRestorer<int> predictor_training_limit(gconf_predictor_training_timeout_ms,
                                                  1000);
  SetUpModelParameters();
  MyPredictor.reset();
  delete Xen->predictors;
  Xen->predictors = new Predictors();
  PredictorWorker worker;

  // Test that worker is not created when guc is false.
  AutoValueRestorer<bool> load_trial_off(gconf_predictor_async_load, false);
  CPPUNIT_ASSERT(!Xen->predictors->ShouldAsyncLoadPredictor());
  // Should not create another worker.
  CPPUNIT_ASSERT(!CreateNewWorker(&worker, 0.0, false /* wait for worker*/));

  // Test that workers are created exactly the number of trial count.
  AutoValueRestorer<bool> load_trial_on(gconf_predictor_async_load, true);
  CPPUNIT_ASSERT(Xen->predictors->ShouldAsyncLoadPredictor());
  CPPUNIT_ASSERT(CreateNewWorker(&worker, 0.0, true /* wait for worker*/));
}

void TestPredictor::CheckOnlyOneWriteSet(
    xen::span<const DataT> features,
    QueryPredictor::QueryPredictorFeatures a_set) {
  for (int i = QueryPredictor::IsInsert; i <= QueryPredictor::IsUnload; ++i) {
    if (i != a_set) {
      CPPUNIT_ASSERT_EQUAL(features[i], 0.0f);
    }
  }
  CPPUNIT_ASSERT_EQUAL(features[a_set], 1.0f);
}

void TestPredictor::TestQueryTypes() {
  gconf_query_label = const_cast<char*>("test");
  gconf_enable_wlm = true;
  SetCurrentTransaction(1);
  Query_desc* q = Query_desc::Create("", Query_desc::QueryCmdTypeNone);
  Query query;
  memset(&query, 0, sizeof(Query));
  EState es;
  memset(&es, 0, sizeof(EState));

  // Test IsSelect is set.
  query.into = NULL;
  query.commandType = CMD_SELECT;
  SetOperationTypeForSqa(&query, q->features());
  SetPredictorOperationType<QueryPredictor>(&query, q->PredictorFeatures());
  q->SetPredictorQueryTypeFeature(&query, false /* is_spectrum_copy */,
                                  false /* is_unload */);
  CheckOnlyOneSelectSet(q->PredictorFeatures(), QueryPredictor::IsSelect);
  CheckOnlyOneSelectSet(
      xen::span<double>{q->features(), QueryClassifier::kMaxFeatures},
      QueryClassifier::IsSelect);

  // Test IsCtas is set.
  RangeVar r;
  query.into = &r;
  SetOperationTypeForSqa(&query, q->features());
  SetPredictorOperationType<QueryPredictor>(&query, q->PredictorFeatures());
  q->SetPredictorQueryTypeFeature(&query, false /* is_spectrum_copy */,
                                  false /* is_unload */);
  CheckOnlyOneSelectSet(q->PredictorFeatures(), QueryPredictor::IsCtas);
  CheckOnlyOneSelectSet(
      xen::span<double>{q->features(), QueryClassifier::kMaxFeatures},
      QueryClassifier::IsCtas);

  // IsCtas should remain set in rest of the tests because it's copied from
  // query classifier feature set. This also tests that a write feature can be
  // set in conjunction with other select features.
  // Test IsInsert is set.
  query.commandType = CMD_INSERT;
  q->SetPredictorQueryTypeFeature(&query, false /* is_spectrum_copy */,
                                  false /* is_unload */);
  CheckOnlyOneSelectSet(
      xen::span<double>{q->features(), QueryClassifier::kMaxFeatures},
      QueryPredictor::IsCtas);
  CheckOnlyOneWriteSet(q->PredictorFeatures(), QueryPredictor::IsInsert);
  q->PredictorFeatures()[QueryPredictor::IsInsert] = 0;

  // Test IsUpdate is set.
  query.commandType = CMD_UPDATE;
  q->SetPredictorQueryTypeFeature(&query, false /* is_spectrum_copy */,
                                  false /* is_unload */);
  CheckOnlyOneSelectSet(
      xen::span<double>{q->features(), QueryClassifier::kMaxFeatures},
      QueryPredictor::IsCtas);
  CheckOnlyOneWriteSet(q->PredictorFeatures(), QueryPredictor::IsUpdate);
  q->PredictorFeatures()[QueryPredictor::IsUpdate] = 0;

  // Test IsDelete is set.
  query.commandType = CMD_DELETE;
  q->SetPredictorQueryTypeFeature(&query, false /* is_spectrum_copy */,
                                  false /* is_unload */);
  CheckOnlyOneSelectSet(
      xen::span<double>{q->features(), QueryClassifier::kMaxFeatures},
      QueryPredictor::IsCtas);
  CheckOnlyOneWriteSet(q->PredictorFeatures(), QueryPredictor::IsDelete);
  q->PredictorFeatures()[QueryPredictor::IsDelete] = 0;

  // Test IsSpectrumCopy is set.
  q->SetPredictorQueryTypeFeature(nullptr /* QueryDesc */,
                                  true /* is_spectrum_copy */,
                                  false /* is_unload */);
  CheckOnlyOneSelectSet(
      xen::span<double>{q->features(), QueryClassifier::kMaxFeatures},
      QueryPredictor::IsCtas);
  CheckOnlyOneWriteSet(q->PredictorFeatures(), QueryPredictor::IsSpectrumCopy);
  q->PredictorFeatures()[QueryPredictor::IsSpectrumCopy] = 0;

  // Test IsUnload is set.
  q->SetPredictorQueryTypeFeature(nullptr /* QueryDesc */,
                                  false /* is_spectrum_copy */,
                                  true /* is_unload */);
  CheckOnlyOneSelectSet(
      xen::span<double>{q->features(), QueryClassifier::kMaxFeatures},
      QueryPredictor::IsCtas);
  CheckOnlyOneWriteSet(q->PredictorFeatures(), QueryPredictor::IsUnload);

  Query_desc_deleter()(q);
}

void TestPredictor::TestAnalyzeQuery() {
  gconf_query_label = const_cast<char*>("test");
  gconf_enable_wlm = true;
  SetCurrentTransaction(1);
  QueryDescPtr q(Query_desc::Create("", Query_desc::QueryCmdTypeNone));
  PopulatePredictorSqaFeaturesAnalyze(q.get(), false /*is_sampling_select*/,
                                      0 /*rows*/, 10 /*arbitrary width*/);
  for (int i = 0; i < QueryPredictor::kMaxFeatures; ++i) {
    CPPUNIT_ASSERT_DOUBLES_EQUAL(q->features()[i], 0.0,
                                 std::numeric_limits<float>::epsilon());
  }
  for (int i = 0; i < QueryPredictor::kMaxFeatures; ++i) {
    CPPUNIT_ASSERT_DOUBLES_EQUAL(q->PredictorFeatures()[i], 0.0,
                                 std::numeric_limits<float>::epsilon());
  }
  PopulatePredictorSqaFeaturesAnalyze(q.get(), false /*is_sampling_select*/,
                                      10 /*arbitrary rows*/, 0 /*width*/);
  for (int i = 0; i < QueryPredictor::kMaxFeatures; ++i) {
    CPPUNIT_ASSERT_DOUBLES_EQUAL(q->features()[i], 0.0,
                                 std::numeric_limits<float>::epsilon());
  }
  for (int i = 0; i < QueryPredictor::kMaxFeatures; ++i) {
    CPPUNIT_ASSERT_DOUBLES_EQUAL(q->PredictorFeatures()[i], 0.0,
                                 std::numeric_limits<float>::epsilon());
  }
}

/// Checks if the collector contains expected number of small and large samples.
static void CheckCollectorContent(DataCollector& collector,
                                  const int num_expected,
                                  const float expected_feature,
                                  const float expected_output) {
  const float* inputs = collector.GetInputs();
  const float* outputs = collector.GetOutputs();
  const int input_dimension = collector.GetInputDimension();
  const int len = collector.GetInputSize();
  auto num_found = 0;
  for (int i = 0; i < len; ++i) {
    bool found = true;
    for (int j = 0; j < input_dimension; ++j) {
      if (*(inputs + i * input_dimension + j) != expected_feature) {
        found = false;
      }
    }
    if (found && outputs[i] == expected_output) {
      num_found += 1;
    }
  }
  CPPUNIT_ASSERT_EQUAL(num_found, num_expected);
}

void TestPredictor::TestBalancedCollector() {
  // data_window needs to be uneven so if sample doesn't fit, the bucket we are
  // currently adding to is shrunk.
  constexpr int data_window = 11;
  constexpr int num_small = 5;
  constexpr int num_large = data_window - num_small;
  constexpr float small_output = 1.0;
  constexpr float small_feature = small_output + 1.0;
  constexpr float large_output = 1.0e6;
  // Balancing should be on output not input.
  constexpr float large_feature = small_output + 2.0;
  static_assert(num_large >= num_small);
  static_assert(data_window == num_small + num_large);
  static_assert(small_output < large_output);
  // Create a balancing predictor with two buckets, kSmall can't be 0 because
  // that is the default initialization value.
  constexpr int input_dimension = 42;
  auto collector = BalancedCollector({0, large_output}, "Test", input_dimension,
                                     data_window);

  {
    auto input_small = std::vector<DataT>(input_dimension);
    for (int i = 0; i < input_dimension; ++i) {
      input_small[i] = small_feature;
    }
    for (auto i = 0; i < num_small; ++i) {
      collector.AddData(input_small, small_output);
    }
  }
  {
    auto input_large = std::vector<DataT>(input_dimension);
    // We expect the balancing on the output, not the input.
    for (int i = 0; i < input_dimension; ++i) {
      input_large[i] = large_feature;
    }
    // Add more large samples to predictor than can fit, balancing should
    // prevent small samples to be pushed out.
    for (auto i = 0; i < 2 * num_large; ++i) {
      collector.AddData(input_large, large_output);
    }
  }
  {
    // Split collector so that everything should end up in the training
    // collector to check if balancing collector  correctly balances and merges
    // the data.
    auto [training, test] = collector.Split(0);

    CPPUNIT_ASSERT_EQUAL(training->GetInputSize(), data_window);
    CheckCollectorContent(*training, num_small, small_feature, small_output);
    CheckCollectorContent(*training, num_large, large_feature, large_output);

    CPPUNIT_ASSERT_EQUAL(test->GetInputSize(), 0);
  }
  {
    // Split collector so that everything should end up in the test
    // collector to check if balancing collector  correctly balances and merges
    // the data.
    auto [training, test] = collector.Split(100);

    CPPUNIT_ASSERT_EQUAL(training->GetInputSize(), 0);

    CPPUNIT_ASSERT_EQUAL(test->GetInputSize(), data_window);
    CheckCollectorContent(*test, num_small, small_feature, small_output);
    CheckCollectorContent(*test, num_large, large_feature, large_output);
  }
  {
    // Split collector into training and test, checking that all buckets are
    // present in both training and test, splitting happens before merging of
    // buckets.
    // As data_window is uneven, we end up with an implementation specific test.
    // It would be ok when implementation changes if the amounts are swapped by
    // one from larger to smaller.
    auto [training, test] = collector.Split(50);

    CPPUNIT_ASSERT_EQUAL(training->GetInputSize(), 6);
    CheckCollectorContent(*training, 3, small_feature, small_output);
    CheckCollectorContent(*training, 3, large_feature, large_output);

    CPPUNIT_ASSERT_EQUAL(test->GetInputSize(), 5);
    CheckCollectorContent(*test, 2, small_feature, small_output);
    CheckCollectorContent(*test, 3, large_feature, large_output);
  }
}

// We want to test two main features here, deduplication and balancing.
// Test plans as follows to verify balancing:
// 1. We assume the capacity of DDC is 12, the balance bucket,
//    bucket {0, 10e6, 60e6}, i.e., 3 buckets called 1, 2, 3.
// 2. We add 11 UNIQUE queries to DDC, their bucket belongings are [4, 6, 4]
// 3. We added repeated queries to bucket 1.
// 4. We call Split to see if bucket size are [4, 5, 3].
// 5. We verify that there are unique 12 queries in total.

// The 1st UNIQUE query will have (input, output) as ([1, 1, ..., 1], 1e6)
// The 2nd one will be ([2, 2, ..., 2], 1e6), bucket size [2, 0, 0]
// The 5th one will be ([5, 5, ..., 5], 15e6), bucket size [4, 1, 0]
// The 11th one will be ([11, 11, ..., 11], 65e6), bucket size [4, 6, 1]
// The 12th one will be ([12, 12, ..., 12], 65e6), bucket size [4, 6, 2]
// The 13th one will be ([13, 13, ..., 13], 65e6), bucket size [4, 5, 3]
void TestPredictor::TestDedupCollector() {
  constexpr int data_window = 12;

  constexpr int num_bucket1 = 4;
  constexpr int num_bucket2 = 6;
  constexpr int num_bucket3 = 3;

  constexpr float value_bucket1 = 5e6;
  constexpr float value_bucket2 = 15e6;
  constexpr float value_bucket3 = 65e6;

  constexpr int input_dimension = 32;
  xvector<DataT> bucket_range = {0, 10e6, 60e6};
  auto aggr_func_avg = [](const xlist<DataT, MtPredictor>& values) -> DataT {
    return std::accumulate(values.begin(), values.end(), 0.0) /
           static_cast<DataT>(values.size());
  };
  auto collector =
      DeDupCollector(bucket_range, "Test", input_dimension, data_window,
                     10 /* max repetitions per query to save */, aggr_func_avg);
  // Add queries, with repetitions and different bucket belongings to
  // DeDupCollector and test the decompress params of function
  // DeDupCollector::ToDataCollector();
  {
    int feature_value = 1;
    for (auto i = 0; i < num_bucket1; ++i) {
      auto input = std::vector<DataT>(input_dimension, feature_value);
      collector.AddData(input, value_bucket1);
      // repeated queries
      collector.AddData(input, value_bucket1);
      // make sure query input are unique
      feature_value += 1;
    }

    // Now we test decompress argument of DeDupCollector::ToDataCollector()
    // So far, we have num_bucket1 unique queries, each repeated twice.
    // If we allow decompress, we should get all num_bucket1 * 2 queries back.
    DataCollector dc("ContainerForCPPUnitTest", input_dimension,
                     collector.GetInputSize(), true);
    collector.ToDataCollector(&dc, true /*decompress*/, data_window, 1);
    CPPUNIT_ASSERT_EQUAL(num_bucket1 * 2, dc.GetInputSize());

    dc.Reset();
    // If we disallow decompress, we should only get num_bucket1 unique queries.
    collector.ToDataCollector(&dc, false /*not decompress*/, 0, 1);
    CPPUNIT_ASSERT_EQUAL(num_bucket1, dc.GetInputSize());

    for (auto i = 0; i < num_bucket2; ++i) {
      auto input = std::vector<DataT>(input_dimension, feature_value);
      collector.AddData(input, value_bucket2);
      // make sure query input are unique
      feature_value += 1;
    }

    for (auto i = 0; i < num_bucket3; ++i) {
      auto input = std::vector<DataT>(input_dimension, feature_value);
      collector.AddData(input, value_bucket3);
      // make sure query input are unique
      feature_value += 1;
    }
  }

  // Here we test the Split() function, also check the balancing algorithm,
  // i.e., the 5th queries should have been kicked out.
  {
    // Split collector so that everything should end up in the training
    // collector to check if it correctly balances and deduplicate the data.
    auto [training, test] = collector.Split(0, data_window);
    CPPUNIT_ASSERT_EQUAL(data_window, training->GetInputSize());
    CPPUNIT_ASSERT_EQUAL(0, test->GetInputSize());

    std::vector<int> bucket_size(bucket_range.size(), 0);
    const float* outputs = training->GetOutputs();
    const float* inputs = training->GetInputs();
    bool is_5th_exist = false;
    for (auto i = 0; i < training->GetInputSize(); ++i) {
      auto bucket = std::upper_bound(bucket_range.begin(), bucket_range.end(),
                                     outputs[i]);
      if (bucket != std::begin(bucket_range)) {
        bucket = std::prev(bucket);
      }
      int bucket_idx = bucket - bucket_range.begin();
      bucket_size[bucket_idx] += 1;
      // Based on the design of balancing mechanism, the 5th query should have
      // been kicked-out when the 13th query came, here we check it the 5th was
      // actually kicked out.
      is_5th_exist = is_5th_exist || (5 == inputs[i * input_dimension]);
    }
    CPPUNIT_ASSERT_EQUAL(4, bucket_size[0]);
    CPPUNIT_ASSERT_EQUAL(5, bucket_size[1]);
    CPPUNIT_ASSERT_EQUAL(3, bucket_size[2]);
    // The 5th unique query (in the 2nd bucket) should have been kicked out
    // because of the 13th queries, based on LRU algorithm.
    CPPUNIT_ASSERT(!is_5th_exist);
  }
  // Test using all /100% queries for training, nothing for testing, this is
  // a corner case test for Split() function.
  {
    // Split collector so that everything should end up in the test
    // collector to check if it correctly balances and merges the data.
    auto [training, test] = collector.Split(100, data_window);
    CPPUNIT_ASSERT_EQUAL(0, training->GetInputSize());
    CPPUNIT_ASSERT_EQUAL(data_window, test->GetInputSize());
  }
  {
    // Split collector so that everything should end up in the training
    // collector to check if it correctly balances and merges the data.
    auto [training, test] = collector.Split(0, data_window);
    CPPUNIT_ASSERT_EQUAL(data_window, training->GetInputSize());
    CPPUNIT_ASSERT_EQUAL(0, test->GetInputSize());
  }

  // Test min_num_rep param of function DeDupCollector::ToDataCollector(). This
  // functionality is important to build zero-distance kNN with different k.
  {
    DataCollector dc("ContainerForCPPUnitTest", input_dimension,
                     collector.GetInputSize(), true);
    collector.ToDataCollector(&dc, false, 0, 1);
    CPPUNIT_ASSERT_EQUAL((int)collector.GetUniqueQuerySize(),
                         dc.GetInputSize());
    // We reset dc, and then call ToDataCollector again but only
    // Get unique queries which have repeated at least twice.
    // As we only repeat queries in bucket 1, we should get num_bucket1 queries.
    dc.Reset();
    collector.ToDataCollector(&dc, false /* not decompress*/, 0, 2);
    CPPUNIT_ASSERT_EQUAL(num_bucket1, dc.GetInputSize());
  }
  // We test the static method, PreprocessRanges, which pre-process arg ranges
  // supplied to the constructor.
  {
    xvector<DataT> expected_reset = {0};
    // A empty range.
    CPPUNIT_ASSERT(expected_reset == DeDupCollector::PreprocessRanges({}));
    // range not start with 0.
    CPPUNIT_ASSERT(expected_reset == DeDupCollector::PreprocessRanges({5, 6}));
    // range with duplication.
    CPPUNIT_ASSERT(expected_reset ==
                   DeDupCollector::PreprocessRanges({5, 5, 6}));
    // range will be sorted.
    xvector<DataT> expected_sort = {0, 6, 15};
    CPPUNIT_ASSERT(expected_sort ==
                   DeDupCollector::PreprocessRanges({0, 15, 6}));
  }
}

void TestPredictor::TestDedupCollectorWithRounding() {
  constexpr int input_dimension = 32;
  xvector<DataT> bucket_range = {0, 10e6, 60e6};
  constexpr int data_window = 12;

  DataT feature_value1 = 1.234;
  DataT feature_value2 = 1.23412;
  constexpr DataT label_const = 60;
  auto input1 = std::vector<DataT>(input_dimension, feature_value1);
  auto input2 = std::vector<DataT>(input_dimension, feature_value2);
  auto aggr_func_avg = [](const xlist<DataT, MtPredictor>& values) -> DataT {
    return std::accumulate(values.begin(), values.end(), 0.0) /
           static_cast<DataT>(values.size());
  };
  // Case 1, we disable fuzzy match in DDC.
  {
    auto collector = DeDupCollector(bucket_range, "Test", input_dimension,
                                    data_window,
                                    10 /* max repetitions per query to save */,
                                    aggr_func_avg);
    AutoValueRestorer<int> restore_gconf_predictor_ddc_fuzzy_match_mul(
        gconf_predictor_ddc_fuzzy_match_mul,
        gconf_predictor_ddc_fuzzy_match_mul);
    gconf_predictor_ddc_fuzzy_match_mul = -1;
    // We will artificially have two queries where the difference on feature
    // values are less than 2 decimal digit and expect DDC views them
    // differently.
    collector.AddData(input1, label_const);
    collector.AddData(input2, label_const);
    CPPUNIT_ASSERT_EQUAL(size_t(2), collector.GetUniqueQuerySize());
  }
  // Case 2, we enable fuzzy match in DDC.
  {
    auto collector = DeDupCollector(bucket_range, "Test", input_dimension,
                                    data_window,
                                    10 /* max repetitions per query to save */,
                                    aggr_func_avg);
    AutoValueRestorer<int> restore_gconf_predictor_ddc_fuzzy_match_mul(
        gconf_predictor_ddc_fuzzy_match_mul,
        gconf_predictor_ddc_fuzzy_match_mul);
    gconf_predictor_ddc_fuzzy_match_mul = 1000;
    // We will artificially have two queries where the difference on feature
    // values are less than 3 decimal digit and expect DDC views them the same.

    collector.AddData(input1, label_const);
    collector.AddData(input2, label_const);
    CPPUNIT_ASSERT_EQUAL(size_t(1), collector.GetUniqueQuerySize());
  }
}

// We include corner cases in this test as common cases in TestDedupCollector
// are alreay make it very big. The test flow will be described before each
// tests. Test cases are seperated using scope brace.
void TestPredictor::TestDedupCollectorCornerCases() {
  constexpr int data_window = 10;
  constexpr int input_dimension = 32;
  // coner case where there is only one bucket, i.e., no balancing as bucket
  // must start from 0.
  xvector<DataT> bucket_range = {0};
  auto aggr_func_avg = [](const xlist<DataT, MtPredictor>& values) -> DataT {
    return std::accumulate(values.begin(), values.end(), 0.0) /
           static_cast<DataT>(values.size());
  };
  auto collector =
      DeDupCollector(bucket_range, "Test", input_dimension, data_window,
                     10 /* max repetitions per query to save */, aggr_func_avg);
  // As the capacity is 10, and there is only one bucket (no balancing), query
  // will be kicked out as FIFO order, thus the 1st one will be kicked out when
  // the 11th one comes.
  // The 1st UNIQUE query will have (input, output) as ([1, 1, ..., 1], 1)
  // The 2nd one will be ([2, 2, ..., 2], 2)
  // ...
  // The ith one will be ([i, i, ..., i], i)
  {
    for (auto i = 0; i < data_window + 1; ++i) {
      auto input = std::vector<DataT>(input_dimension, i + 1);
      collector.AddData(input, i + 1);
      // repeated queries
      collector.AddData(input, i + 1);
    }
    DataCollector dc("ContainerForCPPUnitTest", input_dimension,
                     collector.GetInputSize(), true);
    collector.ToDataCollector(&dc, false, 0, 1);
    const float* outputs = dc.GetOutputs();
    const float* inputs = dc.GetInputs();
    bool is_1st_exist = false;
    for (auto i = 0; i < dc.GetInputSize(); ++i) {
      is_1st_exist = is_1st_exist ||
                     (1 == inputs[i * input_dimension]) ||
                     (1 == outputs[i]);
    }
    // The 1st unique query should have been kicked out because of the 11th
    // queries, based on LRU algorithm.
    CPPUNIT_ASSERT(!is_1st_exist);
  }
}

// Here we test class RepeatedQueryAttr, the member function GetAverage.
// We did not test other functions because they are already tested in
// TestDedupCollector.
// We set the max_repetition_count as 10 but we will add 15 repetitions to
// RepeatedQueryAttr. Assume we start from 1st to 15th repetition, the value
// will be the index of the repetition, i.e., 1, 2, 3, ..., 15.
// As it will kickout the oldest repetition when full, GetAverage should return
// the average of [6, 7, 8, ..., 15] = 10.5
void TestPredictor::TestRepeatedQueryAttr() {
  constexpr int max_repetition_count = 10;
  auto repeated_attr = RepeatedQueryAttr(max_repetition_count);

  for (auto i = 1; i < 16; ++i) {
    repeated_attr.AddOneRepetition(i, -1);
  }
  CPPUNIT_ASSERT_EQUAL(static_cast<DataT>(10.5), repeated_attr.GetAverage());
}

void TestPredictor::SetUpModelParameters() {
  // Need to fulfill following equation so that also error predictor has test
  // data, model actually gets built:
  // (gconf_predictor_default_error_test_data_percentage *
  // gconf_predictor_default_test_data_percentage *
  // gconf_predictor_default_data_window) / (100 * 100) > 1
  gconf_predictor_default_refresh_rate = 10;
  gconf_predictor_burst_exec_time_refresh_rate = 10;
  gconf_mv_refresh_time_predictor_refresh_rate = 10;
  gconf_predictor_copy_memory_refresh_rate = 10;
  gconf_predictor_default_test_data_percentage = 30;
  gconf_predictor_default_error_test_data_percentage = 10;
  gconf_predictor_default_data_window = 40;
  gconf_predictor_exec_time_data_window = 40;
  gconf_predictor_memory_data_window = 40;
  gconf_predictor_copy_memory_data_window = 40;
  gconf_predictor_local_query_scaling_data_window = 40;
  gconf_predictor_local_copy_scaling_data_window = 40;
  // As we fill the collector halve with small samples, halve with large
  // samples, the window size should be even.
  CPPUNIT_ASSERT(gconf_predictor_default_data_window % 2 == 0);
  CPPUNIT_ASSERT(gconf_predictor_exec_time_data_window % 2 == 0);
  CPPUNIT_ASSERT(gconf_predictor_memory_data_window % 2 == 0);
  CPPUNIT_ASSERT(gconf_predictor_copy_memory_data_window % 2 == 0);
  CPPUNIT_ASSERT(gconf_predictor_local_query_scaling_data_window % 2 == 0);
  CPPUNIT_ASSERT(gconf_predictor_local_copy_scaling_data_window % 2 == 0);
}

void TestPredictor::TrainModel() {
  SetCurrentTransaction(100);
  // Fill the collector with enough data so that later the model actually gets
  // built.
  for (int i = 0; i < gconf_predictor_default_data_window; i += 2) {
    // Small.
    Xen->predictors->execution_time_predictor_->CollectQuery(
        small_query_features_, kSmallOutput);
    Xen->predictors->execution_without_compile_time_predictor_->CollectQuery(
        small_query_features_, kSmallOutput);
    Xen->predictors->burst_exec_time_predictor_->CollectQuery(
        small_query_features_, kSmallOutput);
    Xen->predictors->memory_predictor_->CollectQuery(small_query_features_,
                                                     kSmallOutput);
    Xen->predictors->mv_refresh_exec_time_predictor_->CollectQuery(
        small_mv_features_, kSmallOutput);
    if (gconf_vacuum_predictor_enable) {
      Xen->predictors->vacuum_exec_time_predictor_->CollectQuery(
          small_vac_features_, kSmallOutput);
    }
    if (gconf_local_query_scaling_predictor_enable) {
      Xen->predictors->local_query_scaling_predictor_->CollectQuery(
          small_local_query_scaling_features_, kSmallOutput);
    }
    if (gconf_copy_exec_time_predictor) {
      Xen->predictors->copy_exec_time_predictor_->CollectQuery(
          small_copy_features_,           kSmallOutput);
    }
    if (gconf_copy_query_memory_predictor_enable) {
      Xen->predictors->copy_memory_predictor_->CollectQuery(
          small_copy_memory_features_,
          kSmallOutput);
    }
    // Large.
    Xen->predictors->execution_time_predictor_->CollectQuery(
        large_query_features_, kLargeOutput);
    Xen->predictors->execution_without_compile_time_predictor_->CollectQuery(
        large_query_features_, kLargeOutput);
    Xen->predictors->burst_exec_time_predictor_->CollectQuery(
        large_query_features_, kLargeOutput);
    Xen->predictors->memory_predictor_->CollectQuery(large_query_features_,
                                                     kLargeOutput);
    Xen->predictors->mv_refresh_exec_time_predictor_->CollectQuery(
        large_mv_features_, kLargeOutput);
    if (gconf_vacuum_predictor_enable) {
      Xen->predictors->vacuum_exec_time_predictor_->CollectQuery(
          large_vac_features_, kLargeOutput);
    }
    if (gconf_local_query_scaling_predictor_enable) {
      Xen->predictors->local_query_scaling_predictor_->CollectQuery(
          large_local_query_scaling_features_, kLargeOutput);
    }
    if (gconf_copy_exec_time_predictor) {
      Xen->predictors->copy_exec_time_predictor_->CollectQuery(
          large_copy_features_, kLargeOutput);
    }
    if (gconf_copy_query_memory_predictor_enable) {
      Xen->predictors->copy_memory_predictor_->CollectQuery(
          large_copy_memory_features_, kLargeOutput);
    }
  }
  // Build all the models, for normal predictor as well as for error predictor.
  Xen->predictors->execution_time_predictor_->Build(
      false, gconf_predictor_training_timeout_ms);
  Xen->predictors->execution_time_error_predictor_->Build(
      false, gconf_predictor_training_timeout_ms);
  Xen->predictors->execution_without_compile_time_predictor_->Build(
      false, gconf_predictor_training_timeout_ms);
  Xen->predictors->execution_without_compile_time_error_predictor_->Build(
      false, gconf_predictor_training_timeout_ms);
  Xen->predictors->burst_exec_time_predictor_->Build(
      false, gconf_predictor_training_timeout_ms);
  Xen->predictors->burst_exec_time_error_predictor_->Build(
      false, gconf_predictor_training_timeout_ms);
  Xen->predictors->memory_predictor_->Build(
      false, gconf_predictor_training_timeout_ms);
  Xen->predictors->memory_error_predictor_->Build(
      false, gconf_predictor_training_timeout_ms);
  Xen->predictors->mv_refresh_exec_time_predictor_->Build(
      false, gconf_predictor_training_timeout_ms);
  Xen->predictors->mv_refresh_exec_time_error_predictor_->Build(
      false, gconf_predictor_training_timeout_ms);
  if (gconf_vacuum_predictor_enable) {
    Xen->predictors->vacuum_exec_time_predictor_->Build(
        false, gconf_vacuum_predictor_training_timeout_ms);
    Xen->predictors->vacuum_exec_time_error_predictor_->Build(
        false, gconf_vacuum_predictor_training_timeout_ms);
  }
  if (gconf_local_query_scaling_predictor_enable) {
    Xen->predictors->local_query_scaling_predictor_->Build(
        false, gconf_predictor_training_timeout_ms);
  }
  if (gconf_copy_exec_time_predictor) {
    Xen->predictors->copy_exec_time_predictor_->Build(
        false, gconf_predictor_training_timeout_ms);
    Xen->predictors->copy_exec_time_error_predictor_->Build(
        false, gconf_predictor_training_timeout_ms);
  }
  if (gconf_copy_query_memory_predictor_enable) {
    Xen->predictors->copy_memory_predictor_->Build(
        false, gconf_predictor_training_timeout_ms);
  }
}

void TestPredictor::VerifyModel() {
  // Verify that all the data was loaded.
  CPPUNIT_ASSERT_EQUAL(
      gconf_predictor_exec_time_data_window,
      static_cast<int>(
          Xen->predictors->execution_time_predictor_->GetInputSize()));
  CPPUNIT_ASSERT_EQUAL(
      gconf_predictor_exec_time_data_window *
          gconf_predictor_default_test_data_percentage / 100,
      static_cast<int>(
          Xen->predictors->execution_time_error_predictor_->GetInputSize()));
  CPPUNIT_ASSERT_EQUAL(
      gconf_predictor_default_data_window,
      static_cast<int>(
          Xen->predictors->execution_without_compile_time_predictor_
              ->GetInputSize()));
  CPPUNIT_ASSERT_EQUAL(
      gconf_predictor_default_data_window *
          gconf_predictor_default_test_data_percentage / 100,
      static_cast<int>(
          Xen->predictors->execution_without_compile_time_error_predictor_
              ->GetInputSize()));
  CPPUNIT_ASSERT_EQUAL(
      gconf_predictor_default_data_window,
      static_cast<int>(
          Xen->predictors->burst_exec_time_predictor_->GetInputSize()));
  CPPUNIT_ASSERT_EQUAL(
      gconf_predictor_default_data_window *
          gconf_predictor_default_test_data_percentage / 100,
      static_cast<int>(
          Xen->predictors->burst_exec_time_error_predictor_->GetInputSize()));
  CPPUNIT_ASSERT_EQUAL(
      gconf_predictor_memory_data_window,
      static_cast<int>(Xen->predictors->memory_predictor_->GetInputSize()));
  CPPUNIT_ASSERT_EQUAL(gconf_predictor_memory_data_window *
                           gconf_predictor_default_test_data_percentage / 100,
                       static_cast<int>(Xen->predictors->memory_error_predictor_
                                            ->GetInputSize()));
  CPPUNIT_ASSERT_EQUAL(
      gconf_predictor_default_data_window,
      static_cast<int>(
          Xen->predictors->mv_refresh_exec_time_predictor_->GetInputSize()));
  CPPUNIT_ASSERT_EQUAL(
      gconf_predictor_default_data_window *
          gconf_predictor_default_test_data_percentage / 100,
      static_cast<int>(Xen->predictors->mv_refresh_exec_time_error_predictor_
                           ->GetInputSize()));
  if (gconf_vacuum_predictor_enable) {
    CPPUNIT_ASSERT_EQUAL(
        gconf_predictor_default_data_window,
        static_cast<int>(
            Xen->predictors->vacuum_exec_time_predictor_->GetInputSize()));
    CPPUNIT_ASSERT_EQUAL(
        gconf_predictor_default_data_window *
            gconf_predictor_default_test_data_percentage / 100,
        static_cast<int>(Xen->predictors->vacuum_exec_time_error_predictor_
                             ->GetInputSize()));
  }
  if (gconf_local_query_scaling_predictor_enable) {
    CPPUNIT_ASSERT_EQUAL(
        gconf_predictor_local_query_scaling_data_window,
        static_cast<int>(
            Xen->predictors->local_query_scaling_predictor_->GetInputSize()));
  }
  if (gconf_copy_exec_time_predictor) {
    CPPUNIT_ASSERT_EQUAL(
        gconf_predictor_local_copy_scaling_data_window,
        static_cast<int>(
            Xen->predictors->copy_exec_time_predictor_->GetInputSize()));
    CPPUNIT_ASSERT_EQUAL(
        gconf_predictor_local_copy_scaling_data_window *
            gconf_predictor_default_test_data_percentage / 100,
        static_cast<int>(
            Xen->predictors->copy_exec_time_error_predictor_->GetInputSize()));
  }
  if (gconf_copy_query_memory_predictor_enable) {
    CPPUNIT_ASSERT_EQUAL(
        gconf_predictor_copy_memory_data_window,
        static_cast<int>(
            Xen->predictors->copy_memory_predictor_->GetInputSize()));
  }

  // Verify that all the the model gives correct predictions.
  // Small.
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      kSmallOutput,
      MyPredictor->execution_time_predictor_client_->Predict(
          small_query_features_),
      1.0);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      0.0,
      MyPredictor->execution_time_error_predictor_client_->Predict(
          small_query_features_),
      0.1);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      kSmallOutput,
      MyPredictor->execution_time_without_compile_predictor_client_->Predict(
          small_query_features_),
      1.0);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      0.0,
      MyPredictor->execution_time_without_compile_error_predictor_client_
          ->Predict(small_query_features_),
      0.1);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      kSmallOutput,
      MyPredictor->burst_exec_time_predictor_client_->Predict(
          small_query_features_),
      1.0);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      0.0,
      MyPredictor->burst_exec_time_error_predictor_client_->Predict(
          small_query_features_),
      0.1);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      kSmallOutput,
      MyPredictor->memory_predictor_client_->Predict(
          small_query_features_),
      1.0);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      0.0,
      MyPredictor->memory_error_predictor_client_->Predict(
          small_query_features_),
      0.1);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      kSmallOutput,
      MyPredictor->mv_refresh_exec_time_predictor_client_->Predict(
          small_mv_features_),
      1.0);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      0.0,
      MyPredictor->mv_refresh_exec_time_error_predictor_client_->Predict(
          small_mv_features_),
      0.1);
  if (gconf_vacuum_predictor_enable) {
    CPPUNIT_ASSERT_DOUBLES_EQUAL(
        kSmallOutput,
        MyPredictor->vacuum_exec_time_predictor_client_->Predict(
            small_vac_features_),
        1.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(
        0.0,
        MyPredictor->vacuum_exec_time_error_predictor_client_->Predict(
            small_vac_features_),
        0.1);
  }
  if (gconf_local_query_scaling_predictor_enable) {
    CPPUNIT_ASSERT_DOUBLES_EQUAL(
        kSmallOutput,
        MyPredictor->local_query_scaling_predictor_client_->Predict(
            small_local_query_scaling_features_),
        1.0);
  }
  if (gconf_copy_exec_time_predictor) {
    CPPUNIT_ASSERT_DOUBLES_EQUAL(
        kSmallOutput,
        MyPredictor->copy_exec_time_predictor_client_->Predict(
            small_copy_features_),
        1.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(
        0.0,
        MyPredictor->copy_exec_time_error_predictor_client_->Predict(
            small_copy_features_),
        1.0);
  }
  if (gconf_copy_query_memory_predictor_enable) {
    CPPUNIT_ASSERT_DOUBLES_EQUAL(
        kSmallOutput,
        MyPredictor->copy_memory_predictor_client_->Predict(
            small_copy_memory_features_),
        1.0);
  }
  // Large.
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      kLargeOutput,
      MyPredictor->execution_time_predictor_client_->Predict(
          large_query_features_),
      1.0);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      0.0,
      MyPredictor->execution_time_error_predictor_client_->Predict(
          large_query_features_),
      0.1);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      kLargeOutput,
      MyPredictor->execution_time_without_compile_predictor_client_->Predict(
          large_query_features_),
      1.0);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      0.0,
      MyPredictor->execution_time_without_compile_error_predictor_client_
          ->Predict(large_query_features_),
      0.1);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      kLargeOutput,
      MyPredictor->burst_exec_time_predictor_client_->Predict(
          large_query_features_),
      1.0);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      0.0,
      MyPredictor->burst_exec_time_error_predictor_client_->Predict(
          large_query_features_),
      0.1);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      kLargeOutput,
      MyPredictor->memory_predictor_client_->Predict(
          large_query_features_),
      1.0);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      0.0,
      MyPredictor->memory_error_predictor_client_->Predict(
          large_query_features_),
      0.1);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      kLargeOutput,
      MyPredictor->mv_refresh_exec_time_predictor_client_->Predict(
          large_mv_features_),
      1.0);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      0.0,
      MyPredictor->mv_refresh_exec_time_error_predictor_client_->Predict(
          large_mv_features_),
      0.1);
  if (gconf_vacuum_predictor_enable) {
    CPPUNIT_ASSERT_DOUBLES_EQUAL(
        kLargeOutput,
        MyPredictor->vacuum_exec_time_predictor_client_->Predict(
            large_vac_features_),
        1.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(
        0.0,
        MyPredictor->vacuum_exec_time_error_predictor_client_->Predict(
            large_vac_features_),
        0.1);
  }
  if (gconf_local_query_scaling_predictor_enable) {
    CPPUNIT_ASSERT_DOUBLES_EQUAL(
        kLargeOutput,
        MyPredictor->local_query_scaling_predictor_client_->Predict(
            large_local_query_scaling_features_),
        1.0);
  }
  if (gconf_copy_exec_time_predictor) {
    CPPUNIT_ASSERT_DOUBLES_EQUAL(
        kLargeOutput,
        MyPredictor->copy_exec_time_predictor_client_->Predict(
            large_copy_features_),
        1.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(
        0.0,
        MyPredictor->copy_exec_time_error_predictor_client_->Predict(
            large_copy_features_),
        1.0);
  }
  if (gconf_copy_query_memory_predictor_enable) {
    CPPUNIT_ASSERT_DOUBLES_EQUAL(
        kLargeOutput,
        MyPredictor->copy_memory_predictor_client_->Predict(
            large_copy_memory_features_),
        1.0);
  }
}

void TestPredictor::TestSerializeDeserialize() {
  auto uuid_generator = boost::uuids::random_generator();
  auto tmp_file_uuid = boost::lexical_cast<std::string>(uuid_generator());
  auto tmp_file_path =
      boost::filesystem::temp_directory_path() / (tmp_file_uuid + ".json");
  CPPUNIT_ASSERT(!boost::filesystem::exists(tmp_file_path));
  auto tmp_file_path_string = tmp_file_path.string();

  SetUpModelParameters();

  MyPredictor.reset();
  delete Xen->predictors;
  Xen->predictors = new Predictors();

  TrainModel();

  // Store model with one balancing bucket.
  Xen->predictors->PersistPredictors(tmp_file_path_string.c_str());
  CPPUNIT_ASSERT(boost::filesystem::exists(tmp_file_path));

  // Load model with multiple balancing buckets.
  std::string ranges = "0;" + std::to_string(kLargeOutput);
  const auto ranges_raw_size = ranges.size() + 1;
  auto ranges_raw = std::unique_ptr<char[], shared_mem_deleter<MtPredictor>>{
      static_cast<char*>(xen::allocator::alloc(ranges_raw_size, MtPredictor))};
  strncpy(ranges_raw.get(), ranges.c_str(), ranges_raw_size);
  CPPUNIT_ASSERT(ranges_raw[ranges_raw_size - 1] == '\0');
  gconf_execution_time_predictor_ranges = ranges_raw.get();
  gconf_execution_without_compile_time_predictor_ranges = ranges_raw.get();
  gconf_memory_predictor_ranges = ranges_raw.get();
  gconf_mv_refresh_exec_time_predictor_ranges = ranges_raw.get();
  gconf_burst_exec_time_predictor_ranges = ranges_raw.get();

  MyPredictor.reset();
  delete Xen->predictors;
  Xen->predictors = new Predictors();

  CPPUNIT_ASSERT(Xen->predictors->LoadPredictors(tmp_file_path_string.c_str()));
  MyPredictor = std::make_shared<PredictorClients>(Xen->predictors);

  VerifyModel();

  CPPUNIT_ASSERT(boost::filesystem::remove(tmp_file_path));

  // Store model with multiple balancing buckets.
  Xen->predictors->PersistPredictors(tmp_file_path_string.c_str());
  CPPUNIT_ASSERT(boost::filesystem::exists(tmp_file_path));

  // Load model with one balancing bucket.
  gconf_execution_time_predictor_ranges = nullptr;
  gconf_execution_without_compile_time_predictor_ranges = nullptr;
  gconf_memory_predictor_ranges = nullptr;
  gconf_mv_refresh_exec_time_predictor_ranges = nullptr;
  gconf_burst_exec_time_predictor_ranges = nullptr;

  MyPredictor.reset();
  delete Xen->predictors;
  Xen->predictors = new Predictors();

  CPPUNIT_ASSERT(Xen->predictors->LoadPredictors(tmp_file_path_string.c_str()));
  MyPredictor = std::make_shared<PredictorClients>(Xen->predictors);

  VerifyModel();

  CPPUNIT_ASSERT(boost::filesystem::remove(tmp_file_path));
}

void TestPredictor::TestSerializeDeserializeRawModelBase64() {
  auto uuid_generator = boost::uuids::random_generator();
  auto tmp_file_uuid = boost::lexical_cast<std::string>(uuid_generator());
  auto tmp_file_path =
      boost::filesystem::temp_directory_path() / (tmp_file_uuid + ".json");
  CPPUNIT_ASSERT(!boost::filesystem::exists(tmp_file_path));
  auto tmp_file_path_string = tmp_file_path.string();

  SetUpModelParameters();

  MyPredictor.reset();
  delete Xen->predictors;
  Xen->predictors = new Predictors();

  TrainModel();

  // Store model as raw, as earlier versions would do.
  auto ei = Event_info{EtPredictorSerializationTestRawModel};
  xen_populate_single_event(&ei);
  Xen->predictors->PersistPredictors(tmp_file_path_string.c_str());
  CPPUNIT_ASSERT(boost::filesystem::exists(tmp_file_path));

  MyPredictor.reset();
  delete Xen->predictors;
  Xen->predictors = new Predictors();

  // Load model as raw, tests backwards compatibility.
  CPPUNIT_ASSERT(Xen->predictors->LoadPredictors(tmp_file_path_string.c_str()));
  MyPredictor = std::make_shared<PredictorClients>(Xen->predictors);

  VerifyModel();

  CPPUNIT_ASSERT(boost::filesystem::remove(tmp_file_path));

  UNSET_EVENT(EtPredictorSerializationTestRawModel);

}

void TestPredictor::TestSerializeDeserializeNoBalancing() {
  auto uuid_generator = boost::uuids::random_generator();
  auto tmp_file_uuid = boost::lexical_cast<std::string>(uuid_generator());
  auto tmp_file_path =
      boost::filesystem::temp_directory_path() / (tmp_file_uuid + ".json");
  CPPUNIT_ASSERT(!boost::filesystem::exists(tmp_file_path));
  auto tmp_file_path_string = tmp_file_path.string();

  SetUpModelParameters();

  MyPredictor.reset();
  delete Xen->predictors;
  Xen->predictors = new Predictors();

  TrainModel();

  // Store model as non balancing, as earlier versions would do.
  auto ei = Event_info{EtPredictorSerializationTestNonBalancing};
  xen_populate_single_event(&ei);
  Xen->predictors->PersistPredictors(tmp_file_path_string.c_str());
  CPPUNIT_ASSERT(boost::filesystem::exists(tmp_file_path));

  MyPredictor.reset();
  delete Xen->predictors;
  Xen->predictors = new Predictors();

  // Load model as non balancing, tests backwards compatibility.
  CPPUNIT_ASSERT(Xen->predictors->LoadPredictors(tmp_file_path_string.c_str()));
  MyPredictor = std::make_shared<PredictorClients>(Xen->predictors);

  VerifyModel();

  CPPUNIT_ASSERT(boost::filesystem::remove(tmp_file_path));

  UNSET_EVENT(EtPredictorSerializationTestNonBalancing);

}

void TestPredictor::TestExtractRanges() {
  {
    auto ranges = Predictors::ExtractRanges("");
    CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(1), ranges.size());
    CPPUNIT_ASSERT_EQUAL(0.f, ranges[0]);
  }
  {
    auto ranges = Predictors::ExtractRanges("0");
    CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(1), ranges.size());
    CPPUNIT_ASSERT_EQUAL(0.f, ranges[0]);
  }
  {
    auto ranges = Predictors::ExtractRanges("10");
    CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(1), ranges.size());
    CPPUNIT_ASSERT_EQUAL(10.f, ranges[0]);
  }
  {
    auto ranges = Predictors::ExtractRanges("10;20");
    CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(2), ranges.size());
    CPPUNIT_ASSERT_EQUAL(10.f, ranges[0]);
    CPPUNIT_ASSERT_EQUAL(20.f, ranges[1]);
  }
  {
    auto ranges = Predictors::ExtractRanges("10;20;30");
    CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(3), ranges.size());
    CPPUNIT_ASSERT_EQUAL(10.f, ranges[0]);
    CPPUNIT_ASSERT_EQUAL(20.f, ranges[1]);
    CPPUNIT_ASSERT_EQUAL(30.f, ranges[2]);
  }
  {
    auto ranges = Predictors::ExtractRanges("123;12;1;");
    CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(3), ranges.size());
    CPPUNIT_ASSERT_EQUAL(123.f, ranges[0]);
    CPPUNIT_ASSERT_EQUAL(12.f, ranges[1]);
    CPPUNIT_ASSERT_EQUAL(1.f, ranges[2]);
  }
  {
    auto ranges = Predictors::ExtractRanges("1;12;123");
    CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(3), ranges.size());
    CPPUNIT_ASSERT_EQUAL(1.f, ranges[0]);
    CPPUNIT_ASSERT_EQUAL(12.f, ranges[1]);
    CPPUNIT_ASSERT_EQUAL(123.f, ranges[2]);
  }
  {
    auto ranges = Predictors::ExtractRanges("fubar");
    CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(1), ranges.size());
    CPPUNIT_ASSERT_EQUAL(0.f, ranges[0]);
  }
  {
    auto ranges = Predictors::ExtractRanges("fubar;20");
    CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(1), ranges.size());
    CPPUNIT_ASSERT_EQUAL(0.f, ranges[0]);
  }
  {
    auto ranges = Predictors::ExtractRanges("fubar;20;30");
    CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(1), ranges.size());
    CPPUNIT_ASSERT_EQUAL(0.f, ranges[0]);
  }
  {
    auto ranges = Predictors::ExtractRanges("10;fubar;30");
    CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(1), ranges.size());
    CPPUNIT_ASSERT_EQUAL(0.f, ranges[0]);
  }
  {
    auto ranges = Predictors::ExtractRanges("10;20;fubar");
    CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(1), ranges.size());
    CPPUNIT_ASSERT_EQUAL(0.f, ranges[0]);
  }
}

// This unit test is designed to test the functionality of concatenate queries
// collected by BalancedCollector and DedupCollector. This functionality can be
// enabled/disabled via GUC, so we will test both scenarios.
// We add 10 unique queries via CollectQuery(), and each unique query will
// repeat twice. So, after this, We should have 10 unique queries in DDC
// and 20 queries in BDC.
void TestPredictor::TestConcatDedupAndBalancedCollector() {
  constexpr int data_window = 100;
  constexpr int num_unique_query = 10;
  constexpr double output = 5e6;
  constexpr int input_dimension = 32;
  xvector<DataT> bucket_range = {0, 10e6, 60e6};
  // ValuePredictor will need the GUC below to set DeDupCollector
  AutoValueRestorer<int> restore_gconf_max_repetition_count_in_ddc(
      gconf_predictor_max_repetition_count_in_ddc,
      gconf_predictor_max_repetition_count_in_ddc);
  gconf_predictor_max_repetition_count_in_ddc = 40;
  auto aggr_func_avg = [](const xlist<DataT, MtPredictor>& values) -> DataT {
    return std::accumulate(values.begin(), values.end(), 0.0) /
           static_cast<DataT>(values.size());
  };
  ValuePredictor val_predictor = ValuePredictor(
      "Test", data_window, 20 /* an arbitrary value for capacity */,
      30 /* percent of data for testing */, input_dimension, bucket_range, "",
      aggr_func_avg, gconf_predictor_max_repetition_count_in_ddc,
      -1 /* no cluster size feature */);
  val_predictor.EnableMoE();
  {
    int feature_value = 1;
    for (auto i = 0; i < num_unique_query; ++i) {
      std::vector<DataT> input(input_dimension, feature_value);
      val_predictor.CollectQuery(input, output);
      val_predictor.CollectQuery(input, output);
      // make sure query input are unique
      feature_value += 1;
    }
  }
  // If concat is enabled, we split queries in BDC into train and test, then
  // we get unique queries from DDC and concat with train. So, we should see
  // 20 (BDC) * 0.3 = 6 in test, and 20 * 0.7 + 10 (DDC) = 24 in training.
  // The concatenation process happedn in ValuePredictor where it has access to
  // both BDC and DDC.
  {
    AutoValueRestorer<bool> predictor_concat_bdc_ddc(
      gconf_predictor_concat_bdc_ddc, true);
    gconf_predictor_concat_bdc_ddc = true;
    auto [training, test] = val_predictor.SplitTrainTest();
    CPPUNIT_ASSERT_EQUAL(24, training->GetInputSize());
    CPPUNIT_ASSERT_EQUAL(6, test->GetInputSize());

    // Test set should have all 10 unique queries.
    std::set<int> set_to_count_unique = {};
    const float* inputs = training->GetInputs();
    for (auto i = 0; i < training->GetInputSize(); ++i) {
      set_to_count_unique.insert(inputs[i * input_dimension]);
    }
    CPPUNIT_ASSERT_EQUAL(num_unique_query, (int)set_to_count_unique.size());
  }
  // If concat is disabled, we will call DeDupCollector::Split to split queries
  // stored in DDC into two DataCollector objects. We should see 20 * 0.7 = 14
  // queries in training and the rest 6 queris in testing set. Here we can see
  // 20 queries in total because we get 10 unique queries plus 10 decompressed
  // repetitions. More details about decompression are avalible in
  // the comments of DeDupCollector::Split().
  {
    AutoValueRestorer<bool> predictor_concat_bdc_ddc(
      gconf_predictor_concat_bdc_ddc, true);
    gconf_predictor_concat_bdc_ddc = false;
    auto [training, test] = val_predictor.SplitTrainTest();
    CPPUNIT_ASSERT_EQUAL(14, training->GetInputSize());
    CPPUNIT_ASSERT_EQUAL(6, test->GetInputSize());
  }
}

void TestPredictor::TestDataCollectorDeserializeDataWithMissingFeatures() {
  // Serialize data collector
  const int input_dimension = 2;
  DataCollector collector("DataCollectorSerialize", input_dimension,
                          gconf_predictor_default_data_window);

  collector.AddData(std::array{1.0f, 2.0f}, 1.0);
  collector.AddData(std::array{3.0f, 4.0f}, 2.0);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  collector.Serialize(writer, 1 /* no effect */);

  // deserialize new collector expecting more features
  const int num_extra_features = 2;
  DataCollector newCollector("TestDeserializeCollector",
                             input_dimension + num_extra_features,
                             gconf_predictor_default_data_window);

  rapidjson::Document doc;
  doc.Parse<0>(buffer.GetString());
  CPPUNIT_ASSERT(newCollector.Deserialize(doc) ==
                 DeserializationResult::kDetectedFeatureChange);

  // Assert deserialized data is as expected
  CPPUNIT_ASSERT_EQUAL(collector.GetInputSize(), newCollector.GetInputSize());

  const DataT* originalFeatures = collector.GetInputs();
  const DataT* newFeatures = newCollector.GetInputs();

  int pos_original = 0;
  int pos_new = 0;
  for (int i = 0; i < collector.GetInputSize(); i++) {
    for (int j = 0; j < input_dimension; j++) {
      CPPUNIT_ASSERT_EQUAL(originalFeatures[pos_original++],
                           newFeatures[pos_new++]);
    }

    for (int j = 0; j < num_extra_features; j++) {
      CPPUNIT_ASSERT_EQUAL(kMissingValue, newFeatures[pos_new++]);
    }
  }
}

void TestPredictor::TestDeDupCollectorDeserializeDataWithMissingFeatures() {
  const int input_dimension = 2;
  const xvector<DataT> ranges{0, 2, 4};
  const int maxRepetitionCount = 10;
  auto aggr_func_avg = [](const xlist<DataT, MtPredictor>& values) -> DataT {
    return std::accumulate(values.begin(), values.end(), 0.0) /
           static_cast<DataT>(values.size());
  };
  // Serialize data collector
  DeDupCollector dedupCollector(
      ranges, "TestSerializeCollector", input_dimension,
      gconf_predictor_default_data_window, maxRepetitionCount, aggr_func_avg);

  dedupCollector.AddData(std::array{1.0f, 2.0f}, 1.0);
  dedupCollector.AddData(std::array{3.0f, 4.0f}, 2.0);
  dedupCollector.AddData(std::array{3.0f, 4.0f}, 2.5);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  writer.StartObject();
  dedupCollector.Serialize(writer);
  writer.EndObject();

  // deserialize new collector expecting more features
  const int num_extra_features = 2;
  DeDupCollector newDedupCollector(
      ranges, "TestDeserializeCollector", input_dimension + num_extra_features,
      gconf_predictor_default_data_window, maxRepetitionCount, aggr_func_avg);

  rapidjson::Document doc;
  doc.Parse<0>(buffer.GetString());
  CPPUNIT_ASSERT(newDedupCollector.Deserialize(doc) ==
                 DeserializationResult::kDetectedFeatureChange);

  // convert dedup collectors to data collectors
  const bool decompress = true;
  const bool max_num_with_dec = 100;
  const bool min_num_rep = 1;

  DataCollector collector("Collector", input_dimension,
                          gconf_predictor_default_data_window);
  dedupCollector.ToDataCollector(&collector, decompress, max_num_with_dec,
                                 min_num_rep);

  DataCollector newCollector("NewCollector",
                             input_dimension + num_extra_features,
                             gconf_predictor_default_data_window);
  newDedupCollector.ToDataCollector(&newCollector, decompress, max_num_with_dec,
                                    min_num_rep);

  // Assert deserialized data is as expected
  CPPUNIT_ASSERT_EQUAL(collector.GetInputSize(), newCollector.GetInputSize());
  const DataT* originalFeatures = collector.GetInputs();
  const DataT* newFeatures = newCollector.GetInputs();

  int pos_original = 0;
  int pos_new = 0;
  for (int i = 0; i < collector.GetInputSize(); i++) {
    for (int j = 0; j < input_dimension; j++) {
      CPPUNIT_ASSERT_EQUAL(originalFeatures[pos_original++],
                           newFeatures[pos_new++]);
    }

    for (int j = 0; j < num_extra_features; j++) {
      CPPUNIT_ASSERT_EQUAL(kMissingValue, newFeatures[pos_new++]);
    }
  }
}

void TestPredictor::TestDataCollectorDeserializeDataWithExtraFeatures() {
  // Serialize data collector
  const int input_dimension = 4;
  DataCollector collector("DataCollectorSerialize", input_dimension,
                          gconf_predictor_default_data_window);
  collector.AddData(std::array{1.0f, 2.0f, 3.0f, 4.0f}, 1.0);
  collector.AddData(std::array{5.0f, 6.0f, 7.0f, 8.0f}, 2.0);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  collector.Serialize(writer, 1 /* no effect */);

  // deserialize new collector expecting more features
  const int new_input_dimension = 2;
  DataCollector newCollector("TestDeserializeCollector", new_input_dimension,
                             gconf_predictor_default_data_window);

  rapidjson::Document doc;
  doc.Parse<0>(buffer.GetString());
  CPPUNIT_ASSERT(newCollector.Deserialize(doc) ==
                 DeserializationResult::kDetectedFeatureChange);

  // Assert deserialized data is as expected
  CPPUNIT_ASSERT_EQUAL(collector.GetInputSize(), newCollector.GetInputSize());

  const DataT* originalFeatures = collector.GetInputs();
  const DataT* newFeatures = newCollector.GetInputs();

  int pos_original = 0;
  int pos_new = 0;
  for (int i = 0; i < collector.GetInputSize(); i++) {
    for (int j = 0; j < input_dimension; j++) {
      const DataT originalValue = originalFeatures[pos_original++];
      if (j < new_input_dimension) {
        CPPUNIT_ASSERT_EQUAL(originalValue, newFeatures[pos_new++]);
      }
    }
  }
}

void TestPredictor::TestDeDupCollectorDeserializeDataWithExtraFeatures() {
  const int input_dimension = 4;
  const xvector<DataT> ranges{0, 2, 4};
  const int maxRepetitionCount = 10;
  auto aggr_func_avg = [](const xlist<DataT, MtPredictor>& values) -> DataT {
    return std::accumulate(values.begin(), values.end(), 0.0) /
           static_cast<DataT>(values.size());
  };
  // Serialize data collector
  DeDupCollector dedupCollector(
      ranges, "TestSerializeCollector", input_dimension,
      gconf_predictor_default_data_window, maxRepetitionCount, aggr_func_avg);

  dedupCollector.AddData(std::array{1.0f, 2.0f, 3.0f, 4.0f}, 1.0);
  dedupCollector.AddData(std::array{1.0f, 2.0f, 3.0f, 4.0f}, 2.0);
  dedupCollector.AddData(std::array{5.0f, 6.0f, 7.0f, 8.0f}, 2.5);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  writer.StartObject();
  dedupCollector.Serialize(writer);
  writer.EndObject();

  // deserialize new collector expecting more features
  const int new_input_dimension = 2;
  DeDupCollector newDedupCollector(
      ranges, "TestDeserializeCollector", new_input_dimension,
      gconf_predictor_default_data_window, maxRepetitionCount, aggr_func_avg);

  rapidjson::Document doc;
  doc.Parse<0>(buffer.GetString());
  CPPUNIT_ASSERT(newDedupCollector.Deserialize(doc) ==
                 DeserializationResult::kDetectedFeatureChange);

  // convert dedup collectors to data collectors
  const bool decompress = true;
  const bool max_num_with_dec = 100;
  const bool min_num_rep = 1;

  DataCollector collector("Collector", input_dimension,
                          gconf_predictor_default_data_window);
  dedupCollector.ToDataCollector(&collector, decompress, max_num_with_dec,
                                 min_num_rep);

  DataCollector newCollector("NewCollector", new_input_dimension,
                             gconf_predictor_default_data_window);
  newDedupCollector.ToDataCollector(&newCollector, decompress, max_num_with_dec,
                                    min_num_rep);

  // Assert deserialized data is as expected
  CPPUNIT_ASSERT_EQUAL(collector.GetInputSize(), newCollector.GetInputSize());
  const DataT* originalFeatures = collector.GetInputs();
  const DataT* newFeatures = newCollector.GetInputs();

  int pos_original = 0;
  int pos_new = 0;
  for (int i = 0; i < collector.GetInputSize(); i++) {
    for (int j = 0; j < input_dimension; j++) {
      const DataT originalValue = originalFeatures[pos_original++];
      if (j < new_input_dimension) {
        CPPUNIT_ASSERT_EQUAL(originalValue, newFeatures[pos_new++]);
      }
    }
  }
}

void TestPredictor::TestDataCollectorDeserializeEmptyData() {
  DataCollector collector("Collector", 10, gconf_predictor_default_data_window);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  collector.Serialize(writer, 1 /* no effect */);

  rapidjson::Document doc;
  doc.Parse<0>(buffer.GetString());

  DataCollector newCollectorWithFewerFeatures(
      "NewCollectorFewer", 5, gconf_predictor_default_data_window);
  CPPUNIT_ASSERT(newCollectorWithFewerFeatures.Deserialize(doc) ==
                 DeserializationResult::kDetectedFeatureChange);

  DataCollector newCollectorWithExtraFeatures(
      "NewCollectorExtra", 15, gconf_predictor_default_data_window);
  CPPUNIT_ASSERT(newCollectorWithExtraFeatures.Deserialize(doc) ==
                 DeserializationResult::kDetectedFeatureChange);
}

void TestPredictor::TestDeDupCollectorDeserializeEmptyData() {
  const xvector<DataT> ranges{0, 2, 4};
  const int maxRepetitionCount = 10;
  auto aggr_func_avg = [](const xlist<DataT, MtPredictor>& values) -> DataT {
    return std::accumulate(values.begin(), values.end(), 0.0) /
           static_cast<DataT>(values.size());
  };
  DeDupCollector collector(ranges, "Collector", 10,
                           gconf_predictor_default_data_window,
                           maxRepetitionCount, aggr_func_avg);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  writer.StartObject();
  collector.Serialize(writer);
  writer.EndObject();

  rapidjson::Document doc;
  doc.Parse<0>(buffer.GetString());

  DeDupCollector newCollectorWithFewerFeatures(
      ranges, "NewCollectorFewer", 5, gconf_predictor_default_data_window,
      maxRepetitionCount, aggr_func_avg);
  CPPUNIT_ASSERT(newCollectorWithFewerFeatures.Deserialize(doc) ==
                 DeserializationResult::kDetectedFeatureChange);

  DeDupCollector newCollectorWithExtraFeatures(
      ranges, "NewCollectorExtra", 15, gconf_predictor_default_data_window,
      maxRepetitionCount, aggr_func_avg);
  CPPUNIT_ASSERT(newCollectorWithExtraFeatures.Deserialize(doc) ==
                 DeserializationResult::kDetectedFeatureChange);
}

void TestPredictor::TestValuePredictorNeedsRebuildOnFeatureChange() {
  // serialize ValuePredictor trained over dummy data
  constexpr int feature_size = 2;
  const int data_window = 100;  // a big number so all data points are used
  const int refresh_rate = 1;   // ensure Build will trigger
  const int test_data_percentage = 50;  // ensure test set is not empty
  const int max_repetition_count = 40;
  const xvector<DataT> ranges = {0};    // use single bucket for simplicity
  auto aggr_func_avg = [](const xlist<DataT, MtPredictor>& values) -> DataT {
    return std::accumulate(values.begin(), values.end(), 0.0) /
           static_cast<DataT>(values.size());
  };
  ValuePredictor predictor("TestPredictor", data_window, refresh_rate,
                           test_data_percentage, feature_size, ranges, "",
                           aggr_func_avg, max_repetition_count,
                           -1 /* no cluster size feature */);
  predictor.CollectQuery(std::array<DataT, feature_size>{0, 1}, 5);
  predictor.CollectQuery(std::array<DataT, feature_size>{2, 3}, 15);
  predictor.CollectQuery(std::array<DataT, feature_size>{4, 5}, 25);
  predictor.CollectQuery(std::array<DataT, feature_size>{6, 7}, 35);

  predictor.Build(false, 1000);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  predictor.Serialize(writer);

  // deserialize previous predictor while expecting extra features
  const int new_feature_size = feature_size + 2;
  ValuePredictor newPredictor("NewTestPredictor", data_window, refresh_rate,
                              test_data_percentage, new_feature_size, ranges,
                              "", aggr_func_avg, max_repetition_count,
                              -1 /* no cluster size feature */);

  rapidjson::Document doc;
  doc.Parse<0>(buffer.GetString());
  CPPUNIT_ASSERT(newPredictor.Deserialize(doc));

  // assert deserialized predictor has empty dump and is marked for rebuild,
  // even though no new queries were collected
  std::unique_ptr<QueryPredictorCtxt> ctxt(newPredictor.GetContext());
  CPPUNIT_ASSERT(newPredictor.IsEmpty(ctxt.get()));
  CPPUNIT_ASSERT(newPredictor.ShouldRebuildXGB());

  // assert model no longer needs rebuild after Build is called
  newPredictor.Build(false, 1000);
  CPPUNIT_ASSERT(!newPredictor.ShouldRebuildXGB());
}

void TestPredictor::TestEnsembleScalingPrediction() {
  const double EPSILON = 0.001;
  const double exec_time_pred_us = 50e6;
  SetCurrentTransaction(1);
  const int matched_cluster_size_1 = 8;
  QueryDescPtr q(Query_desc::Create("", Query_desc::QueryCmdTypeNone));
  q->SetPredictedExecTime(exec_time_pred_us, 0 /* no error bound*/);
  q->predictions_.pretrained_exec_time_sec_[4] = 100;
  q->predictions_.pretrained_exec_time_sec_[8] = 60;
  q->predictions_.pretrained_exec_time_sec_[16] = 40;
  q->predictions_.pretrained_exec_time_sec_[32] = 25;
  q->predictions_.pretrained_exec_time_sec_[64] = 18;

  q->predictions_.local_exec_time_sec_[5] = 30;
  // Case 1, we do not have predictions for the same scale from both pretrained
  // and local scaling predictor, thus no ensemble will happen.
  {
    std::unordered_map<int, double> revised_pred;
    double ensemble_wieght;
    std::tie(revised_pred, ensemble_wieght) =
      EnsembleLocalAndPretrainedScalingPredictor(
        q->predictions_.pretrained_exec_time_sec_,
        q->predictions_.local_exec_time_sec_);
    // As main cluster size is 5, we cannot find paired prediction of Mr and
    // Mg. so, nothing will change.
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ensemble_wieght, 1.0, EPSILON);
    CPPUNIT_ASSERT(q->predictions_.pretrained_exec_time_sec_.size() ==
                   revised_pred.size());
    for (auto const& pair : revised_pred) {
      CPPUNIT_ASSERT(q->predictions_.pretrained_exec_time_sec_.count(
              pair.first) > 0);
      CPPUNIT_ASSERT_DOUBLES_EQUAL(revised_pred[pair.first],
          q->predictions_.pretrained_exec_time_sec_[pair.first], EPSILON);
    }
  }
  // Case 2, there is one pair of predictions, made by local and pre-trained
  // predictors, for the same scale. Thus ensemble will happen.
  // In this test, the optimized weight should be 50 / 60.
  {
    const double desired_weight = 50. / 60.;
    // Setup guardrail.
    gconf_predictor_scaling_ensemble_clip_min = 0.1;
    gconf_predictor_scaling_ensemble_clip_max = 10.0;
    q->predictions_.local_exec_time_sec_[matched_cluster_size_1] =
        exec_time_pred_us / US_PER_SEC;

    std::unordered_map<int, double> revised_pred;
    double ensemble_wieght;
    std::tie(revised_pred, ensemble_wieght) =
      EnsembleLocalAndPretrainedScalingPredictor(
        q->predictions_.pretrained_exec_time_sec_,
        q->predictions_.local_exec_time_sec_);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ensemble_wieght, desired_weight, EPSILON);
    CPPUNIT_ASSERT(q->predictions_.pretrained_exec_time_sec_.size() ==
                   revised_pred.size());
    for (auto const& pair : revised_pred) {
      CPPUNIT_ASSERT(q->predictions_.pretrained_exec_time_sec_.count(
          pair.first) > 0);
      if (matched_cluster_size_1 == pair.first) {
        CPPUNIT_ASSERT_DOUBLES_EQUAL(revised_pred[pair.first],
          exec_time_pred_us / US_PER_SEC, EPSILON);
      } else {
        CPPUNIT_ASSERT_DOUBLES_EQUAL(revised_pred[pair.first],
          q->predictions_.pretrained_exec_time_sec_[pair.first] *
              desired_weight,
          EPSILON);
      }
    }
  }

  // Test the GuardRail. In this test, we test GuardRail will clip computed
  // weight to a pre-set value.
  // In the following two scenarios, the optimized weight should be 50 / 60,
  // we will test both upper and lower bound.
  {
    const int matched_cluster_size = 8;
    gconf_predictor_scaling_ensemble_clip_min = 1;
    gconf_predictor_scaling_ensemble_clip_max = 10.0;
    // as the computed weight will be 50. / 60., it is less than the clip_min
    // so the final ensemble_weight should be clamp to
    // gconf_predictor_scaling_ensemble_clip_min.
    const double desired_weight = gconf_predictor_scaling_ensemble_clip_min;

    std::unordered_map<int, double> revised_pred;
    double ensemble_wieght;
    std::tie(revised_pred, ensemble_wieght) =
      EnsembleLocalAndPretrainedScalingPredictor(
        q->predictions_.pretrained_exec_time_sec_,
        q->predictions_.local_exec_time_sec_);

    CPPUNIT_ASSERT_DOUBLES_EQUAL(ensemble_wieght, desired_weight, EPSILON);
    CPPUNIT_ASSERT(q->predictions_.pretrained_exec_time_sec_.size() ==
                   revised_pred.size());

    for (auto const& pair : revised_pred) {
      CPPUNIT_ASSERT(q->predictions_.pretrained_exec_time_sec_.count(
          pair.first) > 0);
      if (matched_cluster_size == pair.first) {
        CPPUNIT_ASSERT_DOUBLES_EQUAL(revised_pred[pair.first],
          exec_time_pred_us / US_PER_SEC, EPSILON);
      } else {
        CPPUNIT_ASSERT_DOUBLES_EQUAL(revised_pred[pair.first],
          q->predictions_.pretrained_exec_time_sec_[pair.first] *
              desired_weight,
          EPSILON);
      }
    }
  }
  // Test upper bound.
  {
    const int matched_cluster_size = 8;
    gconf_predictor_scaling_ensemble_clip_min = 0.1;
    gconf_predictor_scaling_ensemble_clip_max = 0.5;
    // as the computed weight will be 50. / 60., it is less than the clip_max
    // so the final ensemble_weight should be clamp to
    // gconf_predictor_scaling_ensemble_clip_max.
    const double desired_weight = gconf_predictor_scaling_ensemble_clip_max;

    std::unordered_map<int, double> revised_pred;
    double ensemble_wieght;
    std::tie(revised_pred, ensemble_wieght) =
      EnsembleLocalAndPretrainedScalingPredictor(
        q->predictions_.pretrained_exec_time_sec_,
        q->predictions_.local_exec_time_sec_);

    CPPUNIT_ASSERT_DOUBLES_EQUAL(ensemble_wieght, desired_weight, EPSILON);
    CPPUNIT_ASSERT(q->predictions_.pretrained_exec_time_sec_.size() ==
                   revised_pred.size());

    for (auto const& pair : revised_pred) {
      CPPUNIT_ASSERT(q->predictions_.pretrained_exec_time_sec_.count(
          pair.first) > 0);
      if (matched_cluster_size == pair.first) {
        CPPUNIT_ASSERT_DOUBLES_EQUAL(revised_pred[pair.first],
          exec_time_pred_us / US_PER_SEC, EPSILON);
      } else {
        CPPUNIT_ASSERT_DOUBLES_EQUAL(revised_pred[pair.first],
          q->predictions_.pretrained_exec_time_sec_[pair.first] *
              desired_weight,
          EPSILON);
      }
    }
  }
  // Test HitKNN API
  // here we reuse the Query_desc created at the begining of this test.
  // as we have never set predictions_.observed_exec_time_sec_, we expect the
  // API returns false at the begining, then we set one scale and expect true
  // only for the cluster size of setting.
  {
    const int matched_cluster_size = 8;
    std::vector<int>scales{4, 8, 16, 32, 64};
    for (const auto scale : scales) {
      CPPUNIT_ASSERT(q->predictions_.HitKNN(scale) == false);
    }
    q->predictions_.observed_exec_time_sec_[matched_cluster_size] = 50;
    for (const auto scale : scales) {
      CPPUNIT_ASSERT((q->predictions_.HitKNN(scale) == false) ||
                     (scale == matched_cluster_size));
    }
  }
  // Test a case where local scaling predictor can make predictiosn for more
  // than 1 scales.
  {
    const double exec_time_pred_us_2 = 20e6;
    const int matched_cluster_size_2 = 32;
    q->predictions_.local_exec_time_sec_[matched_cluster_size_2] =
            exec_time_pred_us_2 / US_PER_SEC;
    const double desired_weight = (50. * 60. + 20. * 25.) /
                                  (std::pow(60., 2) + std::pow(25., 2));
    gconf_predictor_scaling_ensemble_clip_min = 0.1;
    gconf_predictor_scaling_ensemble_clip_max = 10.;
    std::unordered_map<int, double> revised_pred;
    double ensemble_wieght;
    std::tie(revised_pred, ensemble_wieght) =
      EnsembleLocalAndPretrainedScalingPredictor(
        q->predictions_.pretrained_exec_time_sec_,
        q->predictions_.local_exec_time_sec_);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ensemble_wieght, desired_weight, EPSILON);
    CPPUNIT_ASSERT(q->predictions_.pretrained_exec_time_sec_.size() ==
                   revised_pred.size());

    for (auto const& pair : revised_pred) {
      // The pre-trained predictions must exists.
      CPPUNIT_ASSERT(q->predictions_.pretrained_exec_time_sec_.count(
          pair.first) > 0);
      if (matched_cluster_size_1 == pair.first) {
        // when cluster size matched, we should expect a over-written prediction
        // from the local scaling predictor.
        CPPUNIT_ASSERT_DOUBLES_EQUAL(revised_pred[pair.first],
          exec_time_pred_us / US_PER_SEC, EPSILON);
      } else if (matched_cluster_size_2 == pair.first) {
        CPPUNIT_ASSERT_DOUBLES_EQUAL(revised_pred[pair.first],
          exec_time_pred_us_2 / US_PER_SEC, EPSILON);
      } else {
        // when cluster size is not matched, we should expect a revised
        // prediction by pre-trained predictor from the ensemble algorithm.
        CPPUNIT_ASSERT_DOUBLES_EQUAL(revised_pred[pair.first],
          q->predictions_.pretrained_exec_time_sec_[pair.first] *
              desired_weight,
          EPSILON);
      }
    }
  }
}

void TestPredictor::TestHyperParameterParse() {
  bool local_predictor_enable_moe_memory = gconf_predictor_enable_moe_memory;
  gconf_predictor_enable_moe_memory = true;
  char* local_hyper_parameter = gconf_predictor_memory_moe_param_override;
  // Invalid json provided, it will fail to parse and use default value.
  gconf_predictor_memory_moe_param_override = "{\"quantile\"}";
  bool isParseSuccess = Predictors::IsJsonParseSuccessful(
      gconf_predictor_memory_moe_param_override);
  CPPUNIT_ASSERT(!isParseSuccess);
  // Json parse failed, so we use default value for memory MoE, which is
  // "{\"quantile\": \"0.98\", \"objective\":\"reg:quantilesquarederror\"}".
  MyPredictor.reset();
  delete Xen->predictors;
  Xen->predictors = new Predictors();
  bool isInitSuccess = false;
  if (Xen->predictors->memory_predictor_.get()->hyper_param_json_.find(
          std::string("0.98")) != std::string::npos) {
    isInitSuccess = true;
  }
  CPPUNIT_ASSERT(isInitSuccess);

  // Valid json provided, it will parse successfully.
  gconf_predictor_memory_moe_param_override = "{\"quantile\": \"1.0\"}";
  isParseSuccess = Predictors::IsJsonParseSuccessful(
      gconf_predictor_memory_moe_param_override);
  CPPUNIT_ASSERT(isParseSuccess);
  MyPredictor.reset();
  delete Xen->predictors;
  Xen->predictors = new Predictors();
  isInitSuccess = false;
  if (Xen->predictors->memory_predictor_.get()->hyper_param_json_.find(
          std::string("1.0")) != std::string::npos) {
    isInitSuccess = true;
  }
  CPPUNIT_ASSERT(isInitSuccess);
  // Restore Memory predictor related GUCs.
  gconf_predictor_memory_moe_param_override = local_hyper_parameter;
  gconf_predictor_enable_moe_memory = local_predictor_enable_moe_memory;
}

void TestPredictor::TestErrorPredictionWithAndWithoutMoe() {
  SetCurrentTransaction(1);
  QueryDescPtr q(Query_desc::Create("", Query_desc::QueryCmdTypeNone));

  // If MoE is enabled, return 0 irrespectively of squared error value
  CPPUNIT_ASSERT_EQUAL(0., q->GetErrorPrediction(100., true));
  CPPUNIT_ASSERT_EQUAL(0., q->GetErrorPrediction(-100., true));
  CPPUNIT_ASSERT_EQUAL(0., q->GetErrorPrediction(0., true));

  // If MoE is disabled and value is negative, we return it unchanged
  CPPUNIT_ASSERT_EQUAL(-10., q->GetErrorPrediction(-10., false));

  // If MoE is disabled and value is positive, return the square root
  CPPUNIT_ASSERT_EQUAL(0., q->GetErrorPrediction(0., false));
  CPPUNIT_ASSERT_EQUAL(8., q->GetErrorPrediction(64., false));
}

void TestPredictor::TestScalingPredictorFeaturesAreCorrectlyPopulated() {
  SetCurrentTransaction(1);
  QueryDescPtr q(Query_desc::Create("", Query_desc::QueryCmdTypeNone));

  auto scaling_features = q->QueryScalingPredictorFeatures();
  DataT expected_scaling_features[QueryScalingPredictor::kMaxFeatures]{};

  // Populate PredictorFeatures with values 0, 1, 2, ...
  auto predictor_features = q->PredictorFeatures();
  int pos = 0;
  for (int i = 0; i < QueryPredictor::kMaxFeatures; ++i) {
    predictor_features[i] = static_cast<DataT>(i);
    expected_scaling_features[pos++] = predictor_features[i];
  }

  // Populate SpectrumFeatures with values 0, -1, -2, ...
  auto spectrum_features = q->SpectrumFeatures();
  for (int i = 0; i < dory::SpectrumPredictorFeatures::kMaxFeatures; ++i) {
    spectrum_features[i] = static_cast<DataT>(-i);
    if (i != dory::SpectrumPredictorFeatures::SpectrumLocation) {
      // SpectrumLocation is not part of scaling features.
      expected_scaling_features[pos++] = spectrum_features[i];
    }
  }

  // Verify feature population for non-spectrum queries.
  // In this case, all spectrum features apart from IsSpectrum must be missing.
  expected_scaling_features[QueryScalingPredictor::IsSpectrum] = 0.;
  spectrum_features[dory::SpectrumPredictorFeatures::IsSpectrum] = 0.;
  q->PopulateQueryScalingPredictorFeatures();

  for (int i = 0; i < QueryScalingPredictor::ClusterSize; ++i) {
    if (i <= QueryScalingPredictor::IsSpectrum) {
      CPPUNIT_ASSERT_EQUAL(scaling_features[i], expected_scaling_features[i]);
    } else {
      CPPUNIT_ASSERT_EQUAL(scaling_features[i], kMissingValue);
    }
  }

  // Verify feature population for spectrum queries.
  // In this case, no feature should be missing.
  expected_scaling_features[QueryScalingPredictor::IsSpectrum] = 1.;
  spectrum_features[dory::SpectrumPredictorFeatures::IsSpectrum] = 1.;
  q->PopulateQueryScalingPredictorFeatures();
  for (int i = 0; i < QueryScalingPredictor::ClusterSize; ++i) {
    CPPUNIT_ASSERT_EQUAL(scaling_features[i], expected_scaling_features[i]);
  }
}

void TestPredictor::TestCopyQueryPredictorFileSizeFuzzyMatch() {
  SetCurrentTransaction(1);
  QueryDescPtr query(Query_desc::Create("", Query_desc::QueryCmdTypeCopy));
  auto copy_features = query->CopyPredictorFeatures();
  DataT
      expected_features_disable_rounding[QueryScalingPredictor::kMaxFeatures]{};
  DataT
      expected_features_enable_rounding[QueryScalingPredictor::kMaxFeatures]{};
  static_assert(CopyExecTimePredictor::MaxFileSize ==
                CopyExecTimePredictor::MinFileSize - 1);
  static_assert(CopyExecTimePredictor::MinFileSize ==
                CopyExecTimePredictor::P10FileSize - 1);
  static_assert(CopyExecTimePredictor::P10FileSize ==
                CopyExecTimePredictor::P50FileSize - 1);
  static_assert(CopyExecTimePredictor::P50FileSize ==
                CopyExecTimePredictor::P90FileSize - 1);
  // Only features related to file size will be rounded and need to be tested
  // here.
  for (int i = CopyExecTimePredictor::MaxFileSize;
       i <= CopyExecTimePredictor::P90FileSize; ++i) {
    // Populate file size features start with MinFileSize as 100 + 7.
    copy_features[i] = static_cast<DataT>(100 + i);
    expected_features_disable_rounding[i] = static_cast<DataT>(100 + i);
    expected_features_enable_rounding[i] = 100;
  }
  // Disable fuzzy match for copy execution time predictor.
  gconf_predictor_fuzzy_match_copy_exec_time = -1;
  RoundXenCopyFileSizeFeatures(copy_features);
  for (int i = CopyExecTimePredictor::MaxFileSize;
       i <= CopyExecTimePredictor::P90FileSize; ++i) {
    CPPUNIT_ASSERT_DOUBLES_EQUAL(copy_features[i],
                                 expected_features_disable_rounding[i],
                                 std::numeric_limits<float>::epsilon());
  }
  // copy_features should remain unchanged, so we continue testing with fuzzy
  // match. Enable fuzzy match for copy execution time predictor with 100 as a
  // rounding factor.
  gconf_predictor_fuzzy_match_copy_exec_time = 100;
  RoundXenCopyFileSizeFeatures(copy_features);
  for (int i = CopyExecTimePredictor::MaxFileSize;
       i <= CopyExecTimePredictor::P90FileSize; ++i) {
    CPPUNIT_ASSERT_DOUBLES_EQUAL(copy_features[i],
                                 expected_features_enable_rounding[i],
                                 std::numeric_limits<float>::epsilon());
  }
}

// This unit test is designed to test the functionality of refreshing Knn and
// XGBoost in MoE asynchronous.
void TestPredictor::TestAsyncXgboostKnnRefresh() {
  constexpr int data_window = 2000;
  constexpr int num_unique_query = 1000;
  constexpr double output = 5e6;
  constexpr int input_dimension = 32;
  xvector<DataT> bucket_range = {0, 10e6, 60e6};
  // ValuePredictor will need the GUC below to setup.
  AutoValueRestorer<int> restore_gconf_max_repetition_count_in_ddc(
      gconf_predictor_max_repetition_count_in_ddc,
      gconf_predictor_max_repetition_count_in_ddc);
  gconf_predictor_max_repetition_count_in_ddc = 40;

  AutoValueRestorer<int> restore_gconf_predictor_knn_default_refresh_rate(
      gconf_predictor_knn_default_refresh_rate,
      gconf_predictor_knn_default_refresh_rate);

  AutoValueRestorer<int> restore_gconf_predictor_default_refresh_rate(
      gconf_predictor_default_refresh_rate,
      gconf_predictor_default_refresh_rate);

  AutoValueRestorer<int> restore_gconf_predictor_training_timeout_ms(
      gconf_predictor_training_timeout_ms, gconf_predictor_training_timeout_ms);

  // Refresh rate for Knn.
  gconf_predictor_knn_default_refresh_rate = 50;
  // Refresh rate for XGBoost.
  gconf_predictor_default_refresh_rate = 200;

  // Training timeout for knn.
  gconf_predictor_training_timeout_ms = 1000;

  auto aggr_func_avg = [](const xlist<DataT, MtPredictor>& values) -> DataT {
    return std::accumulate(values.begin(), values.end(), 0.0) /
           static_cast<DataT>(values.size());
  };

  // If MoE is not enabled, there should be no effect.
  {
    std::unique_ptr<ValuePredictor> val_predictor(new ValuePredictor(
        "Test", data_window, gconf_predictor_default_refresh_rate,
        30 /* percent of data for testing */, input_dimension, bucket_range,
        "" /* default hyper params */, aggr_func_avg,
        gconf_predictor_max_repetition_count_in_ddc,
        -1 /* no cluster size feature */));

    bool flag_is_off = false;
    for (auto i = 0; i < num_unique_query; ++i) {
      std::vector<DataT> input(input_dimension, i);
      val_predictor->CollectQuery(input, output);
      // Predictor worker calls this function periodically to check if we need
      // to rebuild, here we mimic the behavior after each addition of query
      // to data collector.
      val_predictor->ShouldRebuildXGB();
      flag_is_off = flag_is_off || val_predictor->ShouldRebuildKNN();
    }
    CPPUNIT_ASSERT(!flag_is_off);
  }

  // In this case, we simulate the way that the number of queries collected
  // from 1 to the max capacity, and verify the number of KNN refreshes.
  {
    std::unique_ptr<ValuePredictor> val_predictor(new ValuePredictor(
        "Test", data_window, gconf_predictor_default_refresh_rate,
        30 /* percent of data for testing */, input_dimension, bucket_range,
        "" /* default hyper params */, aggr_func_avg,
        gconf_predictor_max_repetition_count_in_ddc,
        -1 /* no cluster size feature */));
    std::unique_ptr<ValuePredictorCtxt> ctxt(
        dynamic_cast<ValuePredictorCtxt*>(val_predictor->GetContext()));
    std::unique_ptr<QueryPredictorClient> val_predictor_client(
        new ValuePredictorClient("Test", ctxt.get()));
    val_predictor->EnableMoE();

    int num_of_refresh = 0;
    for (auto i = 0; i < num_unique_query; ++i) {
      std::vector<DataT> input(input_dimension, i);
      val_predictor->CollectQuery(input, output);
      // We simulate repeated queries to test that KNN refreshing is based
      // on number of unique queries, not number of (repeated) queries.
      val_predictor->CollectQuery(input, output);
      val_predictor->ShouldRebuildXGB();
      if (val_predictor->ShouldRebuildKNN()) {
        num_of_refresh += 1;
        val_predictor->BuildKnn(false, gconf_predictor_training_timeout_ms);
        std::unique_ptr<ValuePredictorCtxt> ctxt(
            dynamic_cast<ValuePredictorCtxt*>(val_predictor->GetContext()));
        val_predictor_client.reset(
            new ValuePredictorClient("Test", ctxt.get()));
        val_predictor->ClearRebuildKNNFlag();
      }
      // When number of unique queries is less than the guc, there should not
      // be any refresh. Note, i starts from 0.
      if (i + 1 <= gconf_predictor_knn_default_refresh_rate - 1) {
        double pred =
            val_predictor_client->Predict(input, -1);
        CPPUNIT_ASSERT_DOUBLES_EQUAL(pred, -1.0,
                                     std::numeric_limits<float>::epsilon());
        CPPUNIT_ASSERT_EQUAL(0, num_of_refresh);
      }
      // The first refresh should happen right after number of unique queries
      // hits gconf_predictor_knn_default_refresh_rate.
      if (i + 1 == gconf_predictor_knn_default_refresh_rate) {
        double pred =
            val_predictor_client->Predict(input, -1);
        CPPUNIT_ASSERT_EQUAL(output, pred);
        CPPUNIT_ASSERT_EQUAL(1, num_of_refresh);
      }
    }
    CPPUNIT_ASSERT_EQUAL(num_of_refresh,
                  num_unique_query / gconf_predictor_knn_default_refresh_rate);
  }
}

void TestPredictor::TestXGBoostPredictionUncertainty() {
  constexpr int data_window = 2000;
  constexpr int num_unique_query = 1000;
  constexpr double output = 5e6;
  constexpr int input_dimension = 32;
  // assume last feature is cluster_size
  constexpr int cluster_size_index = input_dimension - 1;
  xvector<DataT> bucket_range = {0, 10e6, 60e6};
  // ValuePredictor will need the GUC below to setup.
  AutoValueRestorer<int> restore_gconf_max_repetition_count_in_ddc(
      gconf_predictor_max_repetition_count_in_ddc,
      gconf_predictor_max_repetition_count_in_ddc);
  gconf_predictor_max_repetition_count_in_ddc = 40;

  AutoValueRestorer<int> restore_gconf_predictor_knn_default_refresh_rate(
      gconf_predictor_knn_default_refresh_rate,
      gconf_predictor_knn_default_refresh_rate);

  AutoValueRestorer<int> restore_gconf_predictor_default_refresh_rate(
      gconf_predictor_default_refresh_rate,
      gconf_predictor_default_refresh_rate);

  AutoValueRestorer<int> restore_gconf_predictor_training_timeout_ms(
      gconf_predictor_training_timeout_ms, gconf_predictor_training_timeout_ms);

  AutoValueRestorer<double> restore_gconf_predictor_uncertainty_threshold(
      gconf_predictor_uncertainty_threshold,
      gconf_predictor_uncertainty_threshold);

  // Refresh rate for Knn.
  gconf_predictor_knn_default_refresh_rate = 0;
  // Refresh rate for XGBoost.
  gconf_predictor_default_refresh_rate = 200;

  // Training timeout for knn.
  gconf_predictor_training_timeout_ms = 10000;
  gconf_predictor_uncertainty_threshold = 0.5;

  auto aggr_func_avg = [](const xlist<DataT, MtPredictor>& values) -> DataT {
    return std::accumulate(values.begin(), values.end(), 0.0) /
           static_cast<DataT>(values.size());
  };

  std::unique_ptr<ValuePredictor> val_predictor(new ValuePredictor(
      "Test", data_window, gconf_predictor_default_refresh_rate,
      50 /* percent of data for testing */, input_dimension, bucket_range,
      "" /* default hyper params */, aggr_func_avg,
      gconf_predictor_max_repetition_count_in_ddc, cluster_size_index));
  val_predictor->EnableMoE();
  std::unique_ptr<ValuePredictorCtxt> ctxt(
      dynamic_cast<ValuePredictorCtxt*>(val_predictor->GetContext()));
  std::unique_ptr<QueryPredictorClient> val_predictor_client(
      new ValuePredictorClient("Test", ctxt.get()));
  int training_round = 0;
  // Submiting 1000 unique queries.
  // 100 queries with cluster size 4
  // 100 queries with cluster size 8
  // 200 queries with cluster size 16
  // 400 queries with cluster size 32
  // 200 queries with cluster size 64
  // For each training, last 50% queries will be used for testing,
  // and will not be collected by the histogram.
  // Expected results:
  // First build:
  //  Cluster Size: 4, Uncertainty Score: 0.0
  // Second build:
  //  Cluster Size: 4, Uncertainty Score: 0.5
  //  Cluster Size: 8, Uncertainty Score: 0.5
  // Third build:
  //  Cluster Size: 4, Uncertainty Score: 0.67
  //  Cluster Size: 8, Uncertainty Score: 0.67
  //  Cluster Size: 16, Uncertainty Score: 0.67
  // Fourth build:
  //  Cluster Size: 4, Uncertainty Score: 0.75
  //  Cluster Size: 8, Uncertainty Score: 0.75
  //  Cluster Size: 16, Uncertainty Score: 0.5
  // Fifth build:
  //  Cluster Size: 4, Uncertainty Score: 0.8
  //  Cluster Size: 8, Uncertainty Score: 0.8
  //  Cluster Size: 16, Uncertainty Score: 0.6
  //  Cluster Size: 32, Uncertainty Score: 0.8
  for (auto i = 0; i < num_unique_query; ++i) {
    std::vector<DataT> input(input_dimension, i);
    // Replace cluster size feature.
    if (i < 100) {
      input[cluster_size_index] = 4;
    } else if (i < 200) {
      input[cluster_size_index] = 8;
    } else if (i < 400) {
      input[cluster_size_index] = 16;
    } else if (i < 800) {
      input[cluster_size_index] = 32;
    } else {
      input[cluster_size_index] = 64;
    }
    val_predictor->CollectQuery(input, output);
    if (val_predictor->ShouldRebuildXGB()) {
      training_round += 1;
      // Query scale histogram will be built with XGBoost.
      val_predictor->BuildXGBoost(false, gconf_predictor_training_timeout_ms);
      std::unique_ptr<ValuePredictorCtxt> ctxt(
          dynamic_cast<ValuePredictorCtxt*>(val_predictor->GetContext()));
      *(val_predictor->GetNewDataCounter()) = 0;
      val_predictor_client.reset(new ValuePredictorClient("Test", ctxt.get()));
      if (training_round == 1) {
        std::vector<int> cluster_sizes = {4, 8, 16};
        std::vector<double> expected_uncertainty = {0.0, 1.0, 1.0};
        CheckHistogramUncertaintyResults(val_predictor_client, cluster_sizes,
                                         expected_uncertainty);
      } else if (training_round == 2) {
        std::vector<int> cluster_sizes = {4, 8, 16};
        std::vector<double> expected_uncertainty = {0.5, 0.5, 1.0};
        CheckHistogramUncertaintyResults(val_predictor_client, cluster_sizes,
                                         expected_uncertainty);
      } else if (training_round == 3) {
        std::vector<int> cluster_sizes = {4, 8, 16, 32};
        std::vector<double> expected_uncertainty = {2.0 / 3, 2.0 / 3, 2.0 / 3,
                                                    1.0};
        CheckHistogramUncertaintyResults(val_predictor_client, cluster_sizes,
                                         expected_uncertainty);
      } else if (training_round == 4) {
        std::vector<int> cluster_sizes = {4, 8, 16, 32};
        std::vector<double> expected_uncertainty = {0.75, 0.75, 0.5, 1.0};
        CheckHistogramUncertaintyResults(val_predictor_client, cluster_sizes,
                                         expected_uncertainty);
      } else if (training_round == 5) {
        std::vector<int> cluster_sizes = {4, 8, 16, 32, 64};
        std::vector<double> expected_uncertainty = {0.8, 0.8, 0.6, 0.8, 1.0};
        CheckHistogramUncertaintyResults(val_predictor_client, cluster_sizes,
                                         expected_uncertainty);
      }
    }
  }
}

void TestPredictor::TestEnsembleScalingPredictionWithoutOverwrite() {
  AutoValueRestorer avr(gconf_predictor_ensemble_enable_overwriting, false);
  const double EPSILON = 0.001;
  SetCurrentTransaction(1);
  QueryDescPtr q(Query_desc::Create("", Query_desc::QueryCmdTypeNone));

  q->predictions_.pretrained_exec_time_sec_[4] = 100;
  q->predictions_.pretrained_exec_time_sec_[8] = 60;
  q->predictions_.pretrained_exec_time_sec_[16] = 40;
  q->predictions_.pretrained_exec_time_sec_[32] = 25;
  q->predictions_.pretrained_exec_time_sec_[64] = 18;

  q->predictions_.local_exec_time_sec_[8] = 50;
  q->predictions_.local_exec_time_sec_[32] = 20;

  const double desired_weight = (50. * 60. + 20. * 25.) /
                                (std::pow(60., 2) + std::pow(25., 2));
  gconf_predictor_scaling_ensemble_clip_min = 0.1;
  gconf_predictor_scaling_ensemble_clip_max = 10.;
  std::unordered_map<int, double> revised_pred;
  double ensemble_wieght;
  std::tie(revised_pred, ensemble_wieght) =
    EnsembleLocalAndPretrainedScalingPredictor(
      q->predictions_.pretrained_exec_time_sec_,
      q->predictions_.local_exec_time_sec_);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(ensemble_wieght, desired_weight, EPSILON);
  CPPUNIT_ASSERT(q->predictions_.pretrained_exec_time_sec_.size() ==
                  revised_pred.size());

  for (auto const& pair : revised_pred) {
    // The pre-trained predictions must exists.
    CPPUNIT_ASSERT(q->predictions_.pretrained_exec_time_sec_.count(
        pair.first) > 0);

    CPPUNIT_ASSERT_DOUBLES_EQUAL(revised_pred[pair.first],
      q->predictions_.pretrained_exec_time_sec_[pair.first] *
          desired_weight,
      EPSILON);
  }
}

}  // namespace predictor
