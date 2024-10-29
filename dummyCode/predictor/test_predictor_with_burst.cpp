// Copyright(c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved

#include "predictor/balanced_collector.hpp"
#include "predictor/deduplicated_collector.hpp"
#include "predictor/predictor_main.hpp"
#include "predictor/predictor_util.hpp"
#include "predictor/types.hpp"
#include "sys/query.hpp"
#include "wlm/queryqueue.hpp"
#include "wlm/serviceclass.hpp"
#include "wlm/wlm_test_base.hpp"
#include "wlm/wlm_test_utils.hpp"
#include "wlm/wlmquery.hpp"
#include "wlm/workloadmanager.h"
#include "wlm/workloadmanager.hpp"
#include "xen_test_utils/xen_test_utils.hpp"
#include "xen_utils/auto_value_restorer.hpp"

#include <boost/range/combine.hpp>
#include <boost/shared_ptr.hpp>

#include <cstdlib>

#include <stdlib.h>

extern bool gconf_enable_predictor;
extern int gconf_predictor_default_data_window;
extern bool gconf_predictor_enable_moe_memory;
extern char* gconf_predictor_memory_moe_param_override;
extern char* gconf_predictor_default_xgboost_param_override;
extern char* gconf_predictor_default_moe_xgboost_param_override;
extern bool gconf_predictor_enable_query_total_memory;
extern double gconf_predictor_memory_prediction_rescaling_factor;
extern int gconf_predictor_default_client_refresh_frequency_s;
extern int gconf_predictor_knn_default_refresh_rate;
extern int gconf_predictor_max_repetition_count_in_ddc;
extern int gconf_predictor_memory_data_window;
extern int gconf_predictor_exec_time_data_window;
extern int gconf_predictor_local_query_scaling_data_window;
extern int gconf_predictor_local_copy_scaling_data_window;
extern bool gconf_enable_predictor_collect_burst_query;
extern bool gconf_autowlm_concurrency;
extern bool gconf_autowlm_auto_concurrency_heavy;
extern bool gconf_autowlm_enable_flexible_memory_allocation;
extern bool gconf_is_burst_cluster;
extern int gconf_predictor_ddc_fuzzy_match_mul;
extern double gconf_predictor_scaling_ensemble_clip_min;
extern double gconf_predictor_scaling_ensemble_clip_max;
extern bool gconf_local_query_scaling_predictor_enable;
extern bool gconf_local_query_scaling_predictor_enable_moe;
extern int gconf_predictor_default_refresh_rate;
extern char* gconf_sage_scaling_allowed_burst_sizes;
extern bool gconf_predictor_ensemble_enable_overwriting;
namespace predictor {

class TestPredictorWithBurstQuery : public WlmTestBase {
 public:
  void SetUpGUCs() override;

  CPPUNIT_TEST_SUITE(TestPredictorWithBurstQuery);
  CPPUNIT_TEST(TestQueryLevelMemoryPrediction);
  CPPUNIT_TEST(TestBackwardCompatibilityResizeRollbackDDC);
  CPPUNIT_TEST(TestBackwardCompatibilityResizeRollbackBDC);
  CPPUNIT_TEST(TestBackwardCompatibilityUpgradeDDC);
  CPPUNIT_TEST(TestBackwardCompatibilityUpgradeBDC);
  CPPUNIT_TEST(TestRangeScaleDDC);
  CPPUNIT_TEST(TestRangeScaleBDC);
  CPPUNIT_TEST(TestCopyMemoryFeatureMatch);
  CPPUNIT_TEST(TestPostRunScalingEnsemble);
  CPPUNIT_TEST(TestPostRunScalingEnsembleWithAbort);
  CPPUNIT_TEST(TestEnsembleUsedObsFromOldMainSize);
  CPPUNIT_TEST(TestClusterSizeFeatureRestored);
  CPPUNIT_TEST_SUITE_END();

 public:
  void setUp() override;
  void tearDown() override;

 private:
  /// Test when we enable predict at query level so that query run on any burst
  /// size will be collected by predictor for training.
  void TestQueryLevelMemoryPrediction();
  void TestBackwardCompatibilityResizeRollbackDDC();
  void TestBackwardCompatibilityResizeRollbackBDC();
  void TestBackwardCompatibilityUpgradeDDC();
  void TestBackwardCompatibilityUpgradeBDC();
  void TestRangeScaleDDC();
  void TestRangeScaleBDC();
  /// Test if local features match with scaling features for copy queries.
  void TestCopyMemoryFeatureMatch();
  void InitQueryForPredictor(Query_desc* q);
  void TestPostRunScalingEnsemble();
  void TestPostRunScalingEnsembleWithAbort();
  void VerifyPostRunScalingEnsemble(Query_desc* q, double exec_time_sec,
                                    int exec_cluster_size);
  void VerifyAllEnsembleResultsMissing(Query_desc* q);

  void InitLocalScalingPredictorFeature(xen::span<DataT> features,
                                        DataT cluster_size, DataT feat_value);

  void TestEnsembleUsedObsFromOldMainSize();
  void TestClusterSizeFeatureRestored();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestPredictorWithBurstQuery);

void TestPredictorWithBurstQuery::setUp() {
  XenTestUtils::SetUpXenTopology();
  WlmTestBase::setUp();
}

void TestPredictorWithBurstQuery::tearDown() {
  XenTestUtils::SetUpXenTopology();
  WlmTestBase::tearDown();
}

void TestPredictorWithBurstQuery::SetUpGUCs() {
  WlmTestBase::SetUpGUCs();
  // Set GUCs needed in this test.
  gconf_autowlm_concurrency = true;
  gconf_autowlm_auto_concurrency_heavy = true;
  gconf_autowlm_enable_flexible_memory_allocation = true;
  gconf_is_burst_cluster = false;

  gconf_enable_predictor = true;
  gconf_predictor_default_data_window = 2000;
  gconf_predictor_exec_time_data_window = 2000;
  gconf_predictor_memory_data_window = 2000;
  gconf_predictor_local_query_scaling_data_window = 2000;
  gconf_predictor_local_copy_scaling_data_window = 2000;
  // These GUCs need a value since MoE for memory predictor because they are
  // needed for ValuePredictor constructor.
  gconf_predictor_memory_moe_param_override = "";
  gconf_predictor_default_xgboost_param_override = "";
  gconf_predictor_default_moe_xgboost_param_override = "";
  gconf_predictor_memory_prediction_rescaling_factor = 1.0;
  gconf_predictor_knn_default_refresh_rate = 1;
  gconf_predictor_max_repetition_count_in_ddc = 40;
  gconf_enable_predictor_collect_burst_query = true;

  gconf_predictor_ddc_fuzzy_match_mul = -1;

  // TestPredictor::TestEnsembleScalingPredictionWithoutOverwrite covers
  // the false case specifically.
  gconf_predictor_ensemble_enable_overwriting = true;
}

// In this test, we test:
// (1) if query is collected when bursted to a cluster
//     which has difference size as main;
// (2) the query run on main should also be collected;
// (3) predictor should be resilient with resizing main cluster;
// (4) although we collected query ran on different cluster size,
//     AutoWLM or any other component should get the per-CN mmemory when use
//     Query_desc::GetEstimatedMemoryMb() i.e, query-level-memory divided by
//     current main cluster size.
void TestPredictorWithBurstQuery::TestQueryLevelMemoryPrediction() {
  AutoValueRestorer<bool> restore_predictor_enable_query_level_memory(
      gconf_predictor_enable_query_total_memory, true);

  AutoValueRestorer<bool> restore_predictor_enable_moe_memory(
      gconf_predictor_enable_moe_memory, true);

  AutoValueRestorer<double> restore_memory_prediction_rescaling_factor(
      gconf_predictor_memory_prediction_rescaling_factor, 1.0);

  AutoValueRestorer<int> restore_predictor_default_client_refresh_frequency_s(
      gconf_predictor_default_client_refresh_frequency_s, 0);

  Xen->predictors = new Predictors();
  const int64_t query_level_mem_bytes = 1024l * 1024l * 1024l;
  SetCurrentTransaction(100);
  // Create a query, it will then be configured to run on main and burst and
  // collected by predictor seperately.
  QueryDescPtr query(Query_desc::Create("", Query_desc::QueryCmdTypeNone));
  query->PredictorFeatures()[QueryPredictor::Scan] = 12;
  query->PredictorFeatures()[QueryPredictor::ScanRows] = 123;
  query->PredictorFeatures()[QueryPredictor::IsSelect] = 1;

  auto wlmquery = WLMQuery(query.get());
  // setup of serviceclasses.
  wlmquery.set_running_service_class_id(kFirstManualUserQueue);
  // Assume this query ran on burst cluster.
  wlmquery.SetBurstState(WLMQuery::RunOnBurst);
  wlmquery.set_query_state(WLMQueryState::kDone);
  int main_size =
      static_cast<int>(Xen->cluster_manager->GetNumNodes(MyClusterVersion));
  // Assume burst cluster is 2x of main.
  int burst_size = main_size * 2;
  query->SetBurstNumNodes(burst_size);
  query->SetMaxMemoryNeeded(query_level_mem_bytes / burst_size);
  Xen->wlm->CollectRelevantQueryMainAndBurstForPrediction(&wlmquery);

  // This query should be collected although query is bursted to a different
  // size of cluster.
  CPPUNIT_ASSERT_EQUAL(
      1, *Xen->predictors->memory_predictor_->GetNewDataCounter());

  // Reconfigure to assume run on main.
  wlmquery.SetBurstState(WLMQuery::Undecided);
  // Set cluster size and burst size different.
  query->SetBurstNumNodes(main_size);
  query->SetMaxMemoryNeeded(query_level_mem_bytes / main_size);
  Xen->wlm->CollectRelevantQueryMainAndBurstForPrediction(&wlmquery);
  // This query ran on main should also be collected.
  CPPUNIT_ASSERT_EQUAL(
      2, *Xen->predictors->memory_predictor_->GetNewDataCounter());

  // Simulate resizing main.
  const int resized_main_size = main_size * 4;
  Xen->cluster_manager->num_nodes(resized_main_size, MyClusterVersion);
  main_size =
      static_cast<int>(Xen->cluster_manager->GetNumNodes(MyClusterVersion));

  // Make sure the simulated resizing worked.
  CPPUNIT_ASSERT_EQUAL(resized_main_size, main_size);
  // Reconfigure to assume run on main.
  wlmquery.SetBurstState(WLMQuery::Undecided);
  query->SetMaxMemoryNeeded(query_level_mem_bytes / main_size);
  Xen->wlm->CollectRelevantQueryMainAndBurstForPrediction(&wlmquery);
  // This query ran on resized main should also be collected.
  CPPUNIT_ASSERT_EQUAL(
      3, *Xen->predictors->memory_predictor_->GetNewDataCounter());

  std::unique_ptr<DataCollector> dc =
      Xen->predictors->memory_predictor_->PopulateUniqueQueriesForKNN(
          1 /* minimum reps */);
  // There should be only 1 unique query.
  CPPUNIT_ASSERT_EQUAL(1, dc->GetInputSize());

  // As there are multiple cast between int, float and double, we should allow
  // some accuracy lose.
  const int EPSILON = 100;
  // The collected value should be query-level memory requirment, i.e, total
  // instead of per-CN.
  CPPUNIT_ASSERT_DOUBLES_EQUAL(query_level_mem_bytes, dc->GetOutputs()[0],
                               EPSILON);

  // Now we build the predictor (MoE).
  Xen->predictors->memory_predictor_->Build(false /* no tunning */,
                                            1000 /* timeout in ms*/);
  // Initialize predictor clients.
  MyPredictor = std::make_shared<PredictorClients>(Xen->predictors);
  query->SetQueryPredictions();

  // Although predictor predicts at query level, AutoWLM and others should
  // still get memory prediction of per-CN at current main size.
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      query_level_mem_bytes / main_size,
      query->GetEstimatedMemoryMb() * BYTES_PER_MEGABYTE, EPSILON);

  MyPredictor.reset();
  delete Xen->predictors;
  Xen->predictors = nullptr;
}

// The backward compatibility was implemented as follows:
// when serialize, we save the main cluster size, called dump cluster size,
// and also scale the label according to cluster size, i.e., if the label in
// data collector is 16GB and main clustes size is 4, we will serialize label
// as 16GB/4 = 4GB, this is to make sure that we can roll back. Note that, we
// apply label scaling only when GUC predictor_enable_query_total_memory is
// on.
// when deserialize, there are two cases:
//   (1) if we can find the dump cluster key from the dump file:
//       a. if predictor_enable_query_total_memory is no, we apply label scale
//          to scale 4GB back to 16GB
//       b. no label scale will be applied if GUC
//          predictor_enable_query_total_memory is off
//  (2) if we cannot find the dump cluster key from the dump file,
//      i.e, upgraded from an old version. We will then do label scaling if
//      needed using current cluster size.

// This test covers the DedupDataCollector. We will serialize with one cluster
// size and then make sure expected behavior when
// (1) deserialize on a different main cluster size
// (2) roll back
void TestPredictorWithBurstQuery::TestBackwardCompatibilityResizeRollbackDDC() {
  constexpr int input_dimension = 2;
  const xvector<predictor::DataT> ranges{0, 2, 4};
  const int maxRepetitionCount = 10;
  auto aggr_func_avg = [](const xlist<DataT, MtPredictor>& values) -> DataT {
    return std::accumulate(values.begin(), values.end(), 0.0) /
           static_cast<DataT>(values.size());
  };

  int dump_main_size = 4;
  int resized_main_size = dump_main_size * 2;

  Xen->cluster_manager->num_nodes(dump_main_size, MyClusterVersion);
  // Serialize data collector
  DeDupCollector dedupCollector(
      ranges, "TestBackwardCompatibility", input_dimension,
      200 /* window size */, maxRepetitionCount, aggr_func_avg);
  dedupCollector.should_normalize_output_ = true;
  dedupCollector.AddData(std::array<DataT, input_dimension>{1, 1}, 12.0);
  dedupCollector.AddData(std::array<DataT, input_dimension>{3, 3}, 2.0);
  dedupCollector.AddData(std::array<DataT, input_dimension>{3, 3}, 6.0);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  writer.StartObject();
  dedupCollector.Serialize(writer);
  writer.EndObject();

  // Simulated the case that cluster is resized to 2x.
  Xen->cluster_manager->num_nodes(resized_main_size, MyClusterVersion);
  // deserialize new collector expecting that the label, i.e, query level
  // memory remains the same.
  DeDupCollector newDedupCollector(
      ranges, "TestBackwardCompatibilityResized", input_dimension,
      200 /* window size */, maxRepetitionCount, aggr_func_avg);
  newDedupCollector.should_normalize_output_ = true;
  rapidjson::Document doc;
  doc.Parse<0>(buffer.GetString());
  CPPUNIT_ASSERT(newDedupCollector.Deserialize(doc) ==
                 DeserializationResult::kSuccess);
  // check number of samples deserialized.
  CPPUNIT_ASSERT_EQUAL(3, newDedupCollector.GetInputSize());
  CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(2),
                       newDedupCollector.GetUniqueQuerySize());

  DataCollector dc("ContainerForCPPUnitTest", input_dimension,
                   newDedupCollector.GetInputSize(), true);

  newDedupCollector.ToDataCollector(&dc, false /* no decompress */,
                                    newDedupCollector.GetInputSize(), 1);
  CPPUNIT_ASSERT_EQUAL(2, dc.GetInputSize());

  const float* outputs = dc.GetOutputs();
  const float* inputs = dc.GetInputs();
  const double EPSILON = 0.1;
  for (auto i = 0; i < dc.GetInputSize(); ++i) {
    // use the first feature as a key to check if label value is expected.
    if (inputs[i * input_dimension + 0] == 1) {
        CPPUNIT_ASSERT_DOUBLES_EQUAL(12.0, outputs[i], EPSILON);
    } else if (inputs[i * input_dimension + 0] == 3) {
        CPPUNIT_ASSERT_DOUBLES_EQUAL(4.0, outputs[i], EPSILON);
    } else {
        // something must be wrong if code reaches here.
        CPPUNIT_ASSERT(false);
    }
  }
  // if we roll back, i.e, disable via
  // predictor_enable_query_total_memory = false which ends up with false for
  // should_normalize_output_ for the data collector.
  // label deserialized should be per-CN, i.e, 3.0 and 1.0 respectively.
  // regardless of cluster size
  newDedupCollector.Reset();
  newDedupCollector.should_normalize_output_ = false;
  dc.Reset();
  CPPUNIT_ASSERT(newDedupCollector.Deserialize(doc) ==
                 DeserializationResult::kSuccess);

  newDedupCollector.ToDataCollector(&dc, false /* no decompress */,
                                    newDedupCollector.GetInputSize(), 1);
  CPPUNIT_ASSERT_EQUAL(2, dc.GetInputSize());

  for (auto i = 0; i < dc.GetInputSize(); ++i) {
    // use the first feature as a key to check if label value is expected.
    if (inputs[i * input_dimension + 0] == 1) {
        CPPUNIT_ASSERT_DOUBLES_EQUAL(3.0, outputs[i], EPSILON);
    } else if (inputs[i * input_dimension + 0] == 3) {
        CPPUNIT_ASSERT_DOUBLES_EQUAL(1.0, outputs[i], EPSILON);
    } else {
        // something must be wrong if code reaches here.
        CPPUNIT_ASSERT(false);
    }
  }
}

void TestPredictorWithBurstQuery::TestBackwardCompatibilityResizeRollbackBDC() {
  constexpr int input_dimension = 2;
  const xvector<predictor::DataT> ranges{0, 2, 4};

  int dump_main_size = 4;
  int resized_main_size = dump_main_size * 2;

  Xen->cluster_manager->num_nodes(dump_main_size, MyClusterVersion);
  // Serialize data collector
  BalancedCollector balancedCollector(
      ranges, "TestBackwardCompatibility", input_dimension,
      200 /* window size */);
  balancedCollector.should_normalize_output_ = true;
  balancedCollector.AddData(std::array<DataT, input_dimension>{1, 1}, 12.0);
  balancedCollector.AddData(std::array<DataT, input_dimension>{3, 3}, 4.0);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  writer.StartObject();
  balancedCollector.Serialize(writer);
  writer.EndObject();

  // Simulated the case that cluster is resized to 2x.
  Xen->cluster_manager->num_nodes(resized_main_size, MyClusterVersion);
  // deserialize new collector expecting that the label, i.e, query level
  // memory remains the same.
  BalancedCollector newbalancedCollector(
      ranges, "TestBackwardCompatibilityResized", input_dimension,
      200 /* window size */);
  newbalancedCollector.should_normalize_output_ = true;
  rapidjson::Document doc;
  doc.Parse<0>(buffer.GetString());
  CPPUNIT_ASSERT(newbalancedCollector.Deserialize(doc) ==
                 DeserializationResult::kSuccess);
  // check number of samples deserialized.
  CPPUNIT_ASSERT_EQUAL(2, newbalancedCollector.GetInputSize());
  {
    auto [dc, dummy_dc] = newbalancedCollector.Split(0);

    CPPUNIT_ASSERT_EQUAL(2, dc->GetInputSize());
    CPPUNIT_ASSERT_EQUAL(0, dummy_dc->GetInputSize());
    const float* outputs = dc->GetOutputs();
    const float* inputs = dc->GetInputs();
    const double EPSILON = 0.1;
    for (auto i = 0; i < dc->GetInputSize(); ++i) {
        // use the first feature as a key to check if label value is expected.
        // As predicting at query level is enabled, we should always see
        // 12GB and (6GB + 2GB) / 4 = 2GB respectively.
        if (inputs[i * input_dimension + 0] == 1) {
            CPPUNIT_ASSERT_DOUBLES_EQUAL(12.0, outputs[i], EPSILON);
        } else if (inputs[i * input_dimension + 0] == 3) {
            CPPUNIT_ASSERT_DOUBLES_EQUAL(4.0, outputs[i], EPSILON);
        } else {
            // something must be wrong if code reaches here.
            CPPUNIT_ASSERT(false);
        }
    }
  }
  // if we roll back, i.e, disable via
  // predictor_enable_query_total_memory = false which ends up with false for
  // should_normalize_output_ for the data collector.
  // label deserialized should be per-CN, i.e, 3.0 and 1.0 respectively.
  // regardless of cluster size
  newbalancedCollector.Reset();
  newbalancedCollector.should_normalize_output_ = false;
  CPPUNIT_ASSERT(newbalancedCollector.Deserialize(doc) ==
                 DeserializationResult::kSuccess);
  {
    auto [dc, dummy_dc] = newbalancedCollector.Split(0);

    CPPUNIT_ASSERT_EQUAL(2, dc->GetInputSize());
    CPPUNIT_ASSERT_EQUAL(0, dummy_dc->GetInputSize());
    const float* outputs = dc->GetOutputs();
    const float* inputs = dc->GetInputs();
    const double EPSILON = 0.1;
    for (auto i = 0; i < dc->GetInputSize(); ++i) {
        // use the first feature as a key to check if label value is expected.
        // when predicting at query level is disabled, label should be per-CN.
        // So, we expect to see 12GB / 4 = 3GB and
        // (2 + 6) / 2 / 4 = 1GB respectively.
        if (inputs[i * input_dimension + 0] == 1) {
            CPPUNIT_ASSERT_DOUBLES_EQUAL(3.0, outputs[i], EPSILON);
        } else if (inputs[i * input_dimension + 0] == 3) {
            CPPUNIT_ASSERT_DOUBLES_EQUAL(1.0, outputs[i], EPSILON);
        } else {
            // something must be wrong if code reaches here.
            CPPUNIT_ASSERT(false);
        }
    }
  }
}


void TestPredictorWithBurstQuery::TestBackwardCompatibilityUpgradeDDC() {
  const int input_dimension = 2;
  const xvector<predictor::DataT> ranges{0, 2, 4};
  const int maxRepetitionCount = 10;
  auto aggr_func_avg = [](const xlist<DataT, MtPredictor>& values) -> DataT {
    return std::accumulate(values.begin(), values.end(), 0.0) /
           static_cast<DataT>(values.size());
  };
  // old version DDC dump where cluster_size key is missing.
  const char dump_s[] = "{\"dedup_data\":{\"data_size\":2,\"input_dimension\""
                        ":2,\"data_matrix\":[{\"input\":[1.0,1.0],\"outputs\""
                        ":[3.0],\"output_timestamps\":[758941713567868]},"
                        "{\"input\":[3.0,3.0],\"outputs\":[0.5,1.5],\""
                        "output_timestamps\":[758941713567896,"
                        "758941713567911]}]}}";
  rapidjson::Document doc;
  doc.Parse<0>(dump_s);
  // Simulated the case that cluster has 8 nodes upgrade from a older dump.
  // Here will assume current cluster size is the same as it that created the
  // dump.
  Xen->cluster_manager->num_nodes(8, MyClusterVersion);
  DeDupCollector dedupCollector(
      ranges, "TestBackwardCompatibility", input_dimension,
      200 /* window size */, maxRepetitionCount, aggr_func_avg);
  dedupCollector.should_normalize_output_ = true;
  CPPUNIT_ASSERT(dedupCollector.Deserialize(doc) ==
                 DeserializationResult::kSuccess);
  // check number of samples deserialized.
  CPPUNIT_ASSERT_EQUAL(3, dedupCollector.GetInputSize());
  CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(2),
                       dedupCollector.GetUniqueQuerySize());

  DataCollector dc("ContainerForCPPUnitTest", input_dimension,
                   dedupCollector.GetInputSize(), true);

  dedupCollector.ToDataCollector(&dc, false /* no decompress */,
                                 dedupCollector.GetInputSize(), 1);
  CPPUNIT_ASSERT_EQUAL(2, dc.GetInputSize());

  const float* outputs = dc.GetOutputs();
  const float* inputs = dc.GetInputs();
  const double EPSILON = 0.1;
  for (auto i = 0; i < dc.GetInputSize(); ++i) {
    // use the first feature as a key to check if label value is expected.
    // here we expect 24GB and 8GB respectively, which is wrong, because we can
    // not know the size of cluster that generate samples in the dump and the
    // best we can do is to assume it is the same as the current main.
    // Assume patch n has this CR, the issue happens in a corner case where
    // a cluster is resized when run a patch < n, and then upgrade to patch n.
    if (inputs[i * input_dimension + 0] == 1) {
        CPPUNIT_ASSERT_DOUBLES_EQUAL(24.0, outputs[i], EPSILON);
    } else if (inputs[i * input_dimension + 0] == 3) {
        CPPUNIT_ASSERT_DOUBLES_EQUAL(8.0, outputs[i], EPSILON);
    } else {
        // something must be wrong if code reaches here.
        CPPUNIT_ASSERT(false);
    }
  }
}

void TestPredictorWithBurstQuery::TestBackwardCompatibilityUpgradeBDC() {
  const int input_dimension = 2;
  const xvector<predictor::DataT> ranges{0, 2, 4};
  // old version BDC dump where cluster_size key is missing.
  const char dump_s[] = "{\"ranges_data\":[{\"range\":0.0,\"data\":{\"data_size"
                      "\":0,\"input_dimension\":2,\"data_matrix\":[]}},{\""
                      "range\":2.0,\"data\":{\"data_size\":0,\"input_dimension"
                      "\":2,\"data_matrix\":[]}},{\"range\":4.0,\"data\":{\""
                      "data_size\":2,\"input_dimension\":2,\"data_matrix\":"
                      "[{\"input\":[1.0,1.0],\"output\":3.0},{\"input\":"
                      "[3.0,3.0],\"output\":1.0}]}}]}";
  BalancedCollector balancedCollector(
      ranges, "TestBackwardCompatibilityResized", input_dimension,
      200 /* window size */);
  balancedCollector.should_normalize_output_ = true;
  Xen->cluster_manager->num_nodes(8, MyClusterVersion);
  rapidjson::Document doc;
  doc.Parse<0>(dump_s);
  CPPUNIT_ASSERT(balancedCollector.Deserialize(doc) ==
                 DeserializationResult::kSuccess);
  // check number of samples deserialized.
  CPPUNIT_ASSERT_EQUAL(2, balancedCollector.GetInputSize());
  auto [dc, dummy_dc] = balancedCollector.Split(0);

  CPPUNIT_ASSERT_EQUAL(2, dc->GetInputSize());
  CPPUNIT_ASSERT_EQUAL(0, dummy_dc->GetInputSize());
  const float* outputs = dc->GetOutputs();
  const float* inputs = dc->GetInputs();
  const double EPSILON = 0.1;
  for (auto i = 0; i < dc->GetInputSize(); ++i) {
    // use the first feature as a key to check if label value is expected.
    if (inputs[i * input_dimension + 0] == 1) {
      CPPUNIT_ASSERT_DOUBLES_EQUAL(24.0, outputs[i], EPSILON);
    } else if (inputs[i * input_dimension + 0] == 3) {
      CPPUNIT_ASSERT_DOUBLES_EQUAL(8.0, outputs[i], EPSILON);
    } else {
      // something must be wrong if code reaches here.
      CPPUNIT_ASSERT(false);
    }
  }
}

void TestPredictorWithBurstQuery::TestRangeScaleDDC() {
  constexpr int data_window = 12;

  constexpr int num_bucket1 = 4;
  constexpr int num_bucket2 = 6;
  constexpr int num_bucket3 = 3;

  constexpr float value_bucket1 = 5;
  constexpr float value_bucket2 = 15;
  constexpr float value_bucket3 = 65;

  constexpr int input_dimension = 32;
  xvector<DataT> bucket_range = {0, 10, 60};
  const int main_cluster_size = 2;
  auto aggr_func_avg = [](const xlist<DataT, MtPredictor>& values) -> DataT {
    return std::accumulate(values.begin(), values.end(), 0.0) /
           static_cast<DataT>(values.size());
  };
  // Make sure the call this before constructing DDC so that the cluster size
  // is correctly cached in the ddc object.
  Xen->cluster_manager->num_nodes(main_cluster_size, MyClusterVersion);
  auto collector = DeDupCollector(bucket_range, "Test", input_dimension,
                                  data_window,
                                  10 /* max repetitions per query to save */,
                                  aggr_func_avg);

  collector.should_normalize_output_ = true;
  // after this, the buckets will be [0, 20, 120], and histogram is [4+6, 3, 0]
  // So, after all the AddData() call below, the 1st queries should have been
  // evicted because DDC kicks out the least updated item from the biggest
  // bucket. The expected bucket size is [9, 3, 0];
  // If ScaleBucketRangeByMainSize does not work as expected, the
  // 5th query will be kickedout as histogram is [4, 6, 3], i.e, kick out the
  // 1st one that enters the 2nd bucket, which is the 5th query, end up with
  // [4, 5, 3]
  collector.ScaleBucketRangeByMainSize();

  int feature_value = 1;
  for (auto i = 0; i < num_bucket1; ++i) {
    auto input = std::vector<DataT>(input_dimension, feature_value);
    collector.AddData(input, value_bucket1);
    // repeated queries
    collector.AddData(input, value_bucket1);
    // make sure query input are unique
    feature_value += 1;
  }

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

  // Split collector so that everything should end up in the training
  // collector to check if it correctly balances and deduplicate the data.
  auto [training, test] = collector.Split(0, data_window);
  CPPUNIT_ASSERT_EQUAL(data_window, training->GetInputSize());
  CPPUNIT_ASSERT_EQUAL(0, test->GetInputSize());

  // Compute the expected buckets to validate data in the collector.
  xvector<DataT> new_bucket_range(bucket_range.begin(), bucket_range.end());
  for (size_t i = 0; i < new_bucket_range.size(); ++i) {
    new_bucket_range[i] *= main_cluster_size;
  }

  std::vector<int> bucket_size(new_bucket_range.size(), 0);
  const float* outputs = training->GetOutputs();
  const float* inputs = training->GetInputs();
  bool is_1st_exist = false;
  for (auto i = 0; i < training->GetInputSize(); ++i) {
    auto bucket = std::upper_bound(new_bucket_range.begin(),
                                   new_bucket_range.end(),
                                   outputs[i]);
    if (bucket != std::begin(new_bucket_range)) {
      bucket = std::prev(bucket);
    }
    int bucket_idx = bucket - new_bucket_range.begin();
    bucket_size[bucket_idx] += 1;
    // Based on the design of balancing mechanism, the 1st query should have
    // been kicked-out when the 13th query came, here we check it the 1st was
    // actually kicked out.
    is_1st_exist = is_1st_exist || (1 == inputs[i * input_dimension]);
  }
  CPPUNIT_ASSERT_EQUAL(9, bucket_size[0]);
  CPPUNIT_ASSERT_EQUAL(3, bucket_size[1]);
  CPPUNIT_ASSERT_EQUAL(0, bucket_size[2]);
  // The 1st unique query (in the 2nd bucket) should have been kicked out
  // because of the 13th queries, based on LRU algorithm.
  CPPUNIT_ASSERT(!is_1st_exist);
}

void TestPredictorWithBurstQuery::TestRangeScaleBDC() {
  constexpr int data_window = 12;

  constexpr int num_bucket1 = 4;
  constexpr int num_bucket2 = 6;
  constexpr int num_bucket3 = 3;

  constexpr float value_bucket1 = 5;
  constexpr float value_bucket2 = 15;
  constexpr float value_bucket3 = 65;

  constexpr int input_dimension = 32;
  xvector<DataT> bucket_range = {0, 10, 60};
  const int main_cluster_size = 2;
  // Make sure the call this before constructing DDC so that the cluster size
  // is correctly cached in the ddc object.
  Xen->cluster_manager->num_nodes(2, MyClusterVersion);
  auto collector = BalancedCollector(bucket_range, "Test", input_dimension,
                                     data_window);

  collector.should_normalize_output_ = true;
  // after this, the buckets will be [0, 20, 120], and hist is [4+6, 3, 0]
  // So, after all the AddData() call below, a random query in the 1st bucket
  // should have been evicted because BDC kicks out a random item from the
  // biggest bucket when full. So, results will be [9, 3, 0]
  // If ScaleBucketRangeByMainSize does not work as expected, the
  // one query in the 2nd bucket will be kickedout as hist is [4, 6, 3], i.e,
  // kick out a random one enters from the 2nd bucket end up with [4, 5, 3].
  collector.ScaleBucketRangeByMainSize();

  int feature_value = 1;
  for (auto i = 0; i < num_bucket1; ++i) {
    auto input = std::vector<DataT>(input_dimension, feature_value);
    collector.AddData(input, value_bucket1);
    // repeated queries
    collector.AddData(input, value_bucket1);
    // make sure query input are unique
    feature_value += 1;
  }

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

  // Split collector so that everything should end up in the training
  // collector to check if it correctly balances and deduplicate the data.
  auto [training, test] = collector.Split(0);
  CPPUNIT_ASSERT_EQUAL(data_window, training->GetInputSize());
  CPPUNIT_ASSERT_EQUAL(0, test->GetInputSize());

  // Compute the expected buckets to validate data in the collector.
  xvector<DataT> new_bucket_range(bucket_range.begin(), bucket_range.end());
  for (size_t i = 0; i < new_bucket_range.size(); ++i) {
    new_bucket_range[i] *= main_cluster_size;
  }
  std::vector<int> bucket_size(new_bucket_range.size(), 0);
  const float* outputs = training->GetOutputs();
  for (auto i = 0; i < training->GetInputSize(); ++i) {
    auto bucket = std::upper_bound(new_bucket_range.begin(),
                                   new_bucket_range.end(),
                                   outputs[i]);
    if (bucket != std::begin(new_bucket_range)) {
      bucket = std::prev(bucket);
    }
    int bucket_idx = bucket - new_bucket_range.begin();
    bucket_size[bucket_idx] += 1;
  }
  CPPUNIT_ASSERT_EQUAL(9, bucket_size[0]);
  CPPUNIT_ASSERT_EQUAL(3, bucket_size[1]);
  CPPUNIT_ASSERT_EQUAL(0, bucket_size[2]);
}

void TestPredictorWithBurstQuery::TestCopyMemoryFeatureMatch() {
  QueryDescPtr query(Query_desc::Create("", Query_desc::QueryCmdTypeCopy));
  // Local execution time features
  query->CopyPredictorFeatures()[CopyExecTimePredictor::TableDistKey] = 1;
  query->CopyPredictorFeatures()[CopyExecTimePredictor::NumFiles] = 123;
  query->CopyPredictorFeatures()[CopyExecTimePredictor::MaxFileSize] = 1000;
  query->CopyPredictorFeatures()[CopyExecTimePredictor::MinFileSize] = 1;
  query->CopyPredictorFeatures()[CopyExecTimePredictor::P10FileSize] = 10;
  query->CopyPredictorFeatures()[CopyExecTimePredictor::P50FileSize] = 50;
  query->CopyPredictorFeatures()[CopyExecTimePredictor::P90FileSize] = 90;
  query->CopyPredictorFeatures()[CopyExecTimePredictor::IsAnalyze] = 0;

  // Test if pretrained feature is correctly populated.
  PopulateCopyScalingPredictorFeatures(query->CopyPredictorFeatures(),
                                       query->CopyScalingPredictorFeatures());
  const int EPSILON = 1;
  CPPUNIT_ASSERT_DOUBLES_EQUAL(1,
                               query->CopyScalingPredictorFeatures()
                                   [CopyPretrainedPredictor::TableDistKey],
                               EPSILON);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      123,
      query->CopyScalingPredictorFeatures()[CopyPretrainedPredictor::NumFiles],
      EPSILON);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(1000,
                               query->CopyScalingPredictorFeatures()
                                   [CopyPretrainedPredictor::MaxFileSize],
                               EPSILON);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(1,
                               query->CopyScalingPredictorFeatures()
                                   [CopyPretrainedPredictor::MinFileSize],
                               EPSILON);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(10,
                               query->CopyScalingPredictorFeatures()
                                   [CopyPretrainedPredictor::P10FileSize],
                               EPSILON);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(50,
                               query->CopyScalingPredictorFeatures()
                                   [CopyPretrainedPredictor::P50FileSize],
                               EPSILON);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(90,
                               query->CopyScalingPredictorFeatures()
                                   [CopyPretrainedPredictor::P90FileSize],
                               EPSILON);
  CPPUNIT_ASSERT_DOUBLES_EQUAL(
      0,
      query->CopyScalingPredictorFeatures()[CopyPretrainedPredictor::IsAnalyze],
      EPSILON);
}


void TestPredictorWithBurstQuery::VerifyPostRunScalingEnsemble(
    Query_desc* q,
    double exec_time_sec,
    int exec_cluster_size) {
  const double EPSILON = 0.001;
  const double expected_weight = exec_time_sec /
      q->predictions_.pretrained_exec_time_sec_[exec_cluster_size];

  // Check if is it populated as expected, if not,
  // q->predictions_.postrun_ensemble_exec_time_sec_ will be empty
  CPPUNIT_ASSERT(q->predictions_.pretrained_exec_time_sec_.size() ==
                 q->predictions_.postrun_ensemble_exec_time_sec_.size());
  for (const auto& [scale, pred] : q->predictions_.pretrained_exec_time_sec_) {
    // make sure it's populated, otherwise it's empty
    CPPUNIT_ASSERT(q->predictions_.postrun_ensemble_exec_time_sec_.count(
        scale) > 0);

    // now we make sure the revised values are correct.
    if (exec_cluster_size == scale) {
      // prediction of the execution cluster size should be over-written
      CPPUNIT_ASSERT_DOUBLES_EQUAL(exec_time_sec,
          q->predictions_.postrun_ensemble_exec_time_sec_[scale],
          EPSILON);
    } else {
      // make sure revised value is expected
      CPPUNIT_ASSERT_DOUBLES_EQUAL(
          q->predictions_.postrun_ensemble_exec_time_sec_[scale],
          pred * expected_weight,
          EPSILON);
    }
  }
}

// This function makes additional initialization of a QueryDesc object so that
// it's eligible for predictor.
void TestPredictorWithBurstQuery::InitQueryForPredictor(Query_desc* q) {
  q->predictor_features_[QueryPredictor::IsSelect] = 1;
  q->m_system_tables_accessed = 0;
}

// Scenario 1/ when query ran on main, scenario
// 2/ when query ran on burst where
//   2.1 burst is the same size as main
//   2.2 burst is smaller (1/2) than main
//   2.3 burst is bigger(2x) of main
void TestPredictorWithBurstQuery::TestPostRunScalingEnsemble() {
  gconf_predictor_scaling_ensemble_clip_min = 0.1;
  gconf_predictor_scaling_ensemble_clip_max = 10.0;
  const int preset_main_size = 8;
  Xen->cluster_manager->num_nodes(preset_main_size, MyClusterVersion);
  const int64 exec_time_us = 30000000;
  // Create a query, it will then be configured to run on main and burst and
  // collected by predictor seperately.
  QueryDescPtr q(Query_desc::Create("", Query_desc::QueryCmdTypeSelect));
  // make it eligible for predictor
  InitQueryForPredictor(q.get());
  q->predictions_.pretrained_exec_time_sec_[4] = 100;
  q->predictions_.pretrained_exec_time_sec_[8] = 60;
  q->predictions_.pretrained_exec_time_sec_[16] = 40;
  q->predictions_.pretrained_exec_time_sec_[32] = 25;
  q->predictions_.pretrained_exec_time_sec_[64] = 18;

  // scenario 1 when query ran on main
  PopulatePostrunScalingPredictions(q.get(), exec_time_us);
  VerifyPostRunScalingEnsemble(q.get(),
                               static_cast<double>(exec_time_us) / US_PER_SEC,
                               preset_main_size);

  int burst_cluster_size = preset_main_size;
  // scenario 2.1 ran on burst and burst cluster size is same as main
  q->SetBurstNumNodes(burst_cluster_size);
  PopulatePostrunScalingPredictions(q.get(), exec_time_us);
  VerifyPostRunScalingEnsemble(q.get(),
                               static_cast<double>(exec_time_us) / US_PER_SEC,
                               burst_cluster_size);

  // scenario 2.2 ran on burst and burst cluster size is half of main
  burst_cluster_size = preset_main_size / 2;
  q->SetBurstNumNodes(burst_cluster_size);
  PopulatePostrunScalingPredictions(q.get(), exec_time_us);
  VerifyPostRunScalingEnsemble(q.get(),
                               static_cast<double>(exec_time_us) / US_PER_SEC,
                               burst_cluster_size);

  // scenario 2.3 ran on burst and burst cluster size is 2x of main
  burst_cluster_size = preset_main_size * 2;
  q->SetBurstNumNodes(burst_cluster_size);
  PopulatePostrunScalingPredictions(q.get(), exec_time_us);
  VerifyPostRunScalingEnsemble(q.get(),
                               static_cast<double>(exec_time_us) / US_PER_SEC,
                               burst_cluster_size);
}

void TestPredictorWithBurstQuery::VerifyAllEnsembleResultsMissing(
  Query_desc* q) {
    // make sure it's not empty.
    CPPUNIT_ASSERT(q->predictions_.postrun_ensemble_exec_time_sec_.size() > 0);

  for (const auto& [scale, pred] : q->predictions_.pretrained_exec_time_sec_) {
    // Missed prediction should not be because of missed initial prediction.
    // i.e., prediction from pre-trained model should be avaiable.
    CPPUNIT_ASSERT(pred > 0);
    // Despite Pre-trained prediction is avaiable as checked above, the post
    // run prediction should not exist.
    CPPUNIT_ASSERT(q->predictions_.postrun_ensemble_exec_time_sec_[scale] < 0);
  }
}

void TestPredictorWithBurstQuery::TestPostRunScalingEnsembleWithAbort() {
  gconf_predictor_scaling_ensemble_clip_min = 0.1;
  gconf_predictor_scaling_ensemble_clip_max = 10.0;
  const int preset_main_size = 8;
  Xen->cluster_manager->num_nodes(preset_main_size, MyClusterVersion);
  const int64 exec_time_us = 30000000;
  // Create a query, it will then be configured to run on main and burst and
  // collected by predictor seperately.
  QueryDescPtr q(Query_desc::Create("", Query_desc::QueryCmdTypeSelect));
  // make it eligible for predictor
  InitQueryForPredictor(q.get());
  q->predictions_.pretrained_exec_time_sec_[4] = 100;
  q->predictions_.pretrained_exec_time_sec_[8] = 60;
  q->predictions_.pretrained_exec_time_sec_[16] = 40;
  q->predictions_.pretrained_exec_time_sec_[32] = 25;
  q->predictions_.pretrained_exec_time_sec_[64] = 18;

  // scenario 1 via IsAborted()
  q->set_aborted();
  PopulatePostrunScalingPredictions(q.get(), exec_time_us);
  VerifyAllEnsembleResultsMissing(q.get());
  // clear the flag set via set_aborted() for the next test scenario.
  q->m_aborted = false;

  // scenario 2 via IsWlmqueryAborted()
  q->set_wlmquery_aborted();
  VerifyAllEnsembleResultsMissing(q.get());
}

void TestPredictorWithBurstQuery::InitLocalScalingPredictorFeature(
    xen::span<DataT> features, DataT cluster_size, DataT feat_value) {
  for (auto& feature : features) feature = feat_value;
  features[LocalQueryScalingPredictor::IsSelect] = 1;
  features[LocalQueryScalingPredictor::ClusterSize] = cluster_size;
}

void TestPredictorWithBurstQuery::TestEnsembleUsedObsFromOldMainSize() {
  const std::unordered_set<int> old_main_sizes({7, 11, 15, 32});
  std::unordered_map<int, double> expected_ens_predictions;
  // These true values are precomputed offline based on the algorithm design.
  expected_ens_predictions[4] = 62.5;
  expected_ens_predictions[8] = 31.25;
  expected_ens_predictions[16] = 15.625;
  expected_ens_predictions[64] = 3.90625;
  expected_ens_predictions[24] = 10.4167;
  // values below are override of observation.
  expected_ens_predictions[7] = 35.7143;
  expected_ens_predictions[11] = 22.7272;
  expected_ens_predictions[15] = 16.6667;
  expected_ens_predictions[32] = 7.8125;
  DataT
      local_query_scaling_features[LocalQueryScalingPredictor::kMaxFeatures]{};

  AutoValueRestorer avr1(gconf_predictor_max_repetition_count_in_ddc, 40);
  // Refresh rate for Knn.
  AutoValueRestorer avr2(gconf_predictor_knn_default_refresh_rate,
                        old_main_sizes.size());
  // Refresh rate for XGBoost. we do not need to build XGB in this test.
  AutoValueRestorer avr3(gconf_predictor_knn_default_refresh_rate, 200);
  AutoValueRestorer avr4(gconf_local_query_scaling_predictor_enable, true);
  AutoValueRestorer avr5(gconf_local_query_scaling_predictor_enable_moe, true);
  AutoValueRestorer avr6(gconf_sage_scaling_allowed_burst_sizes,
                        const_cast<char*>("4,8,16,32,64"));
  AutoValueRestorer avr7(gconf_predictor_scaling_ensemble_clip_min, 0.0001);
  AutoValueRestorer avr8(gconf_predictor_scaling_ensemble_clip_max, 10000.0);

  auto predictors = std::make_unique<Predictors>();
  AutoValueRestorer avr9(Xen->predictors, predictors.get());
  // Submit queries that were observed with old main cluster size and build the
  // predictor. To make it easier, we assume truth/label execution time scaled
  // with efficiency of 0.8.
  for (const auto cluster_size : old_main_sizes) {
    const double size_one_exec_time_us = 200e6;
    const double scaling_efficiency = 0.8;
    InitLocalScalingPredictorFeature(local_query_scaling_features,
                                     static_cast<DataT>(cluster_size), 0.0);
    Xen->predictors->local_query_scaling_predictor_->CollectQuery(
        local_query_scaling_features,
        size_one_exec_time_us / (scaling_efficiency * cluster_size));
  }

  Xen->predictors->local_query_scaling_predictor_->Build(
      /*should_tune=*/false, /*timeout_ms=*/10000);

  // Initialize predictor clients.
  auto my_predictor = std::make_shared<PredictorClients>(Xen->predictors);
  AutoValueRestorer avr10(MyPredictor, my_predictor);

  SetCurrentTransaction(1);

  QueryDescPtr q(Query_desc::Create("query for test",
                                    Query_desc::QueryCmdTypeSelect));
  // Additional init to make this query MLPredictorEligible
  InitQueryForPredictor(q.get());
  InitLocalScalingPredictorFeature(q->local_query_scaling_predictor_features_,
                                   /*cluster_size=*/24,
                                   /*feat_value=*/0.0);
  // Over-write pre-trained predictions via the event.
  Event_info ei("EtPretrainedPredictionOverWrite,value=100");
  xen_populate_single_event(&ei);
  PopulateScalingPredictions(q.get());

  for (const auto& [scale, pred] : expected_ens_predictions) {
    CPPUNIT_ASSERT_DOUBLES_EQUAL(pred,
                                 q->GetPredictedScalingSeconds(scale),
                                 /*EPSILON=*/0.1);
  }
  UNSET_EVENT(EtPretrainedPredictionOverWrite);
  // Also check if the cluster size in training set are correctly populated.
  std::unordered_set<int> scales_for_training = MyPredictor
      ->local_query_scaling_predictor_client_->GetClusterSizeInTrainSet();
  CPPUNIT_ASSERT(old_main_sizes == scales_for_training);
}

// This test makes sure that cluster_size feature in Query_desc
// is restored after calling GetLocalScalingPrediction.
void TestPredictorWithBurstQuery::TestClusterSizeFeatureRestored() {
  AutoValueRestorer avr1(gconf_predictor_max_repetition_count_in_ddc, 40);
  // Refresh rate for Knn.
  AutoValueRestorer avr2(gconf_predictor_knn_default_refresh_rate, 1);
  // Refresh rate for XGBoost. we do not need to build XGB in this test.
  AutoValueRestorer avr3(gconf_predictor_knn_default_refresh_rate, 200);
  AutoValueRestorer avr4(gconf_local_query_scaling_predictor_enable, true);
  AutoValueRestorer avr5(gconf_local_query_scaling_predictor_enable_moe, true);

  auto predictors = std::make_unique<Predictors>();
  AutoValueRestorer avr6(Xen->predictors, predictors.get());

  // Submit queries to build predictor.
  DataT features[LocalQueryScalingPredictor::kMaxFeatures]{};

  InitLocalScalingPredictorFeature(features,
                                   /*cluster_size=*/4.0,
                                   /*feat_value=*/2.0);
  Xen->predictors->local_query_scaling_predictor_->CollectQuery(
      features, /*output=*/40.0);

  InitLocalScalingPredictorFeature(features,
                                   /*cluster_size=*/8.0,
                                   /*feat_value=*/2.0);
  Xen->predictors->local_query_scaling_predictor_->CollectQuery(
      features, /*output=*/80.0);

  Xen->predictors->local_query_scaling_predictor_->Build(
      /*should_tune=*/false, /*timeout_ms=*/10000);

  // Initialize predictor clients.
  auto my_predictor = std::make_shared<PredictorClients>(Xen->predictors);
  AutoValueRestorer avr7(MyPredictor, my_predictor);
  // Create a Query_desc obj and init cluster_size features
  // by designing, this init cluster_size will be considered
  // as current main size and won't change after iterating over
  // prediction of many different cluster sizes.
  SetCurrentTransaction(1);
  const int main_size = 24;
  QueryDescPtr q(Query_desc::Create("query for test",
                 Query_desc::QueryCmdTypeSelect));

  InitLocalScalingPredictorFeature(q->local_query_scaling_predictor_features_,
                                   main_size, /*feat_value=*/2.0);
  // Additional init to make this query MLPredictorEligible
  InitQueryForPredictor(q.get());
  auto pred = GetLocalScalingPrediction(q.get(), /*cluster_size=*/4.0);
  // Make sure cluster_size feature is unchanged
  CPPUNIT_ASSERT_DOUBLES_EQUAL(main_size,
                               q->local_query_scaling_predictor_features_[
                                  LocalQueryScalingPredictor::ClusterSize],
                               /*EPSILON=*/0.1);
  // Make sure prediction is expected. KNN will hit, and the
  // truth {clusterSize: truth} should be {4: 40, 8: 80} as collected for
  // training above.
  CPPUNIT_ASSERT_DOUBLES_EQUAL(40.0, pred, /*EPSILON=*/0.1);
}

}  // namespace predictor
