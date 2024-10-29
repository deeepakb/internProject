// Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#pragma once

#include <cppunit/extensions/HelperMacros.h>

#include <memory>
#include <string>
#include <vector>

#include "predictor/data_collector.hpp"
#include "predictor/predictor_main.hpp"

namespace predictor {

class PredictorWorker;

class TestPredictor : public CPPUNIT_NS::TestFixture {
 public:
  void setUp();
  void tearDown();

  CPPUNIT_TEST_SUITE(TestPredictor);

  CPPUNIT_TEST(TestDataReset);
  CPPUNIT_TEST(TestDeserializationRead);
  CPPUNIT_TEST(TestPredictorWorkerGucDisabled);
  CPPUNIT_TEST(TestPredictorWorkerWithWlmOccupancy);
  CPPUNIT_TEST(TestPredictorWorkerExists);
  CPPUNIT_TEST(TestPredictorWorkerTooSoonToBeCreated);
  CPPUNIT_TEST(TestPredictorWorkerNoRebuildNeeded);
  CPPUNIT_TEST(TestPredictorWorkerDownloadFromS3);
  CPPUNIT_TEST(TestAnalyzeQuery);
  CPPUNIT_TEST(TestQueryTypes);
  CPPUNIT_TEST(TestBalancedCollector);
  CPPUNIT_TEST(TestDedupCollector);
  CPPUNIT_TEST(TestDedupCollectorCornerCases);
  CPPUNIT_TEST(TestRepeatedQueryAttr);
  CPPUNIT_TEST(TestConcatDedupAndBalancedCollector);
  CPPUNIT_TEST(TestSerializeDeserialize);
  CPPUNIT_TEST(TestSerializeDeserializeRawModelBase64);
  CPPUNIT_TEST(TestSerializeDeserializeNoBalancing);
  CPPUNIT_TEST(TestExtractRanges);
  CPPUNIT_TEST(TestDataCollectorDeserializeDataWithMissingFeatures);
  CPPUNIT_TEST(TestDeDupCollectorDeserializeDataWithMissingFeatures);
  CPPUNIT_TEST(TestDataCollectorDeserializeDataWithExtraFeatures);
  CPPUNIT_TEST(TestDeDupCollectorDeserializeDataWithExtraFeatures);
  CPPUNIT_TEST(TestDataCollectorDeserializeEmptyData);
  CPPUNIT_TEST(TestDeDupCollectorDeserializeEmptyData);
  CPPUNIT_TEST(TestAsyncXgboostKnnRefresh);
  CPPUNIT_TEST(TestValuePredictorNeedsRebuildOnFeatureChange);
  CPPUNIT_TEST(TestEnsembleScalingPrediction);
  CPPUNIT_TEST(TestHyperParameterParse);
  CPPUNIT_TEST(TestErrorPredictionWithAndWithoutMoe);
  CPPUNIT_TEST(TestScalingPredictorFeaturesAreCorrectlyPopulated);
  CPPUNIT_TEST(TestDedupCollectorWithRounding);
  CPPUNIT_TEST(TestCopyQueryPredictorFileSizeFuzzyMatch);
  CPPUNIT_TEST(TestXGBoostPredictionUncertainty);
  CPPUNIT_TEST(TestEnsembleScalingPredictionWithoutOverwrite);

  CPPUNIT_TEST_SUITE_END();

 private:
  void LoadData(std::string filename, DataCollector* data_collector);

  /// Helper function to check the histogram uncertainty results.
  /// @param client: Query predictor client object.
  /// @param expected: Expected histogram results.
  /// @param cluster_size: cluster size for previous results.
  void CheckHistogramUncertaintyResults(
      std::unique_ptr<QueryPredictorClient>& client,
      std::vector<int>& cluster_size, std::vector<double>& expected);

  /// Helper function to call worker start, if desired, wait until the worker
  /// finishes, and then return true if a new worker was created.
  /// worker: Predictor worker object
  /// occupancy: wlm occupancy to call start worker.
  /// should_wait_for_new_worker: Should wait for new worker to finish or no.
  /// Returns true if a new worker is created.
  static bool CreateNewWorker(PredictorWorker* worker, double occupancy,
      bool should_wait_for_new_worker);

  /// Check that only one type of select is set.
  /// @param features: Query predictor feature array.
  /// @param feature_index: The boolean feature index that should be set.
  template <typename T>
  void CheckOnlyOneSelectSet(xen::span<T> features, int feature_index) {
    if (feature_index == QueryPredictor::IsSelect) {
      CPPUNIT_ASSERT_EQUAL(features[QueryPredictor::IsSelect], T(1.0));
      CPPUNIT_ASSERT_EQUAL(features[QueryPredictor::IsCtas], T(0.0));
    }
    if (feature_index == QueryPredictor::IsCtas) {
      CPPUNIT_ASSERT_EQUAL(features[QueryPredictor::IsSelect], T(0.0));
      CPPUNIT_ASSERT_EQUAL(features[QueryPredictor::IsCtas], T(1.0));
    }
  }

  /// Check that all write features except the one mentioned are unset.
  /// @param features: Query predictor feature array.
  /// @param a_set: The boolean feature that should be set.
  void CheckOnlyOneWriteSet(xen::span<const DataT> features,
                            QueryPredictor::QueryPredictorFeatures a_set);

  /// Tests that model data is reset with variable size of data to keep.
  void TestDataReset();

  /// Tests deserialization reader doesn't read files bigger than 16MB(approx).
  /// Predictors::ReadFromFile reads in 16KB chunk and in every chunk the
  /// last byte is null character. So, the Predictors::ReadFromFile can read
  /// 16MB - 1024 total bytes. This test checks that ReadFromFile is not
  /// reading more than 16MB - 1024 bytes.
  void TestDeserializationRead();

  /// Tests predictor workers are not created in case enable_predictor or
  /// enable_predictor_worker guc is disabled.
  void TestPredictorWorkerGucDisabled();

  /// Tests predictor workers with various wlm occupancy.
  void TestPredictorWorkerWithWlmOccupancy();

  /// Tests predictor worker is not created when there is an existing worker.
  void TestPredictorWorkerExists();

  /// Tests predictor worker is not created if it's too soon to start another.
  void TestPredictorWorkerTooSoonToBeCreated();

  /// Tests predictor worker is not created if model doesn't need to be
  /// rebuilt.
  void TestPredictorWorkerNoRebuildNeeded();

  /// Tests predictor worker tries to download from s3 limited times.
  void TestPredictorWorkerDownloadFromS3();

  /// Tests query types.
  void TestQueryTypes();

  /// Tests analyze query for corner cases like 0 rows or width.
  void TestAnalyzeQuery();

  /// Tests if samples are balanced correctly along the ranges.
  void TestBalancedCollector();

  /// Tests if samples are balanced and deduplicated correctly.
  void TestDedupCollector();

  /// Tests corner cases that is rare to be included in TestDedupCollector.
  void TestDedupCollectorCornerCases();

  /// Test the GetAverage member function of class RepeatedQueryAttr.
  /// It also test the behavior to kick out oldest repetition when its capacity
  /// is full.
  void TestRepeatedQueryAttr();

  /// Tests if enabled by GUC, we can concat queries from BalancedCollector
  // and DedupCollector.
  void TestConcatDedupAndBalancedCollector();

  /// Tests if serialization/deserialization is correct.
  void TestSerializeDeserialize();

  /// Tests if serialization/deserialization is backwards compatible with raw
  /// model.
  void TestSerializeDeserializeRawModelBase64();

  /// Tests if serialization/deserialization is backwards compatible with non
  /// balancing data.
  void TestSerializeDeserializeNoBalancing();

  /// Tests if parsing of ranges works correctly.
  void TestExtractRanges();

  /// Test that when deserializing a DataCollector with fewer features than
  /// expected, the missing features will be inputted as kMissingValue
  void TestDataCollectorDeserializeDataWithMissingFeatures();

  /// Test that when deserializing a DeDupCollector with fewer features than
  /// expected, the missing features will be inputted as kMissingValue
  void TestDeDupCollectorDeserializeDataWithMissingFeatures();

  /// Test that when deserializing a DataCollector with more features than
  /// expected, the extra values will be truncated
  void TestDataCollectorDeserializeDataWithExtraFeatures();

  /// Test that when deserializing a DeDupCollector with more features than
  /// expected, the extra values will be truncated
  void TestDeDupCollectorDeserializeDataWithExtraFeatures();

  /// Test DataCollector deserialization when data is empty
  void TestDataCollectorDeserializeEmptyData();

  /// Test DeDupCollector deserialization when data is empty
  void TestDeDupCollectorDeserializeEmptyData();

  /// Test building Knn and XGB asynchronously when MoE is enabled.
  void TestAsyncXgboostKnnRefresh();

  /// Test that ValuePredictor::Deserialize succeeds when there is a feature
  /// mismatch, and ShouldRebuildModel returns true before the model is rebuilt.
  void TestValuePredictorNeedsRebuildOnFeatureChange();

  /// Test Query_desc::EnsembleLocalAndPretrainedScalingPredictor.
  void TestEnsembleScalingPrediction();

  /// Test that parsing a hyper-parameter json correctly, otherwise
  /// use default value.
  void TestHyperParameterParse();

  // Testing DedupCollector with fuzzy match on feature vector.
  void TestDedupCollectorWithRounding();

  /// Test that GetErrorPrediction method in QueryDesc works as
  /// expected, i.e., returns 0 when MoE is enabled, -1 in case of negative
  /// prediction, and the model's estimate otherwise.
  void TestErrorPredictionWithAndWithoutMoe();

  /// Test that scaling predictor features are correctly populated from
  /// query predictor and spectrum features.
  void TestScalingPredictorFeaturesAreCorrectlyPopulated();

  /// Test test fuzzy file size feature is rounded in copy query predictor.
  void TestCopyQueryPredictorFileSizeFuzzyMatch();

  /// Test that xgboost prediction uncertainty is correct.
  void TestXGBoostPredictionUncertainty();

  /// Test that over-writing enemble by observation can be disabled.
  void TestEnsembleScalingPredictionWithoutOverwrite();

  void SetUpModelParameters();

  void TrainModel();

  void VerifyModel();

  /// Local copy of the guc values.
  bool local_enable_predictor_;
  bool local_enable_predictor_worker_;
  bool local_gconf_vacuum_predictor_enable_;
  int local_predictor_default_data_window_;
  int local_predictor_default_refresh_rate_;
  int local_predictor_burst_exec_time_refresh_rate_;
  int local_predictor_training_timeout_ms_;
  int local_vacuum_predictor_training_timeout_ms_;
  int local_predictor_worker_frequency_s_;
  double local_predictor_worker_wlm_occupancy_allowance_;
  bool local_predictor_enable_backwards_feature_compatibility_;
  bool local_local_query_scaling_predictor_enable_moe;
  bool local_local_query_scaling_predictor_enable;
  int local_predictor_fuzzy_match_copy_exec_time;
  bool local_predictor_enable_moe_copy_exec_time;
  bool local_copy_exec_time_predictor;
  bool local_copy_query_memory_predictor_enable;
  bool local_predictor_copy_memory_refresh_rate;
  double local_predictor_uncertainty_threshold;
  static constexpr double kSmallInput = 10.0;
  static constexpr double kSmallOutput = 10.0;
  DataT small_query_features_[QueryPredictor::kMaxFeatures]{};
  DataT small_mv_features_[MvRefreshExecTimePredictor::kMaxFeatures]{};
  DataT small_vac_features_[VacuumExecTimePredictor::kMaxFeatures]{};
  DataT small_local_query_scaling_features_
      [LocalQueryScalingPredictor::kMaxFeatures]{};
  DataT small_copy_features_[CopyExecTimePredictor::kMaxFeatures]{};
  DataT small_copy_memory_features_[CopyMemoryPredictor::kMaxFeatures]{};
  static constexpr double kLargeInput = 100.0;
  static constexpr double kLargeOutput = 100.0;
  DataT large_query_features_[QueryPredictor::kMaxFeatures]{};
  DataT large_mv_features_[MvRefreshExecTimePredictor::kMaxFeatures]{};
  DataT large_vac_features_[VacuumExecTimePredictor::kMaxFeatures]{};
  DataT large_local_query_scaling_features_
      [LocalQueryScalingPredictor::kMaxFeatures]{};
  DataT large_copy_features_[CopyExecTimePredictor::kMaxFeatures]{};
  DataT large_copy_memory_features_[CopyMemoryPredictor::kMaxFeatures]{};
};

}  // namespace predictor
