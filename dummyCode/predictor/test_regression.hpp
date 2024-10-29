// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#pragma once

#include <string>
#include <vector>

#include <cppunit/extensions/HelperMacros.h>

using std::string;

namespace predictor::decision_trees {

/// Tests xgboost regression predictor.
class TestRegression : public CPPUNIT_NS::TestFixture {
 public:
  void setUp();
  void tearDown();

  CPPUNIT_TEST_SUITE(TestRegression);

  CPPUNIT_TEST(TestFitWithoutTuning);
  CPPUNIT_TEST(TestFitWithoutTuningWithMissingData);
  CPPUNIT_TEST(TestFitTimeoutWithoutTuning);
  CPPUNIT_TEST(TestFitWithTuning);
  CPPUNIT_TEST(TestFitTimeoutWithTuning);
  CPPUNIT_TEST(TestLoadEmptyDump);
  CPPUNIT_TEST(TestKNearestNeighbor);
  CPPUNIT_TEST(TestQuantileSquaredErrorAndScore);
  CPPUNIT_TEST(TestCustomizedQuantileLoss);
  CPPUNIT_TEST_SUITE_END();

 private:
  std::vector<std::vector<float>> LoadData(std::string filename);

  void JsonAssert(std::string json_string, int seed, int best_iteration,
                  double best_iteration_score, std::string max_depth = "6",
                  std::string min_child_weight = "1",
                  int training_dimension = 28, std::string eta = "0.3",
                  std::string colsample_bytree = "1",
                  std::string eval_metric = "rmse", std::string gamma = "0",
                  std::string objective = "reg:squarederror",
                  std::string reg_alpha = "0", std::string subsample = "1");

  /// Tests that predictions are as expected without tuning the model.
  void TestFitWithoutTuning();

  /// Tests that predictions remain unchanged when data points filled
  /// with missing values are added to training set.
  void TestFitWithoutTuningWithMissingData();

  /// Tests timeout while training without tuning..
  void TestFitTimeoutWithoutTuning();

  /// Tests that predictions are as expected with tuning the model.
  void TestFitWithTuning();

  /// Tests timeout while training with tuning..
  void TestFitTimeoutWithTuning();

  // Tests that empty model loading in Regression obj doesn't cause a segfault.
  void TestLoadEmptyDump();

  // Tests building a zero-distance KNN model, and verify it with prediction.
  void TestKNearestNeighbor();

  // Test quantile squared error and score is correctly calculated in Booster.
  void TestQuantileSquaredErrorAndScore();

  // Test custimized quantile loss for MoE memory predictor.
  void TestCustomizedQuantileLoss();
};

}  // namespace predictor::decision_trees
