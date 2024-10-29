// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.

#include "predictor/test_regression.hpp"

#include "predictor/data_collector.hpp"
#include "predictor/decision_trees/regression.hpp"
#include "predictor/knn.hpp"
#include "predictor/predictor_api.hpp"
#include "predictor/types.hpp"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"

#include <boost/algorithm/string.hpp>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include <math.h>

#include "predictor/predictor_api.hpp"
#include "predictor/data_collector.hpp"
#include "predictor/decision_trees/regression.hpp"
#include "test_regression.hpp"
#include "predictor/knn.hpp"

using predictor::DataCollector;
using predictor::DataT;
using predictor::kMissingValue;
using predictor::KNearestNeighbor;
using predictor::kUnknownPrediction;
using predictor::decision_trees::Booster;
using predictor::decision_trees::IterScore;
using predictor::decision_trees::Regression;
using predictor::decision_trees::RegressionDump;

extern char* gconf_predictor_default_xgboost_param_override;
extern int gconf_predictor_default_data_window;
extern int gconf_predictor_exec_time_data_window;
extern int gconf_predictor_memory_data_window;
extern int gconf_predictor_copy_memory_data_window;
extern int gconf_predictor_local_query_scaling_data_window;
extern int gconf_predictor_local_copy_scaling_data_window;
extern int gconf_predictor_ddc_fuzzy_match_mul;

namespace predictor::decision_trees {

float EPSILON;

CPPUNIT_TEST_SUITE_REGISTRATION(TestRegression);

void TestRegression::setUp() {
  gconf_predictor_default_xgboost_param_override =
      R"({"colsample_bytree": "1", "eta": "0.3", "eval_metric": "rmse",
         "gamma": "0", "max_depth": "6", "min_child_weight": "1",
         "objective":"reg:squarederror", "reg_alpha": "0",
         "subsample": "1"})";
  gconf_predictor_default_data_window = 2000;
  gconf_predictor_exec_time_data_window = 2000;
  gconf_predictor_memory_data_window = 2000;
  gconf_predictor_copy_memory_data_window = 2000;
  gconf_predictor_local_query_scaling_data_window = 2000;
  gconf_predictor_local_copy_scaling_data_window = 2000;
  EPSILON = 0.001;
  // Disable fuzzy match for this test class.
  gconf_predictor_ddc_fuzzy_match_mul = -1;
}

void TestRegression::tearDown() {
}

std::vector<std::vector<float>> TestRegression::LoadData(std::string filename) {
  std::string delm = ",";
  std::ifstream file(filename);
  std::string line = "";
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
  return raw_data;
}

void TestRegression::JsonAssert(
    std::string json_string, int seed, int best_iteration,
    double best_iteration_score, std::string max_depth,
    std::string min_child_weight, int training_dimension, std::string eta,
    std::string colsample_bytree, std::string eval_metric, std::string gamma,
    std::string objective, std::string reg_alpha, std::string subsample) {
  rapidjson::Document doc;
  doc.Parse<0>(json_string.c_str());
  for (rapidjson::Value::ConstMemberIterator itr = doc.MemberBegin();
       itr != doc.MemberEnd(); ++itr) {
    std::string name = itr->name.GetString();
    if (name == "Seed") {
      CPPUNIT_ASSERT_EQUAL(seed, itr->value.GetInt());
    }
    if (name == "Best iteration") {
      CPPUNIT_ASSERT_EQUAL(best_iteration, itr->value.GetInt());
    }
    if (name == "Best iteration score") {
      double score = itr->value.GetDouble();
      // rmse score can be differ a little. We will allow 1% deviation.
      CPPUNIT_ASSERT_EQUAL(true,
          score > best_iteration_score - best_iteration_score * 0.01 &&
          score < best_iteration_score + best_iteration_score * 0.01);
    }
    if (name == "Training dimension") {
      CPPUNIT_ASSERT_EQUAL(training_dimension, itr->value.GetInt());
    }
    if (name == "Hyper params") {
      for (auto nested_itr = doc["Hyper params"].MemberBegin();
           nested_itr != doc["Hyper params"].MemberEnd(); ++nested_itr) {
        std::string param_name = nested_itr->name.GetString();
        if (param_name == "eta") {
          CPPUNIT_ASSERT_EQUAL(
              true, std::string(nested_itr->value.GetString()) == eta);
        }
        if (param_name == "max_depth") {
          CPPUNIT_ASSERT_EQUAL(
              true, std::string(nested_itr->value.GetString()) == max_depth);
        }
        if (param_name == "min_child_weight") {
          CPPUNIT_ASSERT_EQUAL(
              true,
              std::string(nested_itr->value.GetString()) == min_child_weight);
        }
        if (param_name == "min_child_weight") {
          CPPUNIT_ASSERT_EQUAL(
              true,
              std::string(nested_itr->value.GetString()) == min_child_weight);
        }
        if (param_name == "colsample_bytree") {
          CPPUNIT_ASSERT_EQUAL(
              true,
              std::string(nested_itr->value.GetString()) == colsample_bytree);
        }
        if (param_name == "eval_metric") {
          CPPUNIT_ASSERT_EQUAL(
              true, std::string(nested_itr->value.GetString()) == eval_metric);
        }
        if (param_name == "gamma") {
          CPPUNIT_ASSERT_EQUAL(
              true, std::string(nested_itr->value.GetString()) == gamma);
        }
        if (param_name == "objective") {
          CPPUNIT_ASSERT_EQUAL(
              true, std::string(nested_itr->value.GetString()) == objective);
        }
        if (param_name == "reg_alpha") {
          CPPUNIT_ASSERT_EQUAL(
              true, std::string(nested_itr->value.GetString()) == reg_alpha);
        }
        if (param_name == "subsample") {
          CPPUNIT_ASSERT_EQUAL(
              true, std::string(nested_itr->value.GetString()) == subsample);
        }
      }
    }
  }
}

void TestRegression::TestFitWithoutTuning() {
  std::vector<std::vector<float>> raw_data =
      LoadData("test/cppunit/predictor/exec_time_data.csv");

  std::unique_ptr<DataCollector> execTimeCollector(
      new DataCollector("TestFitWithoutTuningCollector", 28,
                        gconf_predictor_default_data_window));

  std::vector<std::vector<float>>::iterator it;

  // Keep last 30 rows to test the prediction result.
  for (it = raw_data.begin(); it != raw_data.end() - 30; ++it) {
    std::vector<float> input_v((*it).begin(), (*it).end() - 1);
    float output = (*it).back();
    execTimeCollector->AddData(input_v, output);
  }
  size_t data_size = execTimeCollector->GetInputSize();
  execTimeCollector->Shuffle(10);
  int part = static_cast<int>(data_size * 0.9);
  std::unique_ptr<DataCollector> training_data(
    execTimeCollector->CopyData(0, part));
  std::unique_ptr<DataCollector> test_data(
    execTimeCollector->CopyData(part, data_size));

  std::unique_ptr<Regression> regression(new Regression(
    gconf_predictor_default_xgboost_param_override));

  regression->Fit(training_data.get(), test_data.get(), false, 10000);
  std::unique_ptr<RegressionDump> dump(new RegressionDump());
  regression->Unload(dump.get());
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  dump->Serialize(writer);
  JsonAssert(buffer.GetString(), 0, 17, 17741474.0);

  float expected_pred[] = {
      64095004.0000000000, 5223844.0000000000,  3249046.5000000000,
      60587540.0000000000, 89012344.0000000000, 15580504.0000000000,
      24361978.0000000000, 13532578.0000000000, 35089420.0000000000,
      6601778.5000000000,  6216953.0000000000,  2755506.2500000000,
      16075102.0000000000, 31860874.0000000000, 13588286.0000000000,
      2755506.2500000000,  6368896.0000000000,  7486157.0000000000,
      14029192.0000000000, 17374868.0000000000, 180403904.0000000000,
      34742568.0000000000, 3828836.0000000000,  14539280.0000000000,
      5968618.5000000000,  37972432.0000000000, 12771540.0000000000,
      13954878.0000000000, 5239669.5000000000,  9222398.0000000000};
  for (int i = 0; it != raw_data.end(); ++it, ++i) {
    std::vector<float> input_v((*it).begin(), (*it).end() - 1);
    float pred_out = regression->PredictOne(input_v);
    CPPUNIT_ASSERT_EQUAL(expected_pred[i], pred_out);
  }
}

void TestRegression::TestFitWithoutTuningWithMissingData() {
  std::vector<std::vector<float>> raw_data =
      LoadData("test/cppunit/predictor/exec_time_data.csv");

  std::unique_ptr<DataCollector> execTimeCollector(new DataCollector(
      "TestFitWithoutTuning", 28, gconf_predictor_default_data_window));

  std::vector<std::vector<float>>::iterator it;

  // Keep last 30 rows to test the prediction result.
  for (it = raw_data.begin(); it != raw_data.end() - 30; ++it) {
    std::vector<float> input_v((*it).begin(), (*it).end() - 1);
    float output = (*it).back();
    execTimeCollector->AddData(input_v, output);
  }
  size_t data_size = execTimeCollector->GetInputSize();
  execTimeCollector->Shuffle(10);
  int part = static_cast<int>(data_size * 0.9);
  std::unique_ptr<DataCollector> training_data(
      execTimeCollector->CopyData(0, part));
  std::unique_ptr<DataCollector> test_data(
      execTimeCollector->CopyData(part, data_size));

  // Add many rows to training set filled with missing values
  // We expect XGBoost to simply ignore these values despite
  // their large, out-of-range targets.
  std::vector<float> unknownRow(raw_data.size(), kMissingValue);
  for (int i = 0; i < 25; i++) {
    training_data->AddData(unknownRow, -1e6);
  }

  std::unique_ptr<Regression> regression(
      new Regression(gconf_predictor_default_xgboost_param_override));

  regression->Fit(training_data.get(), test_data.get(), false, 10000);

  float expected_pred[] = {
      64095004.0000000000, 5223844.0000000000,  3249046.5000000000,
      60587540.0000000000, 89012344.0000000000, 15580504.0000000000,
      24361978.0000000000, 13532578.0000000000, 35089420.0000000000,
      6601778.5000000000,  6216953.0000000000,  2755506.2500000000,
      16075102.0000000000, 31860874.0000000000, 13588286.0000000000,
      2755506.2500000000,  6368896.0000000000,  7486157.0000000000,
      14029192.0000000000, 17374868.0000000000, 180403904.0000000000,
      34742568.0000000000, 3828836.0000000000,  14539280.0000000000,
      5968618.5000000000,  37972432.0000000000, 12771540.0000000000,
      13954878.0000000000, 5239669.5000000000,  9222398.0000000000};
  for (int i = 0; it != raw_data.end(); ++it, ++i) {
    std::vector<float> input_v((*it).begin(), (*it).end() - 1);
    float pred_out = regression->PredictOne(input_v);
    CPPUNIT_ASSERT_EQUAL(expected_pred[i], pred_out);
  }
}

void TestRegression::TestFitTimeoutWithoutTuning() {
  std::vector<std::vector<float>> raw_data =
      LoadData("test/cppunit/predictor/exec_time_data.csv");

  std::unique_ptr<DataCollector> execTimeCollector(
      new DataCollector("TestFitTimeoutWithoutTuningCollector", 28,
                        gconf_predictor_default_data_window));

  std::vector<std::vector<float>>::iterator it;

  // Keep last 30 rows to test the prediction result.
  for (it = raw_data.begin(); it != raw_data.end() - 30; ++it) {
    std::vector<float> input_v((*it).begin(), (*it).end() - 1);
    float output = (*it).back();
    execTimeCollector->AddData(input_v, output);
  }
  size_t data_size = execTimeCollector->GetInputSize();
  execTimeCollector->Shuffle(10);
  int part = static_cast<int>(data_size * 0.9);
  std::unique_ptr<DataCollector> training_data(
      execTimeCollector->CopyData(0, part));
  std::unique_ptr<DataCollector> test_data(
      execTimeCollector->CopyData(part, data_size));

  std::unique_ptr<Regression> regression(new Regression(
      gconf_predictor_default_xgboost_param_override));

  // Force timeout by setting it to 0
  regression->Fit(training_data.get(), test_data.get(), false, 0);
  std::unique_ptr<RegressionDump> dump(new RegressionDump());
  regression->Unload(dump.get());
  // Test that dump is empty.
  CPPUNIT_ASSERT_EQUAL(true, dump->IsEmptyDump());
}

void TestRegression::TestFitWithTuning() {
  std::vector<std::vector<float>> raw_data =
      LoadData("test/cppunit/predictor/exec_time_data.csv");

  std::unique_ptr<DataCollector> execTimeCollector(
      new DataCollector("TestFitWithTuningCollector", 28,
                        gconf_predictor_default_data_window));

  std::vector<std::vector<float>>::iterator it;

  // Keep last 30 rows to test the prediction result.
  for (it = raw_data.begin(); it != raw_data.end() - 30; ++it) {
    std::vector<float> input_v((*it).begin(), (*it).end() - 1);
    float output = (*it).back();
    execTimeCollector->AddData(input_v, output);
  }
  size_t data_size = execTimeCollector->GetInputSize();
  execTimeCollector->Shuffle(10);
  int part = static_cast<int>(data_size * 0.9);
  std::unique_ptr<DataCollector> training_data(
    execTimeCollector->CopyData(0, part));
  std::unique_ptr<DataCollector> test_data(
    execTimeCollector->CopyData(part, data_size));

  std::unique_ptr<Regression> regression(new Regression(
    gconf_predictor_default_xgboost_param_override, 20));

  regression->Fit(training_data.get(), test_data.get(), true, 60000);
  std::unique_ptr<RegressionDump> dump(new RegressionDump());
  regression->Unload(dump.get());
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  dump->Serialize(writer);
  JsonAssert(buffer.GetString(), 20, 18, 17698160.0, "6", "3");

  float expected_pred[] = {62280540.0000000000,
                           4869772.0000000000,
                           2639939.0000000000,
                           60999212.0000000000,
                           86480408.0000000000,
                           17645374.0000000000,
                           24456228.0000000000,
                           13131250.0000000000,
                           35267844.0000000000,
                           6738867.5000000000,
                           6083788.5000000000,
                           2540265.0000000000,
                           15371702.0000000000,
                           33122080.0000000000,
                           13409996.0000000000,
                           2540265.0000000000,
                           6622332.5000000000,
                           7837293.0000000000,
                           13607314.0000000000,
                           17489678.0000000000,
                           180351344.0000000000,
                           34658568.0000000000,
                           3659331.0000000000,
                           14043246.0000000000,
                           5839294.5000000000,
                           37886620.0000000000,
                           13303540.0000000000,
                           13788814.0000000000,
                           4630564.0000000000,
                           8915148.0000000000,
                           17698160.0000000000};

  for (int i = 0; it != raw_data.end(); ++it, ++i) {
    std::vector<float> input_v((*it).begin(), (*it).end() - 1);
    float pred_out = regression->PredictOne(input_v);
    CPPUNIT_ASSERT_EQUAL(expected_pred[i], pred_out);
  }
}

void TestRegression::TestFitTimeoutWithTuning() {
  std::vector<std::vector<float>> raw_data =
      LoadData("test/cppunit/predictor/exec_time_data.csv");

  std::unique_ptr<DataCollector> execTimeCollector(
      new DataCollector("TestFitTimeoutWithTuningCollector", 28,
                        gconf_predictor_default_data_window));

  std::vector<std::vector<float>>::iterator it;

  // Keep last 30 rows to test the prediction result.
  for (it = raw_data.begin(); it != raw_data.end() - 30; ++it) {
    std::vector<float> input_v((*it).begin(), (*it).end() - 1);
    float output = (*it).back();
    execTimeCollector->AddData(input_v, output);
  }
  size_t data_size = execTimeCollector->GetInputSize();
  execTimeCollector->Shuffle(10);
  int part = static_cast<int>(data_size * 0.9);
  std::unique_ptr<DataCollector> training_data(
      execTimeCollector->CopyData(0, part));
  std::unique_ptr<DataCollector> test_data(
      execTimeCollector->CopyData(part, data_size));

  // Test timeouts
  std::unique_ptr<Regression> regression(new Regression(
      gconf_predictor_default_xgboost_param_override, 20));
  // Force timeout by setting it to 0
  regression->Fit(training_data.get(), test_data.get(), true, 0);
  std::unique_ptr<RegressionDump> dump(new RegressionDump());
  regression->Unload(dump.get());
  CPPUNIT_ASSERT_EQUAL(true, dump->IsEmptyDump());
}

void TestRegression::TestLoadEmptyDump() {
  // Create empty dump.
  std::unique_ptr<RegressionDump> dump(new RegressionDump());
  // Create Regression object from empty dump which will load it.
  // There shouldn't be any segfault while creating the object.
  std::unique_ptr<Regression> regression(new Regression(dump.get()));
  // Creating new unload dump.
  std::unique_ptr<RegressionDump> unloaded_dump(new RegressionDump());
  // Unload it to make sure nothing was loaded.
  regression->Unload(unloaded_dump.get());
  // Assert that unloaded dump is empty.
  CPPUNIT_ASSERT_EQUAL(true, unloaded_dump->IsEmptyDump());
}

// We use 10 testing queries (input, output) here to build KNN
// ([1,1,...,1], 1)
// ([2,2,...,2], 2), ...,
// ([10,10,...,10], 10)
// Thus, if we ask KNN to predict for an input of
// [4,4,...,4], we should get 4.
// However, if we ask the predictor for an input of
// (11, 11, ..., 11), we will get invalid prediction (-1)
void TestRegression::TestKNearestNeighbor() {
  constexpr int input_dimension = 32;
  constexpr int number_of_unique_queries = 10;
  KNearestNeighbor knn_model;

  // Firstly, we test an empty KNN, we expect to see kUnknownPrediction.
  auto pred = knn_model.PredictOne(std::vector<DataT>(input_dimension, 1));
  CPPUNIT_ASSERT_EQUAL(pred, kUnknownPrediction);

  DataCollector collector("ContainerForBuildKnn", input_dimension, 20, false);
  int fv = 1;
  for (auto i = 0; i < number_of_unique_queries; ++i) {
    auto input = std::vector<DataT>(input_dimension, fv);
    collector.AddData(input, fv);
    // make sure query input is unique
    fv += 1;
  }
  // We add a duplicated one, and expect that the 2nd occurrence will be ignored
  // by Fit().
  collector.AddData(std::vector<DataT>(input_dimension, 1), 15);
  knn_model.Fit(&collector, nullptr /* no testing set needed */,
                false /*no tune*/, 500 /*timeout in ms*/);

  // The first query has a duplication, and the 2nd occurrence should have been
  // ignored, so we expect prediction to be the value of the 1st occurrence.
  auto dup = knn_model.PredictOne(std::vector<DataT>(input_dimension, 1));
  CPPUNIT_ASSERT_EQUAL(dup, static_cast<DataT>(1));

  // KNN should make a valid prediction as feature std::vector hits one entry.
  auto valid = knn_model.PredictOne(std::vector<DataT>(input_dimension, 4));
  CPPUNIT_ASSERT_EQUAL(valid, static_cast<DataT>(4));

  // KNN should not be able to make a prediction and return kUnknownPrediction
  // because the feature std::vector is unseen.
  auto invalid = knn_model.PredictOne(std::vector<DataT>(input_dimension, 14));
  CPPUNIT_ASSERT_EQUAL(invalid, kUnknownPrediction);

  // Test BatchPredict
  std::vector<std::vector<DataT>> inputs;
  inputs.emplace_back(input_dimension, 1);
  inputs.emplace_back(input_dimension, 5);
  inputs.emplace_back(input_dimension, 15);
  std::vector<xen::span<const DataT>> inputs_span;
  for (const auto& input : inputs) inputs_span.emplace_back(input);
  // We expect to see {1, 5, -1} as from BatchPredict.
  std::vector<DataT> predictions = knn_model.BatchPredict(inputs_span);
  CPPUNIT_ASSERT_EQUAL(predictions[0], static_cast<DataT>(1));
  CPPUNIT_ASSERT_EQUAL(predictions[1], static_cast<DataT>(5));
  CPPUNIT_ASSERT_EQUAL(predictions[2], kUnknownPrediction);
}

void TestRegression::TestQuantileSquaredErrorAndScore() {
  const DataT pred_res[] = {1.0, 2.0, 3.0, 6.0, 5.0};
  const DataT train_truth[] = {2.0, 4.0, 2.0, 3.0, 5.0};
  std::unique_ptr<DataT[], shared_mem_deleter<MtPredictor>> grad_res;
  std::unique_ptr<DataT[], shared_mem_deleter<MtPredictor>> hess_res;
  grad_res.reset(static_cast<DataT*>(
      xen::allocator::alloc(5 * sizeof(DataT), MtPredictor)));
  hess_res.reset(static_cast<DataT*>(
      xen::allocator::alloc(5 * sizeof(DataT), MtPredictor)));
  Booster::QuantileSquaredError(pred_res, train_truth, grad_res.get(),
                                hess_res.get(), 5, 0.98);
  CPPUNIT_ASSERT(fabs(grad_res[0] - static_cast<DataT>(-1.96)) < EPSILON);
  CPPUNIT_ASSERT(fabs(grad_res[1] - static_cast<DataT>(-3.92)) < EPSILON);
  CPPUNIT_ASSERT(fabs(grad_res[2] - static_cast<DataT>(0.04)) < EPSILON);
  CPPUNIT_ASSERT(fabs(grad_res[3] - static_cast<DataT>(0.12)) < EPSILON);
  CPPUNIT_ASSERT(fabs(grad_res[4] - static_cast<DataT>(0.00)) < EPSILON);
  CPPUNIT_ASSERT(fabs(hess_res[0] - static_cast<DataT>(1.96)) < EPSILON);
  CPPUNIT_ASSERT(fabs(hess_res[1] - static_cast<DataT>(1.96)) < EPSILON);
  CPPUNIT_ASSERT(fabs(hess_res[2] - static_cast<DataT>(0.04)) < EPSILON);
  CPPUNIT_ASSERT(fabs(hess_res[3] - static_cast<DataT>(0.04)) < EPSILON);
  CPPUNIT_ASSERT(fabs(hess_res[4] - static_cast<DataT>(0.04)) < EPSILON);
  DataT res = Booster::QuantileSquaredScore(pred_res, train_truth, 5, 0.98);
  CPPUNIT_ASSERT(fabs(res - static_cast<DataT>(30.2)) < EPSILON);
}

void TestRegression::TestCustomizedQuantileLoss() {
  static const char kMaxDepth[] = "max_depth";
  static const char kMinChildWeight[] = "min_child_weight";
  static const char kSubsample[] = "subsample";
  static const char kColsampleBytree[] = "colsample_bytree";
  static const char kEta[] = "eta";
  static const char kGamma[] = "gamma";
  static const char kRegAlpha[] = "reg_alpha";
  static const char kNthread[] = "nthread";
  static const char kObjective[] = "objective";
  static const char kEvalMetric[] = "eval_metric";
  static const char kQuantile[] = "quantile";
  std::vector<std::vector<float>> raw_data =
      LoadData("test/cppunit/predictor/exec_time_data.csv");

  std::unique_ptr<DataCollector> moeMemoryCollector(new DataCollector(
      "moeMemoryCollector", 28, gconf_predictor_default_data_window));

  std::vector<std::vector<float>>::iterator it;
  for (it = raw_data.begin(); it != raw_data.end() - 30; ++it) {
    std::vector<float> input_v((*it).begin(), (*it).end() - 1);
    float output = (*it).back();
    moeMemoryCollector->AddData(input_v, output);
  }
  size_t data_size = moeMemoryCollector->GetInputSize();
  int part = static_cast<int>(data_size * 0.9);
  std::unique_ptr<DataCollector> training_data(
      moeMemoryCollector->CopyData(0, part));
  std::unique_ptr<DataCollector> test_data(
      moeMemoryCollector->CopyData(part, data_size));
  decision_trees::DMatrixHandlePtr dtrain =
      Regression::CreateDMatrix(training_data.get());
  decision_trees::AutoDMatrixHandler scoped_dtrain(dtrain);
  decision_trees::DMatrixHandlePtr dtest =
      Regression::CreateDMatrix(test_data.get());
  decision_trees::AutoDMatrixHandler scoped_dtest(dtest);
  std::unique_ptr<Booster> best_booster_;
  static const std::map<string, string> xgboostDefaultHyper = {
      {kMaxDepth, "6"},
      {kMinChildWeight, "1"},
      {kSubsample, "1"},
      {kColsampleBytree, "1"},
      {kEta, "0.3"},
      {kGamma, "0"},
      {kRegAlpha, "0"},
      {kNthread, "4"},
      {kObjective, "reg:squarederror"},
      {kEvalMetric, "rmse"},
      {kQuantile, "0.5"},
  };
  static const std::map<string, string> moeDefaultHyper = {
      {kMaxDepth, "6"},
      {kMinChildWeight, "1"},
      {kSubsample, "1"},
      {kColsampleBytree, "1"},
      {kEta, "0.3"},
      {kGamma, "0"},
      {kRegAlpha, "0"},
      {kNthread, "4"},
      {kObjective, "reg:quantilesquarederror"},
      {kEvalMetric, "rmse"},
      {kQuantile, "0.5"},
  };
  Booster xgbooster(dtrain, xgboostDefaultHyper, decision_trees::KMaxBoostRound,
                    1);
  CPPUNIT_ASSERT_EQUAL(xgbooster.is_customized_qse_, false);
  decision_trees::IterScore best_iter_score_xgbooster =
      xgbooster.BuildTempModels(dtrain, dtest, best_booster_, 1000);
  Booster moebooster(dtrain, moeDefaultHyper, decision_trees::KMaxBoostRound,
                     1);
  best_booster_.reset();
  decision_trees::IterScore best_iter_score_moebooster =
      moebooster.BuildTempModels(dtrain, dtest, best_booster_, 1000);
  CPPUNIT_ASSERT_EQUAL(moebooster.is_customized_qse_, true);
  CPPUNIT_ASSERT(best_iter_score_moebooster.score !=
                 best_iter_score_xgbooster.score);
  CPPUNIT_ASSERT(best_iter_score_moebooster.iteration !=
                 best_iter_score_xgbooster.iteration);
}

}  // namespace predictor::decision_trees
