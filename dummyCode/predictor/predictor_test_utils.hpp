#pragma once

#include "predictor/value_predictor.hpp"

namespace predictor {

class MockPredictorClient : public ValuePredictorClient {
 public:
  MockPredictorClient(double output)
      : ValuePredictorClient("TestClient", nullptr), prediction_(output) {}

  double KnnPredict(xen::span<const DataT>) override { return prediction_; }

  double prediction_;
};

class PredictorTestUtils {
 public:
  static void SetupPredictors();
  static void CleanupPredictors();
  static void MockLocalQueryScalingPredictor(double output);
};

}  // namespace predictor
