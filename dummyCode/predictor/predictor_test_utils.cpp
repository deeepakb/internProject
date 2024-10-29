#include "predictor_test_utils.hpp"

#include "predictor/predictor_main.hpp"
#include "predictor/query_predictor.hpp"
#include "xen_utils/xen_global.hpp"

#include <memory>

extern char* gconf_predictor_memory_moe_param_override;
extern char* gconf_predictor_default_xgboost_param_override;
extern char* gconf_predictor_default_moe_xgboost_param_override;

namespace predictor {

void PredictorTestUtils::SetupPredictors() {
  gconf_predictor_memory_moe_param_override = "";
  gconf_predictor_default_xgboost_param_override = "";
  gconf_predictor_default_moe_xgboost_param_override = "";

  delete Xen->predictors;
  Xen->predictors = new Predictors();

  MyPredictor = std::make_shared<PredictorClients>(Xen->predictors);
}

void PredictorTestUtils::CleanupPredictors() {
  MyPredictor.reset();
  delete Xen->predictors;
  Xen->predictors = nullptr;
}

void PredictorTestUtils::MockLocalQueryScalingPredictor(double output) {
  MyPredictor->local_query_scaling_predictor_client_ =
      std::make_unique<MockPredictorClient>(output);
}

}  // namespace predictor
