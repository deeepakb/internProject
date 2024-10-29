#include <cppunit/BriefTestProgressListener.h>
#include <cppunit/CompilerOutputter.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/TestRunner.h>
#include <cppunit/config/SourcePrefix.h>
#include <cppunit/extensions/TestFactoryRegistry.h>

// clang-format off
#include "test_percentile_util_functions.hpp"
#include "sys/step_common.hpp"
#include "sys/step_window.hpp"
#include "nodes/makefuncs.h"
#include "sys/pg_utils.hpp"
#include "catalog/pg_aggregate.h"
#include "parser/parse_agg.h"
#include "parser/parse_func.h"
#include "funcs/funcs_int.hpp"
#include "utils/numeric.h"
#include "sys/numeric_utils.h"
// clang-format on

CPPUNIT_TEST_SUITE_REGISTRATION(Test_Percentile_Util_Functions);

void Test_Percentile_Util_Functions::test_is_percentile_agg_function() {
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_agg(AGG_INT8_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_agg(AGG_INT4_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_agg(AGG_INT2_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_agg(AGG_FLOAT4_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_agg(AGG_FLOAT8_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_agg(AGG_TIMESTAMP_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(
    true, Plan_window_step::is_percentile_agg(AGG_TIMESTAMPTZ_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(
    true, Plan_window_step::is_percentile_agg(AGG_INTERVALY2M_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(
    true, Plan_window_step::is_percentile_agg(AGG_INTERVALD2S_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(
    true, Plan_window_step::is_percentile_agg(AGG_TIME_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(
    true, Plan_window_step::is_percentile_agg(AGG_TIMETZ_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_agg(AGG_NUMERIC_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_agg(AGG_DATE_PERCENTILE_CONT));

  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_agg(AGG_INT8_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_agg(AGG_INT4_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_agg(AGG_INT2_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_agg(AGG_NUMERIC_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_agg(AGG_FLOAT4_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_agg(AGG_FLOAT8_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_agg(AGG_DATE_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_agg(AGG_TIMESTAMP_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(
      true,
      Plan_window_step::is_percentile_agg(AGG_INTERVALY2M_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(
      true,
      Plan_window_step::is_percentile_agg(AGG_INTERVALD2S_PERCENTILE_DISC));

  CPPUNIT_ASSERT_EQUAL(
      false, Plan_window_step::is_percentile_agg(AGG_VARCHAR_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(
      false, Plan_window_step::is_percentile_agg(AGG_CHAR_PERCENTILE_DISC));

  CPPUNIT_ASSERT_EQUAL(false,
                       Plan_window_step::is_percentile_agg(AGG_INT8_SUM));
  CPPUNIT_ASSERT_EQUAL(
      false, Plan_window_step::is_percentile_agg(AGG_FLOAT_RATIO_TO_REPORT));
  CPPUNIT_ASSERT_EQUAL(
      false, Plan_window_step::is_percentile_agg(AGG_QSUMMARY_MERGE_FLOAT8));
}

void Test_Percentile_Util_Functions::test_is_percentile_cont_agg_function() {
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_cont_agg(AGG_INT8_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_cont_agg(AGG_INT4_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_cont_agg(AGG_INT2_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(true, Plan_window_step::is_percentile_cont_agg(
                                 AGG_FLOAT4_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(true, Plan_window_step::is_percentile_cont_agg(
                                 AGG_FLOAT8_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(true, Plan_window_step::is_percentile_cont_agg(
                                 AGG_TIMESTAMP_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(true, Plan_window_step::is_percentile_cont_agg(
                                 AGG_TIMESTAMPTZ_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(true, Plan_window_step::is_percentile_cont_agg(
                                 AGG_INTERVALY2M_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(true, Plan_window_step::is_percentile_cont_agg(
                                 AGG_INTERVALD2S_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(true, Plan_window_step::is_percentile_cont_agg(
                                 AGG_TIME_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(true, Plan_window_step::is_percentile_cont_agg(
                                 AGG_TIMETZ_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(true, Plan_window_step::is_percentile_cont_agg(
                                 AGG_NUMERIC_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_cont_agg(AGG_DATE_PERCENTILE_CONT));

  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_cont_agg(
                                  AGG_INT8_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_cont_agg(
                                  AGG_INT4_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_cont_agg(
                                  AGG_INT2_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_cont_agg(
                                  AGG_NUMERIC_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_cont_agg(
                                  AGG_FLOAT4_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_cont_agg(
                                  AGG_FLOAT8_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_cont_agg(
                                  AGG_DATE_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_cont_agg(
                                  AGG_TIMESTAMP_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_cont_agg(
                                  AGG_TIMESTAMPTZ_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_cont_agg(
                                  AGG_INTERVALY2M_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_cont_agg(
                                  AGG_INTERVALD2S_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_cont_agg(
                                  AGG_VARCHAR_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_cont_agg(
                                  AGG_CHAR_PERCENTILE_DISC));

  CPPUNIT_ASSERT_EQUAL(false,
                       Plan_window_step::is_percentile_cont_agg(AGG_INT8_SUM));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_cont_agg(
                                  AGG_FLOAT_RATIO_TO_REPORT));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_cont_agg(
                                  AGG_QSUMMARY_MERGE_FLOAT8));
}

void Test_Percentile_Util_Functions::test_is_percentile_agg_dist_function() {
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_disc_agg(
                                  AGG_INT8_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_disc_agg(
                                  AGG_INT4_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_disc_agg(
                                  AGG_INT2_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_disc_agg(
                                  AGG_FLOAT4_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_disc_agg(
                                  AGG_FLOAT8_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_disc_agg(
                                  AGG_TIMESTAMP_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_disc_agg(
                                  AGG_TIMESTAMPTZ_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_disc_agg(
                                  AGG_INTERVALY2M_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_disc_agg(
                                  AGG_INTERVALD2S_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_disc_agg(
                                  AGG_TIME_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_disc_agg(
                                  AGG_TIMETZ_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_disc_agg(
                                  AGG_NUMERIC_PERCENTILE_CONT));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_disc_agg(
                                  AGG_DATE_PERCENTILE_CONT));

  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_disc_agg(AGG_INT8_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_disc_agg(AGG_INT4_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_disc_agg(AGG_INT2_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(true, Plan_window_step::is_percentile_disc_agg(
                                 AGG_NUMERIC_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(true, Plan_window_step::is_percentile_disc_agg(
                                 AGG_FLOAT4_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(true, Plan_window_step::is_percentile_disc_agg(
                                 AGG_FLOAT8_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(
      true, Plan_window_step::is_percentile_disc_agg(AGG_DATE_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(true, Plan_window_step::is_percentile_disc_agg(
                                 AGG_TIMESTAMP_PERCENTILE_DISC));

  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_disc_agg(
                                  AGG_VARCHAR_PERCENTILE_DISC));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_disc_agg(
                                  AGG_CHAR_PERCENTILE_DISC));

  CPPUNIT_ASSERT_EQUAL(false,
                       Plan_window_step::is_percentile_disc_agg(AGG_INT8_SUM));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_disc_agg(
                                  AGG_FLOAT_RATIO_TO_REPORT));
  CPPUNIT_ASSERT_EQUAL(false, Plan_window_step::is_percentile_disc_agg(
                                  AGG_QSUMMARY_MERGE_FLOAT8));
}

void Test_Percentile_Util_Functions::test_get_percentile_agg_function_size() {
  Const* p99 = makeConst(NUMERICOID, sizeof(int128),
                         xen2pg_numeric128(99, numeric_size(2, 0)),
                         false, true);
  Const* p99_9 = makeConst(NUMERICOID, sizeof(int128),
                           xen2pg_numeric128(999, numeric_size(3, 1)),
                           false, true);
  Const* p99_99 = makeConst(NUMERICOID, sizeof(int128),
                            xen2pg_numeric128(9999, numeric_size(4, 2)),
                            false, true);

  // int, col type short
  CPPUNIT_ASSERT_EQUAL(numeric_size(INT2_MAX_PRECISION, 0),
                       Plan_common_step::get_percentile_function_size(
                         AGG_INT8_PERCENTILE_CONT, p99, -1 /*invalid for int*/,
                         ColTypeShort));
  CPPUNIT_ASSERT_EQUAL(numeric_size(INT2_MAX_PRECISION + 1, 1),
                       Plan_common_step::get_percentile_function_size(
                         AGG_INT8_PERCENTILE_CONT, p99_9,
                         -1 /*invalid for int*/, ColTypeShort));
  CPPUNIT_ASSERT_EQUAL(numeric_size(INT2_MAX_PRECISION + 2, 2),
                       Plan_common_step::get_percentile_function_size(
                         AGG_INT8_PERCENTILE_CONT, p99_99,
                         -1 /*invalid for int*/, ColTypeShort));

  // int, col type int
  CPPUNIT_ASSERT_EQUAL(numeric_size(INT4_MAX_PRECISION, 0),
                       Plan_common_step::get_percentile_function_size(
                         AGG_INT8_PERCENTILE_CONT, p99,
                         -1 /*invalid for int*/, ColTypeInt));
  CPPUNIT_ASSERT_EQUAL(numeric_size(INT4_MAX_PRECISION + 1, 1),
                       Plan_common_step::get_percentile_function_size(
                         AGG_INT8_PERCENTILE_CONT, p99_9,
                         -1 /*invalid for int*/, ColTypeInt));
  CPPUNIT_ASSERT_EQUAL(numeric_size(INT4_MAX_PRECISION + 2, 2),
                       Plan_common_step::get_percentile_function_size(
                         AGG_INT8_PERCENTILE_CONT, p99_99,
                         -1 /*invalid for int*/, ColTypeInt));

  // int, col type long
  CPPUNIT_ASSERT_EQUAL(numeric_size(INT8_MAX_PRECISION, 0),
                       Plan_common_step::get_percentile_function_size(
                         AGG_INT8_PERCENTILE_CONT, p99, -1 /*invalid for int*/,
                         ColTypeLong));
  CPPUNIT_ASSERT_EQUAL(numeric_size(INT8_MAX_PRECISION + 1, 1),
                       Plan_common_step::get_percentile_function_size(
                         AGG_INT8_PERCENTILE_CONT, p99_9,
                         -1 /*invalid for int*/, ColTypeLong));
  CPPUNIT_ASSERT_EQUAL(numeric_size(INT8_MAX_PRECISION + 2, 2),
                       Plan_common_step::get_percentile_function_size(
                           AGG_INT8_PERCENTILE_CONT, p99_99,
                           -1 /*invalid for int*/, ColTypeLong));

  // float
  CPPUNIT_ASSERT_EQUAL(0, Plan_common_step::get_percentile_function_size(
                            AGG_FLOAT4_MEDIAN, p99,
                            -1 /*doesn't matter for float*/, ColTypeFloat));
  CPPUNIT_ASSERT_EQUAL(0, Plan_common_step::get_percentile_function_size(
                            AGG_FLOAT4_MEDIAN, p99_9,
                            -1 /*doesn't matter for float*/, ColTypeFloat));
  CPPUNIT_ASSERT_EQUAL(0, Plan_common_step::get_percentile_function_size(
                            AGG_FLOAT4_MEDIAN, p99_99,
                            -1 /*doesn't matter for float*/, ColTypeFloat));

  // numeric(x,y)
  CPPUNIT_ASSERT_EQUAL(
      numeric_size(5, 0),
      Plan_common_step::get_percentile_function_size(
        AGG_NUMERIC_PERCENTILE_CONT, p99, numeric_size(5, 0),
        ColType01 /*invalid for numeric*/));
  CPPUNIT_ASSERT_EQUAL(
      numeric_size(6, 1),
      Plan_common_step::get_percentile_function_size(
        AGG_NUMERIC_PERCENTILE_CONT, p99_9, numeric_size(5, 0),
        ColType01 /*invalid for numeric*/));
  CPPUNIT_ASSERT_EQUAL(
      numeric_size(7, 4),
      Plan_common_step::get_percentile_function_size(
        AGG_NUMERIC_PERCENTILE_CONT, p99_99, numeric_size(5, 2),
        ColType01 /*invalid for numeric*/));
  CPPUNIT_ASSERT_EQUAL(
      numeric_size(38, 2),
      Plan_common_step::get_percentile_function_size(
        AGG_NUMERIC_PERCENTILE_CONT, p99_99, numeric_size(38, 2),
        ColType01 /*invalid for numeric*/));
}

void Test_Percentile_Util_Functions::test_get_const_decimal_value() {
  Const* cons = makeConst(NUMERICOID, sizeof(int128),
                          xen2pg_numeric128(156, numeric_size(3, 2)),
                          false, true);
  int size = 0;
  int128 val = pg2xen_numeric(cons->constvalue, &size);
  CPPUNIT_ASSERT(val == 156);
  CPPUNIT_ASSERT_EQUAL(2, numeric_scale(size));
}
