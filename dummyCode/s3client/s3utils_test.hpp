#pragma once

#include <cppunit/extensions/HelperMacros.h>

class S3Utils_test : public CPPUNIT_NS::TestFixture
{
private:
  CPPUNIT_TEST_SUITE( S3Utils_test );
  CPPUNIT_TEST(test_backoff);
  CPPUNIT_TEST(test_backoff_slowdown);
  CPPUNIT_TEST(test_s3_list_xml_versioned_response);
  CPPUNIT_TEST(test_s3_list_xml_unversioned_response);
  CPPUNIT_TEST(test_s3_list_xml_common_prefix_response);
  CPPUNIT_TEST(test_s3_list_v2_xml_full_response);
  CPPUNIT_TEST(test_s3_list_v2_xml_truncated_response);
  CPPUNIT_TEST(test_s3_list_v2_xml_empty_truncated_response);
  CPPUNIT_TEST(test_s3_list_v2_xml_continuation_response);
  CPPUNIT_TEST(test_s3_list_v2_common_prefix_response);
  CPPUNIT_TEST(test_s3_summary_etag);
  CPPUNIT_TEST_SUITE_END();

protected:
  void test_backoff();
  void test_backoff_slowdown();
  void test_s3_list_xml_versioned_response();
  void test_s3_list_xml_unversioned_response();
  void test_s3_list_xml_common_prefix_response();
  void test_s3_list_v2_xml_full_response();
  void test_s3_list_v2_xml_truncated_response();
  void test_s3_list_v2_xml_empty_truncated_response();
  void test_s3_list_v2_xml_continuation_response();
  void test_s3_list_v2_common_prefix_response();
  void test_s3_summary_etag();
};

