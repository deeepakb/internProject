/*
 * Copyright(c) Amazon.com, Inc. or its affiliates, 2017.  All rights reserved.
 */
#include "awsclient/awscredentialsprovider.hpp"
#include "s3client/s3client.hpp"
#include "sys/traffic.hpp"
#include "sys/xen_aws_credentials_mgr.hpp"
#include "xen_test_utils/test_instance_metadata_setup.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <curl/curl.h>

#include <string>

#include <unistd.h>

size_t writefunc(void *contents, size_t size, size_t nmemb, void* userp);

class S3GeneratePresignedUrl_test : public CPPUNIT_NS::TestFixture {
 public:
  void setUp();
  void tearDown();

 private:
  CPPUNIT_TEST_SUITE(S3GeneratePresignedUrl_test);
  CPPUNIT_TEST(positive_test);
  CPPUNIT_TEST(negative_test);
  CPPUNIT_TEST_SUITE_END();

 protected:
  void positive_test();
  void negative_test();
  // Run curl url and return output.
  std::string getContent(const std::string& url);
};

CPPUNIT_TEST_SUITE_REGISTRATION(S3GeneratePresignedUrl_test);

extern char* gconf_hosm_uri;

void S3GeneratePresignedUrl_test::setUp() {
  instance_metadata_provider::Setup();
}

void S3GeneratePresignedUrl_test::tearDown() {
  instance_metadata_provider::TearDown();
}

void S3GeneratePresignedUrl_test::positive_test() {
  const char* bucket = "cookie-monster-s3-ingestion";
  const char* key = "stsclient/small.txt";
  int64 expiration_ = 15 * 60;
  const char* res = "1,1\n2,2\n3,3";
  aws::auth::AWSCredentialsProviderPtr credsptr_ =
      XenAWSCredentialsMgr::defaultCredentialProviderChain("", "rdsdb");

  aws::ClientConfiguration config("us-east-1");
  aws::auth::AWSCredentials creds = credsptr_->getAWSCredentials();
  aws::S3Client s3client(credsptr_, config, traffic_type::kTest);

  // Curl the presigned url and compare output with what we expected.
  // This can verify that the presigned url is working.
  std::string url =
    s3client.GeneratePresignedUrl(creds,
                                  bucket, key, expiration_);
  std::string get = getContent(url);
  CPPUNIT_ASSERT(get.compare(res) == 0);

  // Test getDate function.
  std::string date = s3client.GetDate("%Y%m%d");
  CPPUNIT_ASSERT(date.length() == 8);
  date = s3client.GetDate("%Y%m%dT%H%M%SZ");
  CPPUNIT_ASSERT(date.length() == 16);
}

void S3GeneratePresignedUrl_test::negative_test() {
  const char* bucket = "cookie-monster-s3-ingestion";
  const char* key = "stsclient/small.txt";
  int64 expiration_ = 15 * 60;
  aws::auth::AWSCredentialsProviderPtr credsptr_ =
      XenAWSCredentialsMgr::defaultCredentialProviderChain("", "rdsdb");

  // Test: wrong region.
  aws::ClientConfiguration wrong_region_config("wrong_region");
  aws::S3Client s3_wrong_region(credsptr_, wrong_region_config,
                                traffic_type::kTest);
  std::string url =
    s3_wrong_region.GeneratePresignedUrl(credsptr_->getAWSCredentials(),
                                         bucket, key, expiration_);
  std::string get = getContent(url);
  std::string error("the region 'wrong_region' is wrong");
  CPPUNIT_ASSERT(get.find(error) != std::string::npos);

  // Test: Object not existing.
  aws::ClientConfiguration config("us-east-1");
  aws::S3Client s3(credsptr_, config, traffic_type::kTest);

  url = s3.GeneratePresignedUrl(credsptr_->getAWSCredentials(),
                                bucket, "no_such_file", expiration_);
  get = getContent(url);
  error = "The specified key does not exist";
  CPPUNIT_ASSERT(get.find(error) != std::string::npos);

  // Test: wrong credentials.
  aws::auth::AWSCredentials fake_creds("fake_access_key", "fake_secret_key");
  aws::S3Client s3client("fake_access_key", "fake_secret_key",
                         config, traffic_type::kTest);
  url = s3client.GeneratePresignedUrl(fake_creds, bucket,
                                                  key, expiration_);
  get = getContent(url);
  error = "The AWS Access Key Id you provided does not exist";

  CPPUNIT_ASSERT(get.find(error) != std::string::npos);

  // Test: expiration negative.
  url = s3client.GeneratePresignedUrl(credsptr_->getAWSCredentials(),
                                      bucket, key, -100);
  get = getContent(url);
  error = "X-Amz-Expires must be non-negative";
  CPPUNIT_ASSERT(get.find(error) != std::string::npos);

  // Test: url expired.
  url = s3client.GeneratePresignedUrl(credsptr_->getAWSCredentials(),
                                      bucket, key, 0);
  get = getContent(url);
  error = "Request has expired";
  CPPUNIT_ASSERT(get.find(error) != std::string::npos);

  // Test: url will expire in 5 seconds.
  // Wait for 10 seconds and try download.
  url = s3client.GeneratePresignedUrl(credsptr_->getAWSCredentials(),
                                      bucket, key, 5);
  usleep(10 * 1000 * 1000);
  get = getContent(url);
  error = "Request has expired";
  CPPUNIT_ASSERT(get.find(error) != std::string::npos);
}

std::string S3GeneratePresignedUrl_test::getContent(const std::string& url) {
  CURL* curl = curl_easy_init();
  std::string content;

  if (curl) {
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &content);
    curl_easy_perform(curl);

    curl_easy_cleanup(curl);
  } else {
    // curl error
    CPPUNIT_ASSERT(false);
  }
  return content;
}

size_t writefunc(void *contents, size_t size, size_t nmemb, void* userp) {
  ((std::string*)userp)->append(reinterpret_cast<char*>(contents),
                                size * nmemb);
  return size * nmemb;
}
