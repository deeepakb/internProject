#include "s3utils_test.hpp"

#include "s3client/s3client.hpp"
#include "s3client/s3listxmlresponse.hpp"
#include "s3client/s3utils.hpp"
#include "xen_utils/file_utils.hpp"
#include "xen_utils/xen_global.hpp"

#include <unordered_set>

#include <errno.h>
#include <unistd.h>

CPPUNIT_TEST_SUITE_REGISTRATION( S3Utils_test );

using namespace aws;

void S3Utils_test::test_backoff() {
  int64_t duration_in_ms = 0;
  for(int i = 0; i < 10; i++) {
    duration_in_ms = backoffDuration(i, false);
    /** max backoff is 20000ms
     * base backoff is 500 and 0 <= jitter =>125
     */
    int64_t lower = std::min((500*(1<<i)),20000);
    int64_t higher = std::min((625*(1<<i)),20000);
    CPPUNIT_ASSERT(lower <= duration_in_ms && duration_in_ms <= higher);
  }
}

void S3Utils_test::test_backoff_slowdown() {
  int64_t duration_in_ms = 0;
  for (int i = 0; i < 10; i++) {
    duration_in_ms = backoffDuration(i, true);
    int64_t lower = std::min(1000 + (500 * (1 << i)), 20000);
    // Additional jitter is 500*i for added retries.
    int64_t higher = std::min(1000 + ((500 * i + 625) * (1 << i)), 20000);
    std::cout << lower << " " << higher << std::endl;
    CPPUNIT_ASSERT(lower <= duration_in_ms && duration_in_ms <= higher);
  }
}

// This test checks that for version-ed xml response, following fields are
// correctly parsed from the xml.
// - NextKeyMarker
// - NextVersionIdMarker
// - the bucket name
// - is truncated is correctly set.
// - For each individual object
//    - The object key
//    - object version
//    - etag
//    - size
//  It also checks that only the latest object is added to the vector of
//  S3ObjectSummary.
void S3Utils_test::test_s3_list_xml_versioned_response() {
  std::string filepath(XenDir);
  filepath += "/test/cppunit/s3client/versioned_response.xml";
  std::string content = ReadFile(filepath);
  std::vector<S3ObjectSummary> s3ObjectSummaries;
  S3ListXmlResponse response(content.c_str(), content.size(),
                             &s3ObjectSummaries, nullptr, std::nullopt);
  CPPUNIT_ASSERT_EQUAL(true, response.isTruncated());
  CPPUNIT_ASSERT_EQUAL(
      std::string("stls/over_1000/stl_compile_info/"
                  "stl_compile_info_6411_padbmaster.597981298039961.gz"),
      response.getNextKey());
  CPPUNIT_ASSERT_EQUAL(std::string("wS5BE5En33hGOBqYj8T6U1_bKGRwowJE"),
                       response.getNextVersion());

  CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(2), s3ObjectSummaries.size());

  S3ObjectSummary summary = s3ObjectSummaries[0];
  CPPUNIT_ASSERT_EQUAL(std::string("stls/over_1000/stl_compile_info/"),
                       summary.getKey());
  CPPUNIT_ASSERT_EQUAL(std::string("uRzpZguyOcPXaac0QLSG9v.m4jglvEat"),
                       summary.getVersion());
  CPPUNIT_ASSERT_EQUAL(std::string("d41d8cd98f00b204e9800998ecf8427e"),
                       summary.getETag());
  CPPUNIT_ASSERT_EQUAL(std::string("versioned-test-bucket-dp"),
                       summary.getBucket());
  CPPUNIT_ASSERT_EQUAL(static_cast<int64_t>(0), summary.getSize());
  CPPUNIT_ASSERT_EQUAL(1544667289l, summary.GetLastModified());

  summary = s3ObjectSummaries[1];
  CPPUNIT_ASSERT_EQUAL(
      std::string("stls/over_1000/stl_compile_info/"
                  "stl_compile_info_6411_padbmaster.597981298039961.gz"),
      summary.getKey());
  CPPUNIT_ASSERT_EQUAL(std::string("Q1nxSHlOt4wPj8v91hk3_T_5zIDS9Kug"),
                       summary.getVersion());
  CPPUNIT_ASSERT_EQUAL(std::string("7dca678a68c691ebdd1fc7c2103c04e9"),
                       summary.getETag());
  CPPUNIT_ASSERT_EQUAL(std::string("versioned-test-bucket-dp"),
                       summary.getBucket());
  CPPUNIT_ASSERT_EQUAL(static_cast<int64_t>(200), summary.getSize());
  CPPUNIT_ASSERT_EQUAL(1544677726l, summary.GetLastModified());
}

void S3Utils_test::test_s3_list_xml_unversioned_response() {
  std::string filepath(XenDir);
  filepath += "/test/cppunit/s3client/unversioned_response.xml";
  std::string content = ReadFile(filepath);
  std::vector<S3ObjectSummary> s3ObjectSummaries;
  S3ListXmlResponse response(content.c_str(), content.size(),
                             &s3ObjectSummaries, nullptr, std::nullopt);
  CPPUNIT_ASSERT_EQUAL(false, response.isTruncated());
  CPPUNIT_ASSERT_EQUAL(std::string(""), response.getNextKey());
  CPPUNIT_ASSERT_EQUAL(std::string(""), response.getNextVersion());
  CPPUNIT_ASSERT_EQUAL(std::string(""), response.getNextContinuationToken());

  CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(2), s3ObjectSummaries.size());

  S3ObjectSummary summary = s3ObjectSummaries[0];
  CPPUNIT_ASSERT_EQUAL(std::string("alldatatypes_csv/alldatatypes.csv"),
                       summary.getKey());
  CPPUNIT_ASSERT_EQUAL(std::string(""), summary.getVersion());
  CPPUNIT_ASSERT_EQUAL(std::string("5d9aa9685ad4597dd5f549db54dd12f1"),
                       summary.getETag());
  CPPUNIT_ASSERT_EQUAL(std::string("dory-data"), summary.getBucket());
  CPPUNIT_ASSERT_EQUAL(static_cast<int64_t>(223281), summary.getSize());
  CPPUNIT_ASSERT_EQUAL(1490588655l, summary.GetLastModified());

  summary = s3ObjectSummaries[1];
  CPPUNIT_ASSERT_EQUAL(std::string("alldatatypes_csv/alldatatypes1.csv"),
                       summary.getKey());
  CPPUNIT_ASSERT_EQUAL(std::string(""), summary.getVersion());
  CPPUNIT_ASSERT_EQUAL(std::string("5d9aa9685ad4597dd5f549db54dd12f1"),
                       summary.getETag());
  CPPUNIT_ASSERT_EQUAL(std::string("dory-data"), summary.getBucket());
  CPPUNIT_ASSERT_EQUAL(static_cast<int64_t>(223282), summary.getSize());
  CPPUNIT_ASSERT_EQUAL(1490588655l, summary.GetLastModified());
}

void S3Utils_test::test_s3_list_xml_common_prefix_response() {
  std::string filepath(XenDir);
  filepath += "/test/cppunit/s3client/common_prefix_response.xml";
  std::string content = ReadFile(filepath);
  std::vector<S3ObjectCommonPrefix> s3_object_common_prefix;
  S3ListXmlResponse response(content.c_str(), content.size(), nullptr,
                             &s3_object_common_prefix, std::nullopt);
  CPPUNIT_ASSERT_EQUAL(false, response.isTruncated());
  CPPUNIT_ASSERT_EQUAL(std::string(""), response.getNextKey());
  CPPUNIT_ASSERT_EQUAL(std::string(""), response.getNextVersion());
  CPPUNIT_ASSERT_EQUAL(std::string(""), response.getNextContinuationToken());

  CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(2), s3_object_common_prefix.size());

  S3ObjectCommonPrefix common_prefix = s3_object_common_prefix[0];
  CPPUNIT_ASSERT_EQUAL(
      std::string("jdJqyn8l/qa/701655/stl_aggr/cs/20191230/701655/raw/"),
      common_prefix.get_prefix());

  common_prefix = s3_object_common_prefix[1];
  CPPUNIT_ASSERT_EQUAL(
      std::string("jdJqyn8l/qa/701655/stl_aggr/main/20191231/701655/raw/"),
      common_prefix.get_prefix());
}

void S3Utils_test::test_s3_list_v2_xml_full_response() {
  std::string filepath(XenDir);
  filepath += "/test/cppunit/s3client/list-objects-v2-not-truncated.xml";
  std::string content = ReadFile(filepath);
  std::vector<S3ObjectSummary> s3ObjectSummaries;
  S3ListXmlResponse response(content.c_str(), content.size(),
                             &s3ObjectSummaries, nullptr, std::nullopt);
  CPPUNIT_ASSERT_EQUAL(false, response.isTruncated());
  CPPUNIT_ASSERT_EQUAL(std::string(""), response.getNextKey());
  CPPUNIT_ASSERT_EQUAL(std::string(""), response.getNextVersion());
  CPPUNIT_ASSERT_EQUAL(std::string(""), response.getNextContinuationToken());

  CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(1), s3ObjectSummaries.size());

  auto summary = s3ObjectSummaries[0];
  CPPUNIT_ASSERT_EQUAL(std::string("dory-data"), summary.getBucket());
  CPPUNIT_ASSERT_EQUAL(std::string("STANDARD"), summary.getStorageClass());
  CPPUNIT_ASSERT_EQUAL(std::string(""), summary.getVersion());
  CPPUNIT_ASSERT_EQUAL(static_cast<int64_t>(106258), summary.getSize());
  CPPUNIT_ASSERT_EQUAL(std::string("509864c48129fbb2df505fba7a2d6061"),
                       summary.getETag());
  CPPUNIT_ASSERT_EQUAL(1490589264l, summary.GetLastModified());
  CPPUNIT_ASSERT_EQUAL(
      std::string(
          "alldatatypes_parquet/f9d1890f-9cd8-4ada-a902-2efabd3b3f26-000000"),
      summary.getKey());
}

void S3Utils_test::test_s3_list_v2_xml_truncated_response() {
  std::string filepath(XenDir);
  filepath += "/test/cppunit/s3client/list-objects-v2-truncated.xml";
  std::string content = ReadFile(filepath);
  std::vector<S3ObjectSummary> s3ObjectSummaries;
  S3ListXmlResponse response(content.c_str(), content.size(),
                             &s3ObjectSummaries, nullptr, std::nullopt);
  CPPUNIT_ASSERT_EQUAL(true, response.isTruncated());
  CPPUNIT_ASSERT_EQUAL(std::string(""), response.getNextKey());
  CPPUNIT_ASSERT_EQUAL(std::string(""), response.getNextVersion());

  CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(10), s3ObjectSummaries.size());
  CPPUNIT_ASSERT_EQUAL(
      std::string("1Qe22ByfH6BBK4O31T5p/Sj55354KPW9Gr2m7Sg/"
                  "4I2vMa27hx5Us4UlRv2yb5C6xgelTZ"
                  "D18T95kqAVER+d0VyqVL3/"
                  "dqfOrWMYkk8JAQUgsEIUpohtj4R4rZNUfxDDKVyKLxcya8dI="),
      response.getNextContinuationToken());

  std::unordered_set<std::string> keySet;
  for (auto& summary : s3ObjectSummaries) {
    keySet.insert(summary.getKey());
    CPPUNIT_ASSERT_EQUAL(std::string("dory-data"), summary.getBucket());
    CPPUNIT_ASSERT_EQUAL(std::string("STANDARD"), summary.getStorageClass());
    CPPUNIT_ASSERT_EQUAL(std::string(""), summary.getVersion());
    if (summary.getSize() == 0L) {
      continue;
    }
    CPPUNIT_ASSERT_EQUAL(static_cast<int64_t>(106258), summary.getSize());
    CPPUNIT_ASSERT_EQUAL(std::string("509864c48129fbb2df505fba7a2d6061"),
                         summary.getETag());
  }
  CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(10), keySet.size());
}

void S3Utils_test::test_s3_list_v2_xml_empty_truncated_response() {
  std::string filepath(XenDir);
  filepath += "/test/cppunit/s3client/list-objects-v2-empty-truncated.xml";
  std::string content = ReadFile(filepath);
  std::vector<S3ObjectSummary> s3ObjectSummaries;
  S3ListXmlResponse response(content.c_str(), content.size(),
                             &s3ObjectSummaries, nullptr, std::nullopt);
  CPPUNIT_ASSERT_EQUAL(true, response.isTruncated());
  CPPUNIT_ASSERT_EQUAL(std::string(""), response.getNextKey());
  CPPUNIT_ASSERT_EQUAL(std::string(""), response.getNextVersion());

  CPPUNIT_ASSERT_EQUAL(true, s3ObjectSummaries.empty());
  CPPUNIT_ASSERT_EQUAL(
      std::string("1mox99+1lDN20aYeT9zszfAHvvGXp1XSWooZ2Q1NvtKaeAEmpo7BzrLTBJ"
                  "l606wZcpKbw/8HGWDW4lYJRqXZMf9ZrgRm8TJ/bQazlZMWZ5sIuQRMl7xP"
                  "IXwOhAJwOfSeLGP0CG5D39igyC8HOCYLOuVpPLe8yprAddbGIgG+APcoax"
                  "qEnLJIyme3XHkqWlCcl8T/9n9hQ8MtlZJIs3/94gRxMGrJTvCnSh5RXSPm"
                  "brtQ="),
      response.getNextContinuationToken());
}

void S3Utils_test::test_s3_list_v2_xml_continuation_response() {
  std::string filepath(XenDir);
  filepath += "/test/cppunit/s3client/list-objects-v2-continuation-token.xml";
  std::string content = ReadFile(filepath);
  std::vector<S3ObjectSummary> s3ObjectSummaries;
  S3ListXmlResponse response(content.c_str(), content.size(),
                             &s3ObjectSummaries, nullptr, "");
  CPPUNIT_ASSERT_EQUAL(true, response.isTruncated());
  CPPUNIT_ASSERT_EQUAL(std::string(""), response.getNextKey());
  CPPUNIT_ASSERT_EQUAL(std::string(""), response.getNextVersion());

  CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(10), s3ObjectSummaries.size());
  CPPUNIT_ASSERT_EQUAL(
      std::string("15BnlBuNUewsUKQnzrvIqo/"
                  "ST4WR6jlnog6UzFm5XFGF5wSzL3KDrLhgyuvYffvyDXQ4Bd"
                  "LJuO3vaCqTEnmCyHKzHcE1z0/LbHa1od9uPdW2xwElSZiPMqLM/"
                  "dW5uBbam32lX91GyrSA="),
      response.getNextContinuationToken());

  std::unordered_set<std::string> keySet;
  for (auto& summary : s3ObjectSummaries) {
    keySet.insert(summary.getKey());
    CPPUNIT_ASSERT_EQUAL(std::string("dory-data"), summary.getBucket());
    CPPUNIT_ASSERT_EQUAL(std::string("STANDARD"), summary.getStorageClass());
    CPPUNIT_ASSERT_EQUAL(std::string(""), summary.getVersion());
    CPPUNIT_ASSERT_EQUAL(static_cast<int64_t>(106258), summary.getSize());
    CPPUNIT_ASSERT_EQUAL(std::string("509864c48129fbb2df505fba7a2d6061"),
                         summary.getETag());
  }
  CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(10), keySet.size());
}

void S3Utils_test::test_s3_list_v2_common_prefix_response() {
  std::string filepath(XenDir);
  filepath += "/test/cppunit/s3client/list-objects-v2-common-prefixes.xml";
  std::string content = ReadFile(filepath);
  std::vector<S3ObjectCommonPrefix> s3ObjectCommonPrefixes;
  S3ListXmlResponse response(content.c_str(), content.size(), nullptr,
                             &s3ObjectCommonPrefixes, std::nullopt);
  CPPUNIT_ASSERT_EQUAL(false, response.isTruncated());
  CPPUNIT_ASSERT_EQUAL(std::string(""), response.getNextKey());
  CPPUNIT_ASSERT_EQUAL(std::string(""), response.getNextVersion());
  CPPUNIT_ASSERT_EQUAL(std::string(""), response.getNextContinuationToken());

  CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(1), s3ObjectCommonPrefixes.size());

  auto prefix = s3ObjectCommonPrefixes[0];

  CPPUNIT_ASSERT_EQUAL(std::string("stls/over_1000/stl_compile_info/"),
                       prefix.get_prefix());
}

void S3Utils_test::test_s3_summary_etag() {
  auto summary1 = aws::S3ObjectSummary("bucket", "filename", 0,
                                       "509864c48129fbb2df505fba7a2d6061");
  CPPUNIT_ASSERT_EQUAL(summary1.getETag(),
                       std::string("509864c48129fbb2df505fba7a2d6061"));

  auto summary2 = aws::S3ObjectSummary("bucket", "filename", 0, "NoEtag");
  CPPUNIT_ASSERT_EQUAL(summary2.getETag(), std::string("NoEtag"));

  auto summary3 = aws::S3ObjectSummary("bucket", "filename", 0, "");
  CPPUNIT_ASSERT_EQUAL(summary3.getETag(), std::string(""));

  auto summary4 = aws::S3ObjectSummary("bucket", "filename", 0,
                                       "\"509864c48129fbb2df505fba7a2d6061\"");
  CPPUNIT_ASSERT_EQUAL(summary4.getETag(),
                       std::string("509864c48129fbb2df505fba7a2d6061"));

  auto summary5 = aws::S3ObjectSummary("bucket", "filename", 0, "a");
  CPPUNIT_ASSERT_EQUAL(summary5.getETag(), std::string("a"));

  auto summary6 = aws::S3ObjectSummary("bucket", "filename", 0, "\"");
  CPPUNIT_ASSERT_EQUAL(summary6.getETag(), std::string("\""));

  auto summary7 = aws::S3ObjectSummary("bucket", "filename", 0, "\"\"");
  CPPUNIT_ASSERT_EQUAL(summary7.getETag(), std::string(""));

  auto summary8 = aws::S3ObjectSummary("bucket", "filename", 0, "\"a");
  CPPUNIT_ASSERT_EQUAL(summary8.getETag(), std::string("\"a"));
}
