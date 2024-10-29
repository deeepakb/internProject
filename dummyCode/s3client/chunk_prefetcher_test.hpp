#pragma once

#include <string>
#include <vector>
#include <utility>

#include <cppunit/extensions/HelperMacros.h>

class S3ChunkPrefetcherTest : public CPPUNIT_NS::TestFixture {
 private:
  CPPUNIT_TEST_SUITE(S3ChunkPrefetcherTest);
  CPPUNIT_TEST(prefetch_chunk_with_variable_chunk_size);
  CPPUNIT_TEST(prefetch_chunks_of_empty_s3_obj);
  CPPUNIT_TEST(prefetch_chunks_of_none_exist_s3_obj_test);
  CPPUNIT_TEST(prefetch_return_nullptr_when_finished);
  CPPUNIT_TEST_SUITE_END();

 protected:
  int64_t prefetch_chunks(size_t chunk_size,
                          const std::vector<aws::S3ObjectSummary>& objs,
                          int& first_chunk_count);
  void prefetch_chunk_with_variable_chunk_size();
  void prefetch_chunks_of_empty_s3_obj();
  void prefetch_chunks_of_none_exist_s3_obj_test();
  void prefetch_return_nullptr_when_finished();
  std::string md5_;
};

