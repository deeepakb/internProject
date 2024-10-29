#pragma once

#include <string>
#include <vector>
#include <utility>
#include "s3client/s3client.hpp"

#include <cppunit/extensions/HelperMacros.h>

class S3ChunkReaderTest : public CPPUNIT_NS::TestFixture {
 private:
  CPPUNIT_TEST_SUITE(S3ChunkReaderTest);
  CPPUNIT_TEST(read_chunks_with_variable_chunk_size);
  CPPUNIT_TEST(read_chunks_of_none_exist_s3_obj_test);
  CPPUNIT_TEST(read_chunks_of_empty_s3_obj);
  CPPUNIT_TEST(read_chunks_of_s3_file_size_eq_chunk_size);
  CPPUNIT_TEST(read_chunks_of_read_empty_s3_object_summary);
  CPPUNIT_TEST(read_chunks_return_nullptr_when_finished);
  CPPUNIT_TEST(read_chunks_throw_exception_when_invalid_buffer);
  CPPUNIT_TEST_SUITE_END();

 protected:
  size_t read_chunks(size_t chunk_size,
                     const std::vector<aws::S3ObjectSummary>& objs);
  void read_chunks_with_variable_chunk_size();
  void read_chunks_of_none_exist_s3_obj_test();
  void read_chunks_of_empty_s3_obj();
  void read_chunks_of_s3_file_size_eq_chunk_size();
  void read_chunks_of_read_empty_s3_object_summary();
  void read_chunks_return_nullptr_when_finished();
  void read_chunks_throw_exception_when_invalid_buffer();
  std::string md5_;
};

