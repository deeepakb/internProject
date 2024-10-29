#pragma once

#include <memory>
#include <string>
#include <vector>
#include <utility>

#include <cppunit/extensions/HelperMacros.h>
#include "s3client/s3client.hpp"
#include "s3client/s3_buffered_line_reader.hpp"

class S3BufferedLineReaderTest : public CPPUNIT_NS::TestFixture {
 private:
  CPPUNIT_TEST_SUITE(S3BufferedLineReaderTest);
  CPPUNIT_TEST(read_lines_of_single_gz_obj_test);
  CPPUNIT_TEST(read_lines_of_mutlti_gz_obj_test);
  CPPUNIT_TEST(read_none_exist_s3_obj_test);
  CPPUNIT_TEST(read_empty_s3_obj_list_test);
  CPPUNIT_TEST(read_s3_obj_with_empty_conent_test);
  CPPUNIT_TEST(read_lines_of_single_text_file_test);
  CPPUNIT_TEST(read_lines_of_multi_text_s3_obj_test);
  CPPUNIT_TEST(read_lines_close_before_finished_read_all_lines);
  CPPUNIT_TEST_SUITE_END();

 protected:
  void read_lines_of_single_gz_obj_test();
  void read_lines_of_mutlti_gz_obj_test();
  void read_lines_of_single_text_file_test();
  void read_lines_of_multi_text_s3_obj_test();
  void read_lines_close_before_finished_read_all_lines();
  void read_none_exist_s3_obj_test();
  void read_empty_s3_obj_list_test();
  void read_s3_obj_with_empty_conent_test();

  void create_line_reader(aws::S3Client* s3_client,
                          const std::vector<aws::S3ObjectSummary>& objs,
                          size_t s3_chunk_size, int compress_rate,
                          bool gzipped);

  size_t read_lines_with_chunks(const std::vector<aws::S3ObjectSummary>& objs,
                                size_t s3_chunk_size, int compress_rate,
                                bool gzipped = true);
  std::unique_ptr<aws::S3BufferedLineReader> s3_buffered_line_reader_;
  std::string md5_;
};

