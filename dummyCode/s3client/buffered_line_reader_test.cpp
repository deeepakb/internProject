#include <cppunit/config/SourcePrefix.h>
#include <boost/filesystem.hpp>

#include "s3client/buffered_line_reader_test.hpp"
#include "s3client/hosm_s3lient_test.hpp"
#include "s3client/s3_buffered_line_reader.hpp"
#include "s3client/s3_chunk_buffer.hpp"
#include "xen_utils/md5_helper.hpp"
#include "xen_utils/s3_chunk_buffer_factory.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION(S3BufferedLineReaderTest);

// Create S3BufferedLineReader for with specified block size
void S3BufferedLineReaderTest::create_line_reader(
    aws::S3Client* s3_client, const std::vector<aws::S3ObjectSummary>& objs,
    size_t s3_chunk_size, int estimated_compress_rate, bool gzipped) {
  size_t max_line_size = 50 * 1024;
  fprintf(stdout, "compress rate %d\n", estimated_compress_rate);
  s3_buffered_line_reader_ = std::make_unique<aws::S3BufferedLineReader>(
      gzipped, estimated_compress_rate, s3_chunk_size, max_line_size, s3_client,
      objs, 1);
}

// Read given S3 objects line by line
// Return total s3 line count and md5sum
size_t S3BufferedLineReaderTest::read_lines_with_chunks(
    const std::vector<aws::S3ObjectSummary>& objs, size_t s3_chunk_size,
    int estimated_compress_rate, bool gzipped) {
  auto s3_client = aws::HOSMS3ClientTest::create_s3_client();
  create_line_reader(s3_client.get(), objs, s3_chunk_size,
                     estimated_compress_rate, gzipped);
  auto line_buf = aws::S3ChunkBufferFactory::alloc_xen_buf(50 * 1024);
  md5_helper::Calc md5_calculator;
  md5_helper::IncrementalStart(&md5_calculator);
  size_t len(0), line_count(0);
  int64_t total_size(0);
  Rt_timer timer;
  try {
    while (true) {
      len = s3_buffered_line_reader_->next_line(line_buf.get());
      if (len == 0) {
        break;
      }

      line_count++;
      total_size += static_cast<int64_t>(len);
      md5_helper::IncrementalAdd(line_buf->data(), line_buf->content_size(),
                                 &md5_calculator);
    }
  } catch (std::exception& e) {
    printf("got exception: %s\n", e.what());
  }

  printf(
      "total_size: %ld line_count: %zu E2E time: %lldms "
      "block_read_time: %ldms block_count: %d\n",
      total_size, line_count, timer.elapsed_ms(),
      s3_buffered_line_reader_->total_read_block_time_ms(),
      s3_buffered_line_reader_->total_block_count());

  md5_helper::IncrementalComplete(&md5_calculator, &md5_);
  s3_client->shutdown();
  s3_buffered_line_reader_->close();
  s3_buffered_line_reader_.reset();
  return line_count;
}

// Test none exist s3 obj
void S3BufferedLineReaderTest::read_none_exist_s3_obj_test() {
  int64_t default_size = 1L * 1024 * 1024 * 1024 * 100;
  std::string default_tag = aws::S3ObjectSummary::kDefaultETag;
  std::vector<aws::S3ObjectSummary> objs(
      {{"redshift-monitoring-perf-backup",
        "cppunit_test/none_exist_objs_kouihOuT1234", default_size,
        default_tag}});
  size_t lines = read_lines_with_chunks(
      objs, 1024 * 1024 /* chunk size bytes */, 20 /* compress rate */);
  CPPUNIT_ASSERT_EQUAL(0lu, lines);
}

// Test empty s3 objs list
void S3BufferedLineReaderTest::read_empty_s3_obj_list_test() {
  std::vector<aws::S3ObjectSummary> objs;
  bool empty(false);
  try {
    read_lines_with_chunks(objs, 1024 * 1024, 1 /* compres rate */);
  } catch (std::exception& e) {
    empty = strstr(e.what(), "S3 objs should not be empty") != nullptr;
  }
  CPPUNIT_ASSERT_EQUAL(true, empty);
}

// Test empty s3 objs
void S3BufferedLineReaderTest::read_s3_obj_with_empty_conent_test() {
  int64_t default_size = 1L * 1024 * 1024 * 1024 * 100;
  std::string default_tag = aws::S3ObjectSummary::kDefaultETag;
  std::vector<aws::S3ObjectSummary> objs(
      {{"redshift-monitoring-perf-backup",
        "cppunit_test/empty_content_gz_file.gz", default_size, default_tag}});
  for (int compress_rate = 10; compress_rate < 100; compress_rate += 10) {
    size_t lines =
        read_lines_with_chunks(objs, 1024 * 1024 /* chunk size bytes*/,
                               compress_rate, true /* gzipped */);
    CPPUNIT_ASSERT_EQUAL(0lu, lines);
  }
}

// Read multi S3 text file line by line
// Test chunk size from 1MB to 64MB, block size from 1MB to 64MB
void S3BufferedLineReaderTest::read_lines_of_multi_text_s3_obj_test() {
  int64_t default_size = 1L * 1024 * 1024 * 1024 * 100;
  std::string default_tag = aws::S3ObjectSummary::kDefaultETag;
  std::string expected_md5 = "494d97de7314f4c730933cb20a8ec0a4";
  std::vector<aws::S3ObjectSummary> objs({
      {"redshift-monitoring-perf-backup",
       "cppunit_test/file_with_669511_lines.txt", default_size, default_tag},
      {"redshift-monitoring-perf-backup",
       "cppunit_test/file_with_697694_lines.txt", default_size, default_tag},
      {"redshift-monitoring-perf-backup",
       "cppunit_test/empty_content_gz_file.txt", default_size, default_tag},
      {"redshift-monitoring-perf-backup",
       "cppunit_test/file_with_725924_lines.txt", default_size, default_tag},
  });
  for (size_t chunk_size = 64; chunk_size >= 1; chunk_size /= 2) {
    int compress_rate = 1;
    printf("read s3 lines with s3_chunk_size:%zuMB\n", chunk_size);
    size_t lines = read_lines_with_chunks(
        objs, chunk_size * 1024 * 1024 /*chunk size bytes*/, compress_rate,
        false /* gzipped */);
    CPPUNIT_ASSERT_EQUAL(2093129lu, lines);
    CPPUNIT_ASSERT_EQUAL(expected_md5, md5_);
  }
}

// Read single S3 text file line by line
// Test chunk size from 1MB to 8MB, block size from 1MB to 8MB
void S3BufferedLineReaderTest::read_lines_of_single_text_file_test() {
  int64_t default_size = 1L * 1024 * 1024 * 1024 * 100;
  std::string default_tag = aws::S3ObjectSummary::kDefaultETag;
  std::string expected_md5("d40bb2567c02e1b1623af79777e8cc13");
  std::vector<aws::S3ObjectSummary> objs(
      {{"redshift-monitoring-perf-backup",
        "cppunit_test/file_with_23957_lines.txt", default_size, default_tag}});

  for (size_t chunk_size = 64; chunk_size >= 1; chunk_size /= 2) {
    printf("read s3 lines with s3_chunk_size:%zuMB\n", chunk_size);
    size_t lines = read_lines_with_chunks(
        objs, chunk_size * 1024 * 1024 /* chunk size bytes */,
        1 /*compress_rate */, false /* gzipped */);
    CPPUNIT_ASSERT_EQUAL(23957lu, lines);
    CPPUNIT_ASSERT_EQUAL(expected_md5, md5_);
  }
}

// Read single S3 gzipped file line by line
// Test chunk size from 1MB to 8MB, block size from 1MB to 8MB
void S3BufferedLineReaderTest::read_lines_of_single_gz_obj_test() {
  int64_t default_size = 1L * 1024 * 1024 * 1024 * 100;
  std::string default_tag = aws::S3ObjectSummary::kDefaultETag;
  std::string expected_md5 = "1628955cad3f6789563fe78124fdbdc2";
  std::vector<aws::S3ObjectSummary> objs(
      {{"redshift-monitoring-perf-backup",
        "cppunit_test/CN0_stl_print_0_ComputeBootstrap.647573830078845.gz",
        default_size, default_tag}});

  for (size_t chunk_size = 8; chunk_size >= 1; chunk_size /= 2) {
    for (int compress_rate = 1; compress_rate < 100; compress_rate += 2) {
      printf("read s3 lines with s3_chunk_size:%zuMB\n", chunk_size);
      size_t lines = read_lines_with_chunks(
          objs, chunk_size * 1024 * 1024 /* chunk_size */, compress_rate,
          true /* gzipped */);
      CPPUNIT_ASSERT_EQUAL(669511lu, lines);
      CPPUNIT_ASSERT_EQUAL(expected_md5, md5_);
    }
  }
}

// Read multi S3 gzipped files line by line
// Test chunk size from 1MB to 8MB, block size from 1MB to 8MB
void S3BufferedLineReaderTest::read_lines_of_mutlti_gz_obj_test() {
  int64_t default_size = 1L * 1024 * 1024 * 1024 * 100;
  std::string default_tag = aws::S3ObjectSummary::kDefaultETag;
  std::string expected_md5 = "494d97de7314f4c730933cb20a8ec0a4";
  std::vector<aws::S3ObjectSummary> objs(
      {{"redshift-monitoring-perf-backup",
        "cppunit_test/CN0_stl_print_0_ComputeBootstrap.647573830078845.gz",
        default_size, default_tag},
       {"redshift-monitoring-perf-backup",
        "cppunit_test/CN1_stl_print_0_ComputeBootstrap.647573829687835.gz",
        default_size, default_tag},
       {"redshift-monitoring-perf-backup",
        "cppunit_test/empty_content_gz_file.gz", default_size, default_tag},
       {"redshift-monitoring-perf-backup",
        "cppunit_test/CN2_stl_print_0_ComputeBootstrap.647573829572812.gz",
        default_size, default_tag}});
  for (size_t chunk_size = 8; chunk_size >= 1; chunk_size /= 2) {
    for (int compress_rate = 10; compress_rate <= 100; compress_rate += 10) {
      printf("read s3 lines with s3_chunk_size:%zuMB\n", chunk_size);
      size_t lines = read_lines_with_chunks(
          objs, chunk_size * 1024 * 1024 /*chunk size bytes*/, compress_rate,
          true /* gzipped */);
      CPPUNIT_ASSERT_EQUAL(2093129lu, lines);
      CPPUNIT_ASSERT_EQUAL(expected_md5, md5_);
    }
  }
}

// Test reader are closed as expected even we terminate reader before finished
// reading all lines
void S3BufferedLineReaderTest::
    read_lines_close_before_finished_read_all_lines() {
  int64_t default_size = 1L * 1024 * 1024 * 1024 * 100;
  std::string default_tag = aws::S3ObjectSummary::kDefaultETag;
  std::string expected_md5 = "494d97de7314f4c730933cb20a8ec0a4";
  std::vector<aws::S3ObjectSummary> objs(
      {{"redshift-monitoring-perf-backup",
        "cppunit_test/CN0_stl_print_0_ComputeBootstrap.647573830078845.gz",
        default_size, default_tag}});
  for (int compress_rate = 10; compress_rate <= 100; compress_rate += 10) {
    auto s3_client = aws::HOSMS3ClientTest::create_s3_client();
    create_line_reader(s3_client.get(), objs, 1lu * 1024 * 1024 /*chunk_size*/,
                       compress_rate /* est compress rate */, true /*gzipped*/);
    auto line_buf = aws::S3ChunkBufferFactory::alloc_xen_buf(50 * 1024);
    int lines = 1000;
    // Total lines should be 669511, terminate before finished scan all line to
    // verify reader closed as expected
    while (--lines > 0) {
      s3_buffered_line_reader_->next_line(line_buf.get());
    }
    // Clean reader status
    s3_buffered_line_reader_->close();
    s3_buffered_line_reader_.reset();
    s3_client->shutdown();
    CPPUNIT_ASSERT_EQUAL(0, lines);
  }
}
