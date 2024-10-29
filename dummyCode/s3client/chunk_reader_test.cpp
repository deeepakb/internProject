#include <cppunit/config/SourcePrefix.h>

#include "s3client/chunk_reader_test.hpp"
#include "s3client/hosm_s3lient_test.hpp"
#include "s3client/s3_chunk_reader.hpp"
#include "s3client/s3_chunk_buffer.hpp"
#include "s3client/s3client.hpp"
#include "xen_utils/md5_helper.hpp"
#include "xen_utils/qid.h"
#include "xen_utils/s3_chunk_buffer_factory.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION(S3ChunkReaderTest);

void S3ChunkReaderTest::read_chunks_of_read_empty_s3_object_summary() {
  bool found_exception(false);
  auto s3_client = aws::HOSMS3ClientTest::create_s3_client();
  auto chunk_buf = aws::S3ChunkBufferFactory::alloc_heap_buf(10);
  try {
    auto reader = std::make_unique<aws::S3ChunkReader>(
        s3_client.get(), aws::S3ObjectSummary{"", 0, ""}, chunk_buf.get(), 1);
    reader->next_chunk();
  } catch (std::exception& e) {
    s3_client->shutdown();
    printf("exception: %s\n", e.what());
    if (strstr(e.what(), "The specified bucket is not valid")) {
      found_exception = true;
    }
  }
  CPPUNIT_ASSERT_MESSAGE("Should caught bucket not found exception",
                         found_exception);
}

void S3ChunkReaderTest::read_chunks_throw_exception_when_invalid_buffer() {
  bool found_exception(false);
  auto s3_client = aws::HOSMS3ClientTest::create_s3_client();
  auto chunk_buf = aws::S3ChunkBufferFactory::alloc_heap_buf(0);
  int64_t default_size = 1L * 1024 * 1024 * 1024 * 100;
  std::string default_tag = aws::S3ObjectSummary::kDefaultETag;
  std::string expected_md5 = "d40bb2567c02e1b1623af79777e8cc13";
  aws::S3ObjectSummary obj{"redshift-monitoring-perf-backup",
                           "cppunit_test/file_with_23957_lines.txt",
                           default_size, default_tag};
  try {
    auto reader = std::make_unique<aws::S3ChunkReader>(
        s3_client.get(), aws::S3ObjectSummary{"", 0, ""}, chunk_buf.get(), 1);
  } catch (std::exception& e) {
    s3_client->shutdown();
    printf("exception: %s\n", e.what());
    if (strstr(e.what(),
               "S3ChunkReader buffer capacity should no less than 1")) {
      found_exception = true;
    }
  }
  CPPUNIT_ASSERT_MESSAGE("Should caught invalid buffer capacity exception",
                         found_exception);
}

// Read given S3 objects chunk by chunk
// Return total s3 files size and md5sum
size_t S3ChunkReaderTest::read_chunks(
    size_t chunk_size, const std::vector<aws::S3ObjectSummary>& objs) {
  auto handler_buf = aws::S3ChunkBufferFactory::alloc_xen_buf(chunk_size);
  auto s3_client = aws::HOSMS3ClientTest::create_s3_client();
  size_t total_size(0);
  md5_helper::Calc md5_calculator;
  md5_helper::IncrementalStart(&md5_calculator);
  try {
    for (const auto& obj : objs) {
      auto chunk_reader = std::make_unique<aws::S3ChunkReader>(
          s3_client.get(), obj, handler_buf.get(), 1);
      size_t obj_size(0);
      int first_chunk_count(0);
      while (true) {
        const auto chunk_buf = chunk_reader->next_chunk();
        if (!chunk_buf) {
          break;
        }

        obj_size += chunk_buf->content_size();
        if (chunk_buf->sequence_id() == 0 && chunk_buf->content_size() > 0) {
          ++first_chunk_count;
        }
        md5_helper::IncrementalAdd(chunk_buf->data(), chunk_buf->content_size(),
                                   &md5_calculator);
      }
      total_size += obj_size;
      if (obj_size > 0) {
        CPPUNIT_ASSERT_MESSAGE("Each obj should have exactly one first block",
                               1 == first_chunk_count);
      }
    }
  } catch (std::exception& e) {
    s3_client->shutdown();
    throw;
  }
  s3_client->shutdown();
  md5_helper::IncrementalComplete(&md5_calculator, &md5_);
  return total_size;
}

// Read S3 file chunk by chunk
// Test chunk size from 1MB to 1GB
void S3ChunkReaderTest::read_chunks_with_variable_chunk_size() {
  int64_t default_size = 1L * 1024 * 1024 * 1024 * 100;
  std::string default_tag = aws::S3ObjectSummary::kDefaultETag;
  std::string expected_md5 = "19e2cd869fa735187ee558163ecd8b05";
  std::vector<aws::S3ObjectSummary> objs(
      {{"redshift-monitoring-perf-backup",
        "cppunit_test/CN0_stl_print_0_ComputeBootstrap.647573830078845.gz",
        default_size, default_tag},
       {"redshift-monitoring-perf-backup",
        "cppunit_test/CN1_stl_print_0_ComputeBootstrap.647573829687835.gz",
        default_size, default_tag},
       {"redshift-monitoring-perf-backup",
        "cppunit_test/CN2_stl_print_0_ComputeBootstrap.647573829572812.gz",
        default_size, default_tag}});

  for (size_t chunk_size = 1024; chunk_size >= 1; chunk_size /= 2) {
    Rt_timer timer;
    size_t obj_size = read_chunks(chunk_size * 1024 * 1024, objs);
    int64_t time = timer.elapsed_ms();
    printf("chunk size:%zuMB  obj_size:%zu time:%ld\n", chunk_size, obj_size,
           time);
    CPPUNIT_ASSERT_MESSAGE("File size should match", 18736512lu == obj_size);
    CPPUNIT_ASSERT_MESSAGE("File md5 should match ", expected_md5 == md5_);
  }
}

// Read not existed S3 file
void S3ChunkReaderTest::read_chunks_of_none_exist_s3_obj_test() {
  int64_t default_size = 1L * 1024 * 1024 * 1024 * 100;
  std::string default_tag = aws::S3ObjectSummary::kDefaultETag;
  std::vector<aws::S3ObjectSummary> objs(
      {{"redshift-monitoring-perf-backup",
        "cppunit_test/none_exist_objs_kouihOuT1234", default_size,
        default_tag}});
  bool not_found(false);
  try {
    size_t obj_size = read_chunks(1024lu, objs);
    CPPUNIT_ASSERT_EQUAL(0lu, obj_size);
  } catch (std::exception& e) {
    not_found =
        strstr(e.what(), "The specified key does not exist") != nullptr ||
        strstr(e.what(), "AccessDenied") != nullptr;
  }
  CPPUNIT_ASSERT_MESSAGE("Should not found object", true == not_found);
}

// Read empty S3 file
void S3ChunkReaderTest::read_chunks_of_empty_s3_obj() {
  int64_t default_size = 1L * 1024 * 1024 * 1024 * 100;
  std::string default_tag = aws::S3ObjectSummary::kDefaultETag;
  std::vector<aws::S3ObjectSummary> objs(
      {{"redshift-monitoring-perf-backup",
        "cppunit_test/empty_content_gz_file.txt", default_size, default_tag}});
  size_t obj_size = read_chunks(1024lu, objs);
  CPPUNIT_ASSERT_MESSAGE("Object should be empty", 0lu == obj_size);
}

// Read S3 file with chunk_size as file_size
// S3ChunkReader will try to call next_chunk() two times, the second call
// S3client will throw exception with error code 416. This test assume the
// S3ChunkReader should handled the exception
void S3ChunkReaderTest::read_chunks_of_s3_file_size_eq_chunk_size() {
  int64_t default_size = 1L * 1024 * 1024 * 1024 * 100;
  std::string default_tag = aws::S3ObjectSummary::kDefaultETag;
  std::string expected_md5 = "d40bb2567c02e1b1623af79777e8cc13";
  std::vector<aws::S3ObjectSummary> objs(
      {{"redshift-monitoring-perf-backup",
        "cppunit_test/file_with_23957_lines.txt", default_size, default_tag}});
  size_t obj_size = read_chunks(2 * 1024 * 1024lu, objs);
  CPPUNIT_ASSERT_MESSAGE("File size should match",
                         2 * 1024 * 1024lu == obj_size);
  CPPUNIT_ASSERT_MESSAGE("File md5 should match", expected_md5 == md5_);
}

// Suppose next_chunk return nullptr when finished scan whole s3 object
void S3ChunkReaderTest::read_chunks_return_nullptr_when_finished() {
  int64_t default_size = 1L * 1024 * 1024 * 1024 * 100;
  std::string default_tag = aws::S3ObjectSummary::kDefaultETag;
  aws::S3ObjectSummary obj{"redshift-monitoring-perf-backup",
                           "cppunit_test/file_with_23957_lines.txt",
                           default_size, default_tag};

  auto chunk_buf = aws::S3ChunkBufferFactory::alloc_heap_buf(10 * 1024);
  auto s3_client = aws::HOSMS3ClientTest::create_s3_client();
  auto reader = std::make_unique<aws::S3ChunkReader>(s3_client.get(), obj,
                                                     chunk_buf.get(), 1);
  int chunk_count(0);
  // Drain up chunks
  while (reader->next_chunk()) {
    ++chunk_count;
  }
  CPPUNIT_ASSERT_MESSAGE("Chunk count should match", 205 == chunk_count);

  int not_null(0);
  for (int i = 0; i < 10; ++i) {
    const auto chunk = reader->next_chunk();
    if (chunk) {
      not_null++;
    }
  }
  s3_client->shutdown();
  CPPUNIT_ASSERT_MESSAGE("Chunk should be null", 0 == not_null);
}
