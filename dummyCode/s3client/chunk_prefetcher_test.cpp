#include <cppunit/config/SourcePrefix.h>
#include <boost/filesystem.hpp>

#include "s3client/hosm_s3lient_test.hpp"
#include "s3client/chunk_prefetcher_test.hpp"
#include "s3client/s3_chunk_buffer.hpp"
#include "s3client/s3_chunk_prefetcher.hpp"
#include "s3client/s3_chunk_reader_exception.hpp"
#include "xen_utils/md5_helper.hpp"
#include "xen_utils/s3_chunk_buffer_factory.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION(S3ChunkPrefetcherTest);

// Read given S3 objects chunk by chunk with prefetch
// Return total s3 files size and calculate md5sum
int64_t S3ChunkPrefetcherTest::prefetch_chunks(
    size_t chunk_size, const std::vector<aws::S3ObjectSummary>& objs,
    int& first_chunk_count) {
  auto s3_client = aws::HOSMS3ClientTest::create_s3_client();
  auto prefetch_chunk_reader = std::make_unique<aws::S3ChunkPrefetcher>(
      chunk_size * 1024 * 1024, s3_client.get(), objs, 1);

  size_t block_count(0);
  int64_t total_size(0);
  Rt_timer timer;
  first_chunk_count = 0;
  md5_helper::Calc md5_calculator;
  md5_helper::IncrementalStart(&md5_calculator);
  try {
    while (true) {
      auto chunk_buf = prefetch_chunk_reader->next_chunk();
      if (!chunk_buf) {
        break;
      }
      // Mock data processing time
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      total_size += static_cast<int64_t>(chunk_buf->content_size());
      block_count++;
      if (chunk_buf->sequence_id() == 0 && chunk_buf->content_size() > 0) {
        first_chunk_count++;
      }
      md5_helper::IncrementalAdd(chunk_buf->data(), chunk_buf->content_size(),
                                 &md5_calculator);
    }
  } catch (...) {
    s3_client->shutdown();
    throw;
  }

  printf(
      "chunk_size: %zuMB block_count: %zu total_size: %ld E2E time: %lldms "
      "chunk_read_time: %ldms chunk_wait_time: %ldms\n",
      chunk_size, block_count, total_size, timer.elapsed_ms(),
      prefetch_chunk_reader->total_read_chunk_time_ms(),
      prefetch_chunk_reader->total_wait_chunk_time_ms());

  md5_helper::IncrementalComplete(&md5_calculator, &md5_);
  s3_client->shutdown();
  return total_size;
}

// Read S3 file block by block
// Test chunk size from 1MB to 32MB, block size from 1MB to 128MB
void S3ChunkPrefetcherTest::prefetch_chunk_with_variable_chunk_size() {
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

  for (size_t chunk_size = 32; chunk_size >= 1; chunk_size /= 2) {
    int first_chunk_count(0);
    int64_t total_size = prefetch_chunks(chunk_size, objs, first_chunk_count);
    CPPUNIT_ASSERT_MESSAGE("File size should match", 18736512L == total_size);
    CPPUNIT_ASSERT_MESSAGE("First chunk count should match",
                           3 == first_chunk_count);
    CPPUNIT_ASSERT_MESSAGE("File md5 should match", expected_md5 == md5_);
  }
}

// Read empty S3 file
void S3ChunkPrefetcherTest::prefetch_chunks_of_empty_s3_obj() {
  int64_t default_size = 1L * 1024 * 1024 * 1024 * 100;
  std::string default_tag = aws::S3ObjectSummary::kDefaultETag;
  std::vector<aws::S3ObjectSummary> objs(
      {{"redshift-monitoring-perf-backup",
        "cppunit_test/empty_content_gz_file.txt", default_size, default_tag}});
  int first_chunk_count(0);

  size_t obj_size = prefetch_chunks(1lu, objs, first_chunk_count);

  CPPUNIT_ASSERT_MESSAGE("File size should be 0", 0lu == obj_size);
  CPPUNIT_ASSERT_MESSAGE("First chunk count should be 0",
                         0 == first_chunk_count);
}

// Throw exception when read none existed S3 file
void S3ChunkPrefetcherTest::prefetch_chunks_of_none_exist_s3_obj_test() {
  int64_t default_size = 1L * 1024 * 1024 * 1024 * 100;
  std::string default_tag = aws::S3ObjectSummary::kDefaultETag;
  std::vector<aws::S3ObjectSummary> objs(
      {{"redshift-monitoring-perf-backup",
        "cppunit_test/none_exist_objs_kouihOuT1234", default_size,
        default_tag}});
  bool not_found(false);
  int first_chunk_count(0);
  try {
    prefetch_chunks(512u, objs, first_chunk_count);
  } catch (aws::S3ClientException& e) {
    printf("caught exception: %s\n", e.what());
    not_found =
        strstr(e.what(), "The specified key does not exist") != nullptr ||
        strstr(e.what(), "AccessDenied") != nullptr;
  }
  CPPUNIT_ASSERT_MESSAGE("Should caught object not found exception",
                         true == not_found);
}

// Suppose next_chunk return nullptr when finished scan whole s3 object
void S3ChunkPrefetcherTest::prefetch_return_nullptr_when_finished() {
  int64_t default_size = 1L * 1024 * 1024 * 1024 * 100;
  std::string default_tag = aws::S3ObjectSummary::kDefaultETag;
  std::vector<aws::S3ObjectSummary> objs(
      {{"redshift-monitoring-perf-backup",
        "cppunit_test/file_with_23957_lines.txt", default_size, default_tag}});

  auto s3_client = aws::HOSMS3ClientTest::create_s3_client();
  auto reader = std::make_unique<aws::S3ChunkPrefetcher>(
      10lu * 1024, s3_client.get(), objs, 1);
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
