// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/*
 * TestBackendBasics - Unified parameterized test harness for EC and Replicated
 * backend operations.
 *
 * Two fixture classes are defined, each parameterized over the full set of
 * backend configurations:
 *
 * TestBackendBasics
 *   Parameterized over BackendWriteReadParam (BackendConfig × WriteReadParam).
 *   13 backends × 8 data sizes = 104 instances per test body.
 *
 *   WriteThenRead  – write data, verify protocol messages, read back, verify
 *                    data integrity.
 *   PartialWrite   – create an object, perform a partial write at a non-zero
 *                    offset, read back and verify all three regions.
 *
 * TestECFailover
 *   Parameterized over BackendConfig (EC configs only, 12 instances).
 *   Failover is an EC-specific concept (shard-based primary election).
 *
 *   BasicOSDMapUpdate – write, update OSDMap epoch, verify read still works.
 *   PrimaryFailover   – write, fail OSD 0, verify new primary and degraded
 *                       read with EC reconstruction.
 */

#include <gtest/gtest.h>
#include "test/osd/PGBackendTestFixture.h"
#include "test/osd/TestCommon.h"
#include "messages/MOSDECSubOpWrite.h"

using namespace std;

// ---------------------------------------------------------------------------
// TestBackendBasics fixture
// ---------------------------------------------------------------------------

/**
 * TestBackendBasics - single fixture parameterized over BackendWriteReadParam.
 *
 * The constructor reads the BackendConfig portion of the parameter and
 * configures the base fixture fields (pool_type, k, m, stripe_unit, ec_plugin,
 * ec_technique, ec_optimizations, num_replicas, min_size) before SetUp() is
 * called by GTest.
 */
class TestBackendBasics : public PGBackendTestFixture,
                          public ::testing::WithParamInterface<BackendWriteReadParam> {
public:
  TestBackendBasics() : PGBackendTestFixture() {
    const auto& config = GetParam().backend;
    pool_type = config.pool_type;
    if (pool_type == EC) {
      k = config.k;
      m = config.m;
      stripe_unit = config.stripe_unit;
      ec_plugin = config.ec_plugin;
      ec_technique = config.ec_technique;
      ec_optimizations = config.ec_optimizations;
    } else {
      num_replicas = 3;
      min_size = 2;
    }
  }

  void SetUp() override {
    PGBackendTestFixture::SetUp();
  }
};

// ---------------------------------------------------------------------------
// TestBackendBasics: WriteThenRead
// ---------------------------------------------------------------------------

/**
 * WriteThenRead - write data of the parameterized size, verify protocol
 * messages were sent, read back, and verify data integrity.
 *
 * For EC backends: asserts that MSG_OSD_EC_WRITE messages were sent and that
 * read messages are sent to shards.
 * For Replicated backends: asserts that at least one message was sent.
 */
TEST_P(TestBackendBasics, WriteThenRead) {
  const auto& param = GetParam().write_read;
  const auto& backend_config = GetParam().backend;

  std::string test_data(param.size, param.fill);
  std::string obj_name = "test_backend_" + backend_config.label + "_" + param.label;

  // Execute create+write operation
  int result = create_and_write(obj_name, test_data);
  EXPECT_EQ(result, 0) << param.label << " write should complete successfully";

  // Verify messages were sent to replicas/shards
  ASSERT_GT(listener->sent_messages.size(), 0u)
    << "Should send messages to replicas/shards";

  // For EC backends: verify EC write messages were sent
  if (backend_config.pool_type == EC) {
    int write_messages_sent = 0;
    for (auto msg : listener->sent_messages) {
      if (msg->get_type() == MSG_OSD_EC_WRITE) {
        write_messages_sent++;
      }
    }
    ASSERT_GT(write_messages_sent, 0) << "Should send EC write messages";
  }

  // Clear sent messages before read to distinguish read messages
  listener->sent_messages.clear();
  listener->sent_messages_with_dest.clear();

  // Perform the read operation
  bufferlist read_data;
  int read_result = read_object(
    obj_name,
    0,                  // offset
    test_data.length(), // length
    read_data,
    test_data.length()  // object_size
  );

  EXPECT_GE(read_result, 0) << param.label << " read should complete successfully";

  // Verify data length
  ASSERT_EQ(read_data.length(), test_data.length())
    << param.label << " read data length should match written data length";

  // Verify data content
  std::string read_string(read_data.c_str(), read_data.length());
  EXPECT_EQ(read_string, test_data)
    << param.label << " read data should match written data";

  // For EC backends: verify read messages were sent to shards
  if (backend_config.pool_type == EC) {
    ASSERT_GT(listener->sent_messages.size(), 0u)
      << "Should send read messages to EC shards";
  }

  // All events should be processed by now
  ASSERT_FALSE(event_loop->has_events()) << "Event loop should be idle after read";

  listener->sent_messages.clear();
}

// ---------------------------------------------------------------------------
// TestBackendBasics: PartialWrite
// ---------------------------------------------------------------------------

/**
 * PartialWrite - create an object of the parameterized size (rounded up to a
 * multiple of the stripe width for EC, or used directly for replicated), write
 * a partial region at a non-zero offset, read back and verify that:
 *   - the region before the partial write is unchanged,
 *   - the partial-write region contains the new data,
 *   - the region after the partial write is unchanged.
 */
TEST_P(TestBackendBasics, PartialWrite) {
  const auto& param = GetParam().write_read;
  const auto& backend_config = GetParam().backend;

  std::string obj_name = "test_partial_" + backend_config.label + "_" + param.label;

  // Use the parameterized size as the initial object size, but ensure it is
  // large enough to accommodate a non-trivial partial write.  We need at least
  // 3 regions: prefix, modified, suffix.  Use max(param.size, 3 * 4096) so
  // that even the smallest size parameters produce a meaningful test.
  const size_t initial_size = std::max(param.size, size_t(3 * 4096));

  // Partial write covers the middle third of the object (aligned to 4 KB).
  const size_t region = (initial_size / 3) & ~size_t(4095);  // round down to 4 KB
  const size_t partial_offset = region ? region : 4096;
  const size_t partial_size   = region ? region : 4096;

  // Create initial data filled with the parameterized fill character
  std::string initial_data(initial_size, param.fill);

  int result = create_and_write(obj_name, initial_data, eversion_t(1, 1));
  EXPECT_EQ(result, 0) << param.label << " initial write should complete successfully";

  // Partial write data uses the next fill character (wraps around 'z' -> 'a')
  char partial_fill = (param.fill == 'z') ? 'a' : (param.fill + 1);
  std::string partial_data(partial_size, partial_fill);

  result = write(
    obj_name,
    partial_offset,
    partial_data,
    eversion_t(1, 1),  // prior_version
    eversion_t(1, 2),  // at_version
    initial_size       // object_size
  );
  EXPECT_EQ(result, 0) << param.label << " partial write should complete successfully";

  // Read back the entire object
  bufferlist read_data;
  int read_result = read_object(obj_name, 0, initial_size, read_data, initial_size);
  EXPECT_GE(read_result, 0)
    << param.label << " read after partial write should complete successfully";

  ASSERT_EQ(read_data.length(), initial_size)
    << param.label << " read data length should match object size";

  const char* buf = read_data.c_str();

  // Region before the partial write should be unchanged
  for (size_t i = 0; i < partial_offset; i++) {
    ASSERT_EQ(buf[i], param.fill)
      << param.label << " data before partial write offset should be unchanged at position " << i;
  }

  // Partial-write region should contain the new fill character
  for (size_t i = partial_offset; i < partial_offset + partial_size; i++) {
    ASSERT_EQ(buf[i], partial_fill)
      << param.label << " data at partial write region should be '" << partial_fill
      << "' at position " << i;
  }

  // Region after the partial write should be unchanged
  for (size_t i = partial_offset + partial_size; i < initial_size; i++) {
    ASSERT_EQ(buf[i], param.fill)
      << param.label << " data after partial write region should be unchanged at position " << i;
  }
}

// ---------------------------------------------------------------------------
// Backend configurations and size parameters
// ---------------------------------------------------------------------------

namespace {

const std::vector<BackendConfig> kBackendConfigs = {
  {PGBackendTestFixture::REPLICATED, "", "", false, 4096, 4, 2, "Replicated"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", true,  4096,  4, 2, "EC_ISA_Opt_k4m2_su4k"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", true,  8192,  4, 2, "EC_ISA_Opt_k4m2_su8k"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", true,  16384, 4, 2, "EC_ISA_Opt_k4m2_su16k"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", true,  4096,  2, 1, "EC_ISA_Opt_k2m1_su4k"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", true,  4096,  8, 3, "EC_ISA_Opt_k8m3_su4k"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", false, 4096,  4, 2, "EC_ISA_NonOpt_k4m2_su4k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", true,  4096,  4, 2, "EC_Jerasure_Opt_k4m2_su4k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", true,  8192,  4, 2, "EC_Jerasure_Opt_k4m2_su8k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", true,  16384, 4, 2, "EC_Jerasure_Opt_k4m2_su16k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", true,  4096,  2, 1, "EC_Jerasure_Opt_k2m1_su4k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", true,  4096,  8, 3, "EC_Jerasure_Opt_k8m3_su4k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", false, 4096,  4, 2, "EC_Jerasure_NonOpt_k4m2_su4k"},
};

const std::vector<WriteReadParam> kSizeParams = {
  {4  * 1024,       'A', "4k"},
  {8  * 1024,       'B', "8k"},
  {12 * 1024,       'C', "12k"},
  {12 * 1024 + 512, 'D', "12_5k"},
  {16 * 1024,       'E', "16k"},
  {31 * 1024 + 512, 'F', "31_5k"},
  {32 * 1024,       'G', "32k"},
  {32 * 1024 + 512, 'H', "32_5k"},
};

/**
 * Build the cross-product of kBackendConfigs × kSizeParams.
 */
std::vector<BackendWriteReadParam> make_cross_product() {
  std::vector<BackendWriteReadParam> result;
  result.reserve(kBackendConfigs.size() * kSizeParams.size());
  for (const auto& backend : kBackendConfigs) {
    for (const auto& size : kSizeParams) {
      result.push_back({backend, size});
    }
  }
  return result;
}

}  // namespace

// ---------------------------------------------------------------------------
// Instantiate TestBackendBasics with the full cross-product
// ---------------------------------------------------------------------------

INSTANTIATE_TEST_SUITE_P(
  BackendSizes,
  TestBackendBasics,
  ::testing::ValuesIn(make_cross_product()),
  [](const ::testing::TestParamInfo<BackendWriteReadParam>& info) {
    return info.param.backend.label + "_" + info.param.write_read.label;
  }
);

// ---------------------------------------------------------------------------
// TestECFailover fixture and tests
// ---------------------------------------------------------------------------

/**
 * TestECFailover - tests OSDMap updates and primary failover, parameterized
 * over all EC backend configurations.
 *
 * Failover is an EC-specific concept (shard-based primary election), so only
 * EC configs are included.  The fixture reads k/m/stripe_unit/plugin/technique
 * from the BackendConfig parameter so that every EC variant is exercised.
 */
class TestECFailover : public PGBackendTestFixture,
                       public ::testing::WithParamInterface<BackendConfig> {
public:
  TestECFailover() : PGBackendTestFixture(PGBackendTestFixture::EC) {
    const auto& config = GetParam();
    k = config.k;
    m = config.m;
    stripe_unit = config.stripe_unit;
    ec_plugin = config.ec_plugin;
    ec_technique = config.ec_technique;
    ec_optimizations = config.ec_optimizations;
  }

  void SetUp() override {
    PGBackendTestFixture::SetUp();
  }

  void simulate_osd_failure(int failed_osd, int new_primary_instance)
  {
    auto new_osdmap = std::make_shared<OSDMap>();
    new_osdmap->deepish_copy_from(*osdmap);
    new_osdmap->inc_epoch();

    new_osdmap->set_state(failed_osd, CEPH_OSD_EXISTS);  // Mark as down (exists but not UP)

    pg_shard_t failed_shard(failed_osd, shard_id_t(failed_osd));
    for (auto& [instance_id, list] : listeners) {
      list->shardset.erase(failed_shard);
      list->acting_recovery_backfill_shard_id_set.erase(shard_id_t(failed_osd));
    }

    pg_shard_t new_primary(new_primary_instance, shard_id_t(new_primary_instance));
    update_osdmap(new_osdmap, new_primary);
  }
};

TEST_P(TestECFailover, BasicOSDMapUpdate) {
  const std::string obj_name = "test_failover_object";
  const std::string test_data = "Initial data before OSDMap change";

  int result = create_and_write(obj_name, test_data);
  EXPECT_EQ(result, 0) << "Initial write should complete successfully";

  bufferlist read_data;
  int read_result = read_object(obj_name, 0, test_data.length(), read_data, test_data.length());
  EXPECT_GE(read_result, 0) << "Read should complete successfully";
  ASSERT_EQ(read_data.length(), test_data.length());

  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  new_osdmap->inc_epoch();

  update_osdmap(new_osdmap);

  EXPECT_EQ(osdmap, new_osdmap) << "OSDMap should be updated";
  EXPECT_EQ(listener->osdmap, new_osdmap) << "Listener OSDMap should be updated";

  bufferlist read_data2;
  read_result = read_object(obj_name, 0, test_data.length(), read_data2, test_data.length());
  EXPECT_GE(read_result, 0) << "Read after OSDMap update should complete successfully";
  ASSERT_EQ(read_data2.length(), test_data.length());

  std::string read_string(read_data2.c_str(), read_data2.length());
  EXPECT_EQ(read_string, test_data) << "Data should match after OSDMap update";
}

TEST_P(TestECFailover, PrimaryFailover) {
  const std::string obj_name = "test_primary_failover";
  const std::string test_data = "Data written before primary failover";

  int result = create_and_write(obj_name, test_data);
  EXPECT_EQ(result, 0) << "Initial write should complete successfully";

  bufferlist read_data;
  int read_result = read_object(obj_name, 0, test_data.length(), read_data, test_data.length());
  EXPECT_GE(read_result, 0) << "Read should complete successfully";
  ASSERT_EQ(read_data.length(), test_data.length());

  std::string read_string(read_data.c_str(), read_data.length());
  EXPECT_EQ(read_string, test_data) << "Data should match before failover";

  EXPECT_TRUE(listeners[0]->pgb_is_primary())
    << "Instance 0 should be primary before failover";
  EXPECT_FALSE(listeners[k]->pgb_is_primary())
    << "Instance " << k << " should not be primary before failover";

  // In EC, peering will not allow primary to be instances 1 to k-1 when instance 0 fails
  simulate_osd_failure(0, k);

  EXPECT_FALSE(listeners[0]->pgb_is_primary())
    << "Instance 0 should not be primary after failover";
  EXPECT_TRUE(listeners[k]->pgb_is_primary())
    << "Instance " << k << " should be primary after failover";

  EXPECT_EQ(listener, listeners[k].get())
    << "Listener convenience pointer should point to new primary";
  EXPECT_EQ(backend, backends[k].get())
    << "Backend convenience pointer should point to new primary";

  bufferlist read_data_after;
  int read_result_after = read_object(obj_name, 0, test_data.length(), read_data_after, test_data.length());
  EXPECT_GE(read_result_after, 0) << "Degraded read should complete successfully after failover";
  ASSERT_EQ(read_data_after.length(), test_data.length());

  std::string read_string_after(read_data_after.c_str(), read_data_after.length());
  EXPECT_EQ(read_string_after, test_data) << "Data should match after failover with EC reconstruction";

  EXPECT_GT(listener->osdmap->get_epoch(), 1)
    << "OSDMap epoch should have incremented after failover";
}

// ---------------------------------------------------------------------------
// Instantiate TestECFailover with EC-only backend configurations
// ---------------------------------------------------------------------------

namespace {

std::vector<BackendConfig> make_ec_configs() {
  std::vector<BackendConfig> ec_configs;
  for (const auto& cfg : kBackendConfigs) {
    if (cfg.pool_type == PGBackendTestFixture::EC) {
      ec_configs.push_back(cfg);
    }
  }
  return ec_configs;
}

}  // namespace

INSTANTIATE_TEST_SUITE_P(
  ECBackends,
  TestECFailover,
  ::testing::ValuesIn(make_ec_configs()),
  [](const ::testing::TestParamInfo<BackendConfig>& info) {
    return info.param.label;
  }
);
