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

#pragma once

#include <memory>
#include <map>
#include <vector>
#include "test/osd/PGBackendTestFixture.h"
#include "test/osd/MockPeeringListener.h"
#include "test/osd/MockConnection.h"
#include "test/osd/MockECRecPred.h"
#include "test/osd/MockECReadPred.h"
#include "osd/PeeringState.h"
#include "messages/MOSDPeeringOp.h"

/**
 * ECPeeringTestFixture - EC test fixture with full peering infrastructure
 *
 * This fixture extends PGBackendTestFixture to add full PeeringState support
 * for each shard, enabling comprehensive testing of EC peering, recovery,
 * and failover scenarios. It combines the principles from TestPeeringState
 * with the EC backend infrastructure from PGBackendTestFixture.
 *
 * NOTE: This mock framework only populates PG log entries on the primary
 * (shard 0) via append_log.  Replica shards do not receive log entries or
 * info.last_update propagation.  Tests that check replica state should
 * account for this limitation.
 */
class ECPeeringTestFixture : public PGBackendTestFixture {
protected:
  std::map<int, std::unique_ptr<PeeringState>> shard_peering_states;
  std::map<int, std::unique_ptr<PeeringCtx>> shard_peering_ctxs;
  std::map<int, std::unique_ptr<MockPeeringListener>> shard_peering_listeners;
  
  std::map<int, std::list<MessageRef>> shard_messages;
  std::map<int, std::list<PGPeeringEventRef>> shard_events;
  
  std::vector<int> up;
  std::vector<int> acting;
  std::vector<int> up_acting;
  int up_primary;
  int acting_primary;

  // Raw-pointer map giving this fixture direct access to the backend listeners
  // created by the listener_factory.  The pointers are valid for the lifetime
  // of the test because ownership is transferred to
  // shard_peering_listeners[i]->backend_listener in create_peering_state().
  std::map<int, MockPGBackendListener*> backend_listeners;
  
  class ShardDpp : public NoDoutPrefix {
  public:
    ECPeeringTestFixture *fixture;
    int shard;
    
    ShardDpp(CephContext *cct, ECPeeringTestFixture *f, int s)
      : NoDoutPrefix(cct, ceph_subsys_osd), fixture(f), shard(s) {}
    
    std::ostream& gen_prefix(std::ostream& out) const override {
      out << "shard " << shard << ": ";
      if (fixture->shard_peering_states.contains(shard)) {
        PeeringState *ps = fixture->shard_peering_states[shard].get();
        out << *ps << " ";
      }
      return out;
    }
  };
  std::map<int, std::unique_ptr<ShardDpp>> shard_dpps;
  
  IsPGRecoverablePredicate *get_is_recoverable_predicate() {
    return new MockECRecPred(k, m);
  }
  
  IsPGReadablePredicate *get_is_readable_predicate() {
    return new MockECReadPred(k, m);
  }

public:
  ECPeeringTestFixture() : PGBackendTestFixture(PGBackendTestFixture::EC) {
    // Install a listener_factory so that setup_ec_pool() creates listeners
    // that we can access directly (via backend_listeners[]) without needing
    // to steal ownership via release_listener().
    //
    // The factory records a raw pointer in backend_listeners[instance] and
    // returns the unique_ptr to the base class, which stores it in listeners[].
    // In create_peering_state() we then move that unique_ptr from listeners[]
    // into shard_peering_listeners[]->backend_listener, at which point the
    // raw pointer in backend_listeners[] remains valid (owned by the peering
    // listener for the rest of the test).
    listener_factory = [this](
      int instance,
      std::shared_ptr<OSDMap> om,
      int64_t pool_id,
      DoutPrefixProvider* dpp_arg,
      pg_shard_t whoami,
      pg_shard_t primary) -> std::unique_ptr<MockPGBackendListener>
    {
      auto bl = std::make_unique<MockPGBackendListener>(
        om, pool_id, dpp_arg, whoami, primary);
      // Record raw pointer so tests can access the listener directly
      backend_listeners[instance] = bl.get();
      return bl;
    };
  }
  
  void SetUp() override {
    PGBackendTestFixture::SetUp();
    setup_up_acting();
    for (int i = 0; i < k + m; i++) {
      create_peering_state(i);
    }
  }
  
  void TearDown() override {
    shard_peering_states.clear();
    shard_peering_ctxs.clear();
    shard_peering_listeners.clear();
    shard_dpps.clear();
    shard_messages.clear();
    shard_events.clear();
    PGBackendTestFixture::TearDown();
  }
  
  void setup_up_acting() {
    up.clear();
    acting.clear();
    up_acting.clear();
    
    for (int i = 0; i < k + m; i++) {
      up.push_back(i);
      acting.push_back(i);
      up_acting.push_back(i);
    }
    
    up_primary = 0;
    acting_primary = 0;
  }
  
  PeeringState* create_peering_state(int shard);
  
  PeeringState* get_peering_state(int shard) {
    return shard_peering_states[shard].get();
  }
  
  PeeringCtx* get_peering_ctx(int shard) {
    return shard_peering_ctxs[shard].get();
  }
  
  MockPeeringListener* get_peering_listener(int shard) {
    return shard_peering_listeners[shard].get();
  }
  
  void init_peering(bool dne = false);
  
  void event_initialize() {
    for (int shard : up_acting) {
      auto evt = std::make_shared<PGPeeringEvent>(
        osdmap->get_epoch(),
        osdmap->get_epoch(),
        PeeringState::Initialize());
      
      get_peering_state(shard)->handle_event(evt, get_peering_ctx(shard));
    }
  }
  
  void event_advance_map() {
    for (int shard : up_acting) {
      get_peering_state(shard)->advance_map(
        osdmap, osdmap, up, up_primary, acting, acting_primary,
        *get_peering_ctx(shard));
    }
  }
  
  void event_activate_map() {
    for (int shard : up_acting) {
      get_peering_state(shard)->activate_map(*get_peering_ctx(shard));
    }
  }
  
private:
  // Dispatch all messages from a map<int, Container<MessageRef>>.
  // Templated to work with both std::vector (PeeringCtx::message_map) and
  // std::list (MockPeeringListener::messages).
  template <typename Container>
  bool dispatch_messages_from_map(int from_shard,
                                  std::map<int, Container>& msg_map) {
    bool did_work = false;

    for (auto& [to_shard, msg_list] : msg_map) {
      if (std::find(up_acting.begin(), up_acting.end(), to_shard) == up_acting.end()) {
        continue;
      }

      while (!msg_list.empty()) {
        MessageRef m = msg_list.front();
        msg_list.erase(msg_list.begin());

        // Cast to MOSDPeeringOp - all peering messages inherit from this.
        // Use dynamic_cast with assertion to catch unexpected message types.
        // Use m.get() (not m.detach()) to avoid leaking the raw pointer.
        MOSDPeeringOp *op = dynamic_cast<MOSDPeeringOp*>(m.get());
        ceph_assert(op != nullptr) /* message must be a MOSDPeeringOp */;

        // Set connection peer to the SENDER, not the destination
        ceph_msg_header h = op->get_header();
        h.src.num = from_shard;
        op->set_header(h);

        ConnectionRef conn = new MockConnection(from_shard);
        op->set_connection(conn);

        // get_event() returns a newly allocated PGPeeringEvent,
        // so we take ownership directly into a shared_ptr (matching OSD.cc pattern)
        PGPeeringEventRef evt_ref(op->get_event());

        get_peering_state(to_shard)->handle_event(
          evt_ref,
          get_peering_ctx(to_shard));

        did_work = true;
      }
    }

    return did_work;
  }

public:
  bool dispatch_peering_messages(int from_shard) {
    auto* ctx = get_peering_ctx(from_shard);
    return dispatch_messages_from_map(from_shard, ctx->message_map);
  }

  bool dispatch_cluster_messages(int from_shard) {
    auto& listener = shard_peering_listeners[from_shard];
    return dispatch_messages_from_map(from_shard, listener->messages);
  }
  
  bool dispatch_all_peering_messages() {
    bool did_work = false;
    bool work_this_round;
    
    do {
      work_this_round = false;
      for (int shard : up_acting) {
        // Skip failed OSDs (marked as CRUSH_ITEM_NONE)
        if (shard == CRUSH_ITEM_NONE) {
          continue;
        }
        work_this_round |= dispatch_peering_messages(shard);
      }
      did_work |= work_this_round;
    } while (work_this_round);
    
    return did_work;
  }
  
  bool dispatch_events(int shard, bool stalled = false) {
    auto& listener = shard_peering_listeners[shard];
    std::list<PGPeeringEventRef>& event_queue = 
      stalled ? listener->stalled_events : listener->events;
    
    if (event_queue.empty()) {
      return false;
    }
    
    bool did_work = false;
    while (!event_queue.empty()) {
      PGPeeringEventRef evt = event_queue.front();
      event_queue.pop_front();
      
      get_peering_state(shard)->handle_event(evt, get_peering_ctx(shard));
      did_work = true;
    }
    
    return did_work;
  }
  
  bool dispatch_all_events(bool stalled = false) {
    bool did_work = false;
    bool work_this_round;
    
    do {
      work_this_round = false;
      for (int shard : up_acting) {
        // Skip failed OSDs (marked as CRUSH_ITEM_NONE)
        if (shard == CRUSH_ITEM_NONE) {
          continue;
        }
        work_this_round |= dispatch_events(shard, stalled);
      }
      did_work |= work_this_round;
    } while (work_this_round);
    
    return did_work;
  }
  
  bool dispatch_all_cluster_messages() {
    bool did_work = false;
    bool work_this_round;
    
    do {
      work_this_round = false;
      for (int shard : up_acting) {
        // Skip failed OSDs (marked as CRUSH_ITEM_NONE)
        if (shard == CRUSH_ITEM_NONE) {
          continue;
        }
        work_this_round |= dispatch_cluster_messages(shard);
      }
      did_work |= work_this_round;
    } while (work_this_round);
    
    return did_work;
  }
  
  bool dispatch_all() {
    bool did_work = false;
    bool work_this_round;
    
    do {
      work_this_round = false;
      work_this_round |= dispatch_all_peering_messages();
      work_this_round |= dispatch_all_cluster_messages();
      work_this_round |= dispatch_all_events();
      did_work |= work_this_round;
    } while (work_this_round);
    
    return did_work;
  }
  
  // IMPORTANT: For EC pools, shard positions in acting array must be preserved.
  // Failed OSDs should be replaced with CRUSH_ITEM_NONE, not removed.
  void update_osdmap_with_peering(
    std::shared_ptr<OSDMap> new_osdmap,
    std::optional<pg_shard_t> new_primary = std::nullopt);

  bool new_epoch(bool if_required = false);

  int queue_transaction_helper(int shard, ObjectStore::Transaction&& t);

  // ECPeeringTestFixture overrides this to append log entries to PeeringState.
  void pre_transaction_hook(
    const hobject_t& hoid,
    const std::vector<pg_log_entry_t>& log_entries,
    const eversion_t& at_version) override;

  void run_peering_cycle();
  
  bool all_shards_active() {
    for (int shard : up_acting) {
      // Skip failed OSDs (marked as CRUSH_ITEM_NONE)
      if (shard == CRUSH_ITEM_NONE) {
        continue;
      }
      if (!get_peering_state(shard)->is_active()) {
        return false;
      }
    }
    return true;
  }
  
  // In EC pools, only the primary tracks PG_STATE_CLEAN.
  bool all_shards_clean() {
    if (acting_primary >= 0 && acting_primary != CRUSH_ITEM_NONE) {
      return get_peering_state(acting_primary)->is_clean();
    }
    return false;
  }
  
  std::string get_state_name(int shard) {
    return get_peering_state(shard)->get_current_state();
  }
};

