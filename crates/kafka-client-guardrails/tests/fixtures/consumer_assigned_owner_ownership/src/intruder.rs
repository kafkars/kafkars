//! Deliberately invalid assigned-owner duplication, mutation, and capabilities.

use kafka_driver as kafka_driver;
use kafka_wire as kafka_wire;
use std::thread;
use std::time::{Instant, SystemTime};

#[derive(Clone, Copy)]
struct AssignedConsumerOwner {
    machine: usize,
    topics: usize,
    timers: usize,
    positions: usize,
    fetches: usize,
    close: usize,
    effects: usize,
    raw_position_deadlines: usize,
    pending_positions: usize,
    pending_fetches: usize,
    fault: usize,
    reclaim_faults: usize,
    reclaim_overflow: usize,
}

fn mutate(owner: &mut AssignedConsumerOwner) {
    owner.machine = 1;
    owner.topics = 1;
    owner.timers = 1;
    owner.positions = 1;
    owner.fetches = 1;
    owner.close = 1;
    owner.effects = 1;
    owner.raw_position_deadlines = 1;
    owner.pending_positions = 1;
    owner.pending_fetches = 1;
    owner.fault = 1;
    owner.reclaim_faults = 1;
    owner.reclaim_overflow = 1;
}

struct DirectFetchExecutor;
struct AssignedCloseSlot;
struct FetchAttemptDeadline;

impl DirectFetchExecutor {
    fn create_unbound() -> Self {
        Self
    }
}

impl AssignedCloseSlot {
    fn create_for_assigned_owner() -> Self {
        Self
    }
}

impl FetchAttemptDeadline {
    fn capture_for_fetch() -> Self {
        Self
    }
}

fn steal_protected_calls() {
    let _fetch = DirectFetchExecutor::create_unbound();
    let _close = AssignedCloseSlot::create_for_assigned_owner();
    let _deadline = FetchAttemptDeadline::capture_for_fetch();
}

async fn raw_runtime() {
    let _driver = kafka_driver;
    let _wire = kafka_wire;
    let _tokio = tokio::spawn;
    let _async_std = async_std::task::spawn;
    let _smol = smol::spawn;
    let _thread = thread::spawn;
    let _instant = Instant::now();
    let _system = SystemTime::now();
    let _callback = Callback;
    let _metadata = Metadata;
    let _retry = Retry;
}
