//! Deliberate claim duplication, mutation, construction, and capability theft.

use std::thread;
use std::time::{Instant, SystemTime};

use kafka_driver;
use kafka_wire;

#[derive(Clone, Copy)]
struct AssignedConsumerClaimSlot {
    port: Vec<u8>,
}

#[derive(Clone, Copy)]
struct AssignedConsumerAdmissionCloser;

#[derive(Clone, Copy)]
struct AssignedConsumerHandle;

#[derive(Clone, Copy)]
struct AssignedConsumerTryCloseAccepted;

impl AssignedConsumerClaimSlot {
    fn create_for_engine() -> Self {
        Self { port: Vec::new() }
    }

    fn violate(&mut self) {
        self.port.clear();
    }
}

async fn violate_capabilities() {
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
    let _slot = AssignedConsumerClaimSlot::create_for_engine();
}
