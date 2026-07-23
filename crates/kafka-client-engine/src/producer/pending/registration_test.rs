//! Atomic pending identity, record, and sole-observer registration scenarios.

use std::{sync::Arc, task::Poll, time::Instant};

use bytes::Bytes;
use kafka_client_core::{Deadline, PartitionIndex};

use super::{
    PendingAdmissionRegistry,
    test_support::{CountingWake, poll_send},
};
use crate::producer::ProducerRecord;

#[test]
fn registration_returns_the_observer_for_the_exact_bounded_entry() {
    let mut registry = PendingAdmissionRegistry::new(1, 64);
    let registration = registry
        .register(record(), Deadline::from_tick(19), Instant::now())
        .unwrap_or_else(|error| panic!("pending registration should succeed: {error:?}"));
    let id = registration.id();
    let mut send = registration.into_send();
    assert_eq!(poll_send(&mut send, CountingWake::new()), Poll::Pending);

    let pending = registry
        .take_next()
        .unwrap_or_else(|error| panic!("pending entry should remain indexed: {error:?}"))
        .unwrap_or_else(|| panic!("registered entry should exist"));
    assert_eq!(pending.id(), id);
    drop(send);
    assert!(pending.begin_promotion().is_err());
    assert_eq!(pending.into_record().topic().as_ref(), "orders");
}

fn record() -> ProducerRecord {
    ProducerRecord::new(
        Arc::from("orders"),
        PartitionIndex::from_raw(0),
        1,
        None,
        Some(Bytes::from_static(b"value")),
    )
}
