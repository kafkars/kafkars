//! Atomic pending identity, record, and sole-observer registration scenarios.

use std::{sync::Arc, task::Poll, time::Instant};

use bytes::Bytes;
use kafka_client_core::{Deadline, PartitionIndex};

use super::{
    PendingAdmissionRegistry, PendingAttemptRestoreOutcome,
    test_support::{CountingWake, poll_send},
};
use crate::{clock::OperationDeadline, producer::ProducerRecord};

#[test]
fn registration_returns_the_observer_for_the_exact_bounded_entry() {
    let mut registry = PendingAdmissionRegistry::new(1, 64, 1);
    let registration = registry
        .register(
            record(),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(19), Instant::now()),
        )
        .unwrap_or_else(|error| panic!("pending registration should succeed: {error:?}"));
    let id = registration.id();
    let mut send = registration.into_send();
    assert_eq!(poll_send(&mut send, CountingWake::new()), Poll::Pending);

    let attempt = registry
        .take_next(1)
        .unwrap_or_else(|error| panic!("pending entry should remain indexed: {error:?}"))
        .into_attempt()
        .unwrap_or_else(|| panic!("registered entry should exist"));
    assert_eq!(
        attempt
            .retained_admission_for_test()
            .unwrap_or_else(|| panic!("attempt should retain admission"))
            .id(),
        id
    );
    drop(send);
    let restored = attempt
        .restore(&mut registry)
        .unwrap_or_else(|_failure| panic!("dropped observer restore should resolve"));
    let PendingAttemptRestoreOutcome::Abandoned(pending) = restored else {
        panic!("dropped observer should remove the temporarily restored entry");
    };
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
