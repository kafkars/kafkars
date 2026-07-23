//! Immediate shard admission, byte-bound, and close-state scenarios.

use std::time::Instant;

use kafka_client_core::{AdmissionRejection, Deadline, Moment};

use super::data::ProducerShardData;
use crate::{
    clock::OperationDeadline,
    producer::{
        ProducerRejectionReason,
        admission::ProducerAdmissionFailure,
        admission_test::record,
        host_limits_test::{start, valid_limits},
    },
};

#[test]
fn construction_installs_one_immediate_admission_owner() {
    let data = ProducerShardData::new(start(valid_limits()));
    let stats = data.shard_stats();

    assert_eq!(stats.host.store.records, 0);
    assert_eq!(stats.host.core_completion_slots, 0);
    assert!(stats.accepting);
}

#[test]
fn immediate_records_share_the_host_byte_ceiling() {
    let mut limits = valid_limits();
    limits.retained_bytes = 7;
    let mut data = ProducerShardData::new(start(limits));
    let accepted = data
        .try_admit_explicit(Moment::from_tick(1), deadline(), record("one"))
        .unwrap_or_else(|error| panic!("first record should be accepted: {error:?}"));

    let rejected = data.try_admit_explicit(Moment::from_tick(1), deadline(), record("two"));
    let Err(ProducerAdmissionFailure::Rejected(rejected)) = rejected else {
        panic!("host byte ceiling should reject the second record")
    };
    assert_eq!(
        rejected.reason(),
        ProducerRejectionReason::Store(crate::producer::ProducerStoreError::ByteCapacity)
    );
    assert_eq!(data.shard_stats().host.store.bytes, 4);
    drop(accepted);
}

#[test]
fn close_atomically_stops_immediate_and_core_admission() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    data.close_admission();

    let accepted = data.try_admit_explicit(Moment::from_tick(1), deadline(), record("core"));
    let Err(ProducerAdmissionFailure::Rejected(rejected)) = accepted else {
        panic!("core admission should reject after shard close")
    };
    assert_eq!(
        rejected.reason(),
        ProducerRejectionReason::Core(AdmissionRejection::Closed)
    );
    assert!(!data.shard_stats().accepting);
}

#[test]
fn producer_close_fences_records_flushes_and_repeated_close() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let first = data
        .try_admit_close(Moment::from_tick(1))
        .unwrap_or_else(|error| panic!("first close should be accepted: {error:?}"));

    assert!(!data.shard_stats().accepting);
    assert!(matches!(
        data.try_admit_explicit(Moment::from_tick(2), deadline(), record("late")),
        Err(ProducerAdmissionFailure::Rejected(rejected))
            if rejected.reason()
                == ProducerRejectionReason::Core(AdmissionRejection::Closed)
    ));
    assert!(matches!(
        data.try_admit_flush(Moment::from_tick(2)),
        Err(super::super::flush::FlushAdmissionFailure::Rejected(
            super::super::flush::FlushRejectionReason::Closed
        ))
    ));

    assert!(matches!(
        data.try_admit_close(Moment::from_tick(2)),
        Err(super::super::flush::FlushAdmissionFailure::Rejected(
            super::super::flush::FlushRejectionReason::Closed
        ))
    ));
    assert_eq!(first.into_flush_observer().wait(), Ok(()));
}

#[test]
fn close_capacity_failure_leaves_record_and_flush_admission_running() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let capacity = valid_limits().completion_capacity;
    let reservations: Vec<_> = (0..capacity)
        .map(|_| {
            data.host
                .completions
                .reserve()
                .unwrap_or_else(|error| panic!("test reservation should fit: {error}"))
        })
        .collect();

    assert!(matches!(
        data.try_admit_close(Moment::from_tick(1)),
        Err(super::super::flush::FlushAdmissionFailure::Rejected(
            super::super::flush::FlushRejectionReason::Completion(
                crate::completion::CompletionRegistryError::Full
            )
        ))
    ));
    assert!(data.shard_stats().accepting);
    assert!(data.host.core.admission_is_open());
    for (completion_id, observer) in reservations {
        assert_eq!(
            data.host.completions.rollback_reservation(completion_id),
            Ok(())
        );
        drop(observer);
    }
    let flush = data
        .try_admit_flush(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("failed close must leave flush open: {error:?}"));
    assert!(
        data.try_admit_explicit(Moment::from_tick(2), deadline(), record("still-open"))
            .is_ok()
    );
    assert_eq!(flush.into_flush_observer().wait(), Ok(()));
}

#[test]
fn accepted_close_invariant_still_fences_shared_admission() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    data.host
        .inject_terminal_publish_fault(crate::completion::CompletionRegistryError::NotifierStopped);

    assert!(matches!(
        data.try_admit_close(Moment::from_tick(1)),
        Err(super::super::flush::FlushAdmissionFailure::AcceptedInvariant { .. })
    ));
    assert!(!data.shard_stats().accepting);
    assert!(!data.host.core.admission_is_open());
    assert!(matches!(
        data.try_admit_flush(Moment::from_tick(2)),
        Err(super::super::flush::FlushAdmissionFailure::Rejected(
            super::super::flush::FlushRejectionReason::Closed
        ))
    ));
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(90), Instant::now())
}
