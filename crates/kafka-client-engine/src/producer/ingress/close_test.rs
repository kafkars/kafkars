//! Producer close fast-fence, capacity rollback, and wake-failure scenarios.

use std::{io, sync::Arc, time::Instant};

use bytes::Bytes;
use kafka_client_core::{AdmissionRejection, Deadline, Moment, PartitionIndex};

use super::{
    ProducerPortAcceptedFault, ProducerPortAdmissionError, ProducerPortFlushError,
    ProducerPortRejectionReason, ProducerShardOwner, ProducerShardWake,
};
use crate::{
    clock::OperationDeadline,
    producer::{
        ProducerRecord, ProducerRejectionReason,
        flush::FlushRejectionReason,
        host_limits_test::{start, valid_limits},
    },
};

#[test]
fn wake_failure_keeps_the_clone_shared_close_fence() {
    let wake = Arc::new(FailingWake);
    let owner = ProducerShardOwner::new(start(valid_limits()), wake);
    let port = owner.admission_port();
    let clone = port.clone();
    let accepted = port
        .try_admit_close(Moment::from_tick(1))
        .unwrap_or_else(|error| panic!("close must remain accepted: {error:?}"));
    let (observer, flush_id, fault) = accepted.into_parts();

    assert!(flush_id.is_some());
    assert!(matches!(fault, Err(ProducerPortAcceptedFault::Wake(_))));
    assert!(matches!(
        clone.try_admit_close(Moment::from_tick(2)),
        Err(ProducerPortFlushError::Rejected(
            super::super::flush::FlushRejectionReason::Closed
        ))
    ));
    assert!(matches!(
        clone.try_admit_flush(Moment::from_tick(2)),
        Err(ProducerPortFlushError::Rejected(
            super::super::flush::FlushRejectionReason::Closed
        ))
    ));
    assert_eq!(observer.wait(), Ok(()));
}

#[test]
fn accepted_close_fast_rejects_while_the_host_lock_is_contended() {
    let wake = Arc::new(super::shard_test::CountingWake::default());
    let owner = ProducerShardOwner::new(start(valid_limits()), wake);
    let port = owner.admission_port();
    let clone = port.clone();
    let accepted = port
        .try_admit_close(Moment::from_tick(1))
        .unwrap_or_else(|error| panic!("close should be accepted: {error:?}"));
    let guard = owner
        .try_data()
        .unwrap_or_else(|error| panic!("test should hold the host lock: {error:?}"));

    assert_closed_send(clone.try_admit_explicit(Moment::from_tick(2), deadline(), record()));
    assert_closed_flush(clone.try_admit_flush(Moment::from_tick(2)));
    assert_closed_flush(clone.try_admit_close(Moment::from_tick(2)));

    drop(guard);
    assert_eq!(accepted.into_parts().0.wait(), Ok(()));
}

#[test]
fn failed_close_capacity_leaves_the_fast_fence_open() {
    let wake = Arc::new(super::shard_test::CountingWake::default());
    let owner = ProducerShardOwner::new(start(valid_limits()), wake);
    let port = owner.admission_port();
    let mut guard = owner
        .try_data()
        .unwrap_or_else(|error| panic!("test should fill terminal capacity: {error:?}"));
    let limits = valid_limits();
    let capacity = limits
        .completion_capacity
        .saturating_add(limits.waiting_record_capacity);
    let reservations: Vec<_> = (0..capacity)
        .map(|_| {
            guard
                .host
                .completions
                .reserve()
                .unwrap_or_else(|error| panic!("test reservation should fit: {error}"))
        })
        .collect();
    drop(guard);

    assert!(matches!(
        port.try_admit_close(Moment::from_tick(1)),
        Err(ProducerPortFlushError::Rejected(
            FlushRejectionReason::Completion(crate::completion::CompletionRegistryError::Full)
        ))
    ));

    let mut guard = owner
        .try_data()
        .unwrap_or_else(|error| panic!("test should force lock contention: {error:?}"));
    assert!(matches!(
        port.try_admit_flush(Moment::from_tick(2)),
        Err(ProducerPortFlushError::Contended)
    ));
    assert!(matches!(
        port.try_admit_close(Moment::from_tick(2)),
        Err(ProducerPortFlushError::Contended)
    ));
    assert!(matches!(
        port.try_admit_explicit(Moment::from_tick(2), deadline(), record()),
        Err(ProducerPortAdmissionError::Rejected(ref rejected))
            if rejected.reason() == ProducerPortRejectionReason::Contended
    ));

    for (completion_id, observer) in reservations {
        assert_eq!(
            guard.host.completions.rollback_reservation(completion_id),
            Ok(())
        );
        drop(observer);
    }
}

struct FailingWake;

impl ProducerShardWake for FailingWake {
    fn wake(&self) -> Result<(), super::ProducerShardWakeError> {
        Err(super::ProducerShardWakeError::from_io(io::Error::other(
            "close wake failed",
        )))
    }
}

fn assert_closed_send(result: Result<super::ProducerPortAccepted, ProducerPortAdmissionError>) {
    let Err(ProducerPortAdmissionError::Rejected(rejected)) = result else {
        panic!("accepted close must fast-reject later record admission")
    };
    assert_eq!(
        rejected.reason(),
        ProducerPortRejectionReason::Host(ProducerRejectionReason::Core(
            AdmissionRejection::Closed
        ))
    );
}

fn assert_closed_flush(result: Result<super::ProducerPortFlushAccepted, ProducerPortFlushError>) {
    let Some(ProducerPortFlushError::Rejected(reason)) = result.err() else {
        panic!("accepted close must fast-reject later barrier admission")
    };
    assert!(matches!(reason, FlushRejectionReason::Closed));
}

fn record() -> ProducerRecord {
    ProducerRecord::new(
        Arc::from("orders"),
        PartitionIndex::from_raw(0),
        10,
        None,
        Some(Bytes::from_static(b"value")),
    )
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(90), Instant::now())
}
