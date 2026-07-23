//! Atomic admission, intact rejection, wake, and poison scenarios.

use std::{io, sync::Arc, thread, time::Instant};

use bytes::Bytes;
use kafka_client_core::{AdmissionRejection, Deadline, Moment, PartitionIndex};

use super::{
    ProducerPortAcceptedFault, ProducerPortAdmissionError, ProducerPortPoison,
    ProducerPortPoisonReason, ProducerPortRejectionReason, ProducerShardOwner, ProducerShardWake,
    ProducerShardWakeError, shard_test::CountingWake,
};
use crate::{
    clock::OperationDeadline,
    producer::{
        ProducerHostInvariantError, ProducerRecord, ProducerRejectionReason, ProducerStoreError,
        host_limits_test::{start, valid_limits},
    },
};

#[test]
fn acceptance_synchronously_transfers_all_owned_capacity_and_wakes_once() {
    let (owner, port, wake) = setup();
    let observer = admit(&port, record("orders"));

    let stats = host(&owner).stats();
    assert_eq!(stats.store.records, 1);
    assert_eq!(stats.core_completion_slots, 1);
    assert_eq!(wake.count(), 1);
    drop(observer);
}

#[test]
fn post_commit_wake_failure_preserves_observer_and_owned_capacity() {
    let wake = Arc::new(FailingWake);
    let owner = ProducerShardOwner::new(start(valid_limits()), Arc::clone(&wake));
    let port = owner.admission_port();
    let accepted = match port.try_admit_explicit(now(), deadline(), record("orders")) {
        Ok(accepted) => accepted,
        Err(error) => panic!("admission should commit before waking: {error:?}"),
    };
    let (observer, operation_id, fault) = accepted.into_parts();

    let Err(ProducerPortAcceptedFault::Wake(error)) = fault else {
        panic!("test wake should fail")
    };
    assert_eq!(
        operation_id.map(kafka_client_core::OperationId::get),
        Some(1)
    );
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert_eq!(host(&owner).stats().core_completion_slots, 1);
    drop(observer);
}

#[test]
fn contention_is_immediate_and_returns_the_exact_record() {
    let (owner, port, wake) = setup();
    let guard = host(&owner);
    let topic: Arc<str> = Arc::from("orders");
    let expected = Arc::clone(&topic);
    let result = thread::scope(|scope| {
        scope
            .spawn(move || port.try_admit_explicit(now(), deadline(), record_from(topic)))
            .join()
    });
    let Ok(result) = result else {
        panic!("contention worker should return normally")
    };
    let rejected = rejected(result);

    assert_eq!(rejected.reason(), ProducerPortRejectionReason::Contended);
    assert!(Arc::ptr_eq(rejected.into_record().topic(), &expected));
    assert_eq!(wake.count(), 0);
    drop(guard);
}

#[test]
fn closed_admission_is_observed_once_the_shard_lock_is_available() {
    let (owner, port, wake) = setup();
    owner
        .close_admission()
        .unwrap_or_else(|error| panic!("test should close producer admission: {error:?}"));

    let rejected = rejected(port.try_admit_explicit(now(), deadline(), record("closed")));

    assert_eq!(
        rejected.reason(),
        ProducerPortRejectionReason::Host(ProducerRejectionReason::Core(
            AdmissionRejection::Closed
        ))
    );
    assert_eq!(wake.count(), 0);
}

#[test]
fn bounded_local_rejection_rolls_back_and_returns_the_exact_record() {
    let mut limits = valid_limits();
    limits.retained_bytes = 6;
    let wake = Arc::new(CountingWake::default());
    let owner = ProducerShardOwner::new(start(limits), Arc::clone(&wake));
    let port = owner.admission_port();
    let topic: Arc<str> = Arc::from("orders");
    let expected = Arc::clone(&topic);
    let rejected = rejected(port.try_admit_explicit(now(), deadline(), record_from(topic)));

    assert_eq!(
        rejected.reason(),
        ProducerPortRejectionReason::Host(ProducerRejectionReason::Store(
            ProducerStoreError::ByteCapacity
        ))
    );
    assert!(Arc::ptr_eq(rejected.into_record().topic(), &expected));
    assert_eq!(host(&owner).stats().core_completion_slots, 0);
    assert_eq!(wake.count(), 0);
}

#[test]
fn observer_abandonment_does_not_cancel_admitted_host_work() {
    let (owner, port, wake) = setup();
    let observer = admit(&port, record("orders"));
    drop(observer);

    let stats = host(&owner).stats();
    assert_eq!(stats.store.records, 1);
    assert_eq!(stats.core_completion_slots, 1);
    assert_eq!(wake.count(), 1);
}

#[test]
fn wake_is_requested_only_after_a_committed_admission() {
    let (owner, port, wake) = setup();
    let guard = host(&owner);
    let rejected = port.try_admit_explicit(now(), deadline(), record("blocked"));
    assert!(matches!(
        rejected,
        Err(ProducerPortAdmissionError::Rejected(_))
    ));
    assert_eq!(wake.count(), 0);
    drop(guard);

    let observer = admit(&port, record("accepted"));
    assert_eq!(wake.count(), 1);
    drop(observer);
}

#[test]
fn poisoned_host_is_not_reported_as_semantic_backpressure() {
    let (owner, port, wake) = setup();
    host(&owner).inject_post_acceptance_fault(ProducerHostInvariantError::MissingAdmissionIdentity);
    let first = port.try_admit_explicit(now(), deadline(), record("first"));
    let Ok(accepted) = first else {
        panic!("post-ownership fault must remain top-level accepted")
    };
    let (observer, operation_id, fault) = accepted.into_parts();
    let Err(ProducerPortAcceptedFault::HostInvariant(error)) = fault else {
        panic!("accepted host invariant must remain explicit")
    };
    assert_eq!(error, ProducerHostInvariantError::MissingAdmissionIdentity);
    assert_eq!(
        operation_id.map(kafka_client_core::OperationId::get),
        Some(1)
    );
    drop(observer);

    let topic: Arc<str> = Arc::from("second");
    let expected = Arc::clone(&topic);
    let second = port.try_admit_explicit(now(), deadline(), record_from(topic));
    let Err(ProducerPortAdmissionError::Poisoned(ProducerPortPoison::BeforeAdmission {
        reason,
        record,
    })) = second
    else {
        panic!("poisoned host should return caller ownership explicitly")
    };
    assert_eq!(
        reason,
        ProducerPortPoisonReason::Host(ProducerHostInvariantError::MissingAdmissionIdentity)
    );
    assert!(Arc::ptr_eq(record.topic(), &expected));
    assert_eq!(wake.count(), 0);
}

fn setup() -> (
    ProducerShardOwner,
    super::ProducerAdmissionPort,
    Arc<CountingWake>,
) {
    let wake = Arc::new(CountingWake::default());
    let owner = ProducerShardOwner::new(start(valid_limits()), Arc::clone(&wake));
    let port = owner.admission_port();
    (owner, port, wake)
}

fn host(owner: &ProducerShardOwner) -> std::sync::MutexGuard<'_, crate::producer::ProducerHost> {
    match owner.try_host() {
        Ok(host) => host,
        Err(error) => panic!("test should acquire producer shard: {error:?}"),
    }
}

fn admit(
    port: &super::ProducerAdmissionPort,
    record: ProducerRecord,
) -> crate::ProducerDeliveryObserver {
    match port.try_admit_explicit(now(), deadline(), record) {
        Ok(accepted) => {
            let (observer, operation_id, fault) = accepted.into_parts();
            assert!(operation_id.is_some());
            assert!(fault.is_ok());
            observer
        }
        Err(error) => panic!("admission should succeed: {error:?}"),
    }
}

fn rejected(
    result: Result<super::ProducerPortAccepted, ProducerPortAdmissionError>,
) -> super::ProducerPortRejected {
    match result {
        Err(ProducerPortAdmissionError::Rejected(rejected)) => rejected,
        Err(error) => panic!("expected healthy rejection: {error:?}"),
        Ok(_observer) => panic!("admission should reject"),
    }
}

struct FailingWake;

impl ProducerShardWake for FailingWake {
    fn wake(&self) -> Result<(), ProducerShardWakeError> {
        Err(ProducerShardWakeError::from_io(io::Error::other(
            "test wake failure",
        )))
    }
}

fn record(topic: &str) -> ProducerRecord {
    record_from(Arc::from(topic))
}

fn record_from(topic: Arc<str>) -> ProducerRecord {
    ProducerRecord::new(
        topic,
        PartitionIndex::from_raw(0),
        10,
        None,
        Some(Bytes::from_static(b"x")),
    )
}

const fn now() -> Moment {
    Moment::from_tick(10)
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(90), Instant::now())
}
