//! Contention, host lifetime, close, wake, and abandonment scenarios.

use std::{io, sync::Arc, time::Instant};

use kafka_client_core::{Deadline, Moment};

use crate::{
    ProducerCancelErrorKind, ProducerCancelFaultKind, ProducerCancellationOutcome,
    ProducerDeliveryError, ProducerDeliveryFailureKind, ProducerDeliveryStatus,
    clock::OperationDeadline,
    completion::CompletionRegistryError,
    producer::{
        ProducerHostInvariantError,
        admission_test::record,
        host_limits_test::{start, valid_limits},
        reclaim::CompletionReclaimOutcome,
    },
};

use super::{
    CountingWake, ProducerAdmissionPort, ProducerPortAcceptedFault, ProducerPortCancelError,
    ProducerShardOwner, ProducerShardWake, ProducerShardWakeError,
};

#[test]
fn contention_is_immediate_and_retry_uses_the_same_operation() {
    let (owner, port, wake) = setup();
    let observer = admit(&port);
    let guard = owner
        .try_data()
        .unwrap_or_else(|error| panic!("test should lock shard: {error:?}"));

    let error = observer
        .try_cancel()
        .err()
        .unwrap_or_else(|| panic!("held shard must report contention"));
    assert_eq!(error.kind(), ProducerCancelErrorKind::Contended);
    drop(guard);

    let accepted = observer
        .try_cancel()
        .unwrap_or_else(|error| panic!("retry should cancel: {error}"));
    assert_eq!(
        accepted.outcome(),
        ProducerCancellationOutcome::CancelledNotSent
    );
    assert_eq!(wake.count(), 2);
    assert_cancelled(observer);
}

#[test]
fn weak_capability_and_poisoned_host_report_unavailable() {
    let (owner, port, _wake) = setup();
    let lost = admit(&port);
    drop(port);
    drop(owner);
    assert_eq!(
        lost.try_cancel()
            .err()
            .unwrap_or_else(|| panic!("lost host must reject"))
            .kind(),
        ProducerCancelErrorKind::HostUnavailable
    );
    drop(lost);

    let (owner, port, _wake) = setup();
    owner
        .try_data()
        .unwrap_or_else(|error| panic!("test should lock shard: {error:?}"))
        .inject_post_acceptance_fault(ProducerHostInvariantError::MissingAdmissionIdentity);
    let accepted = port
        .try_admit_explicit(Moment::from_tick(0), deadline(), record("poisoned"))
        .unwrap_or_else(|error| panic!("post-core fault remains accepted: {error:?}"));
    let (observer, _operation_id, fault) = accepted.into_parts();
    assert!(matches!(
        fault,
        Err(ProducerPortAcceptedFault::HostInvariant(_))
    ));
    assert_eq!(
        observer
            .try_cancel()
            .err()
            .unwrap_or_else(|| panic!("poisoned host must reject"))
            .kind(),
        ProducerCancelErrorKind::HostUnavailable
    );
}

#[test]
fn cancellation_remains_available_after_close() {
    let (owner, port, _wake) = setup();
    let observer = admit(&port);
    let close = port
        .try_admit_close(Moment::from_tick(1))
        .unwrap_or_else(|error| panic!("close should be accepted: {error:?}"));

    let accepted = observer
        .try_cancel()
        .unwrap_or_else(|error| panic!("close must not remove cancellation: {error}"));

    assert_eq!(
        accepted.outcome(),
        ProducerCancellationOutcome::CancelledNotSent
    );
    assert_cancelled(observer);
    assert_eq!(close.into_parts().0.wait(), Ok(()));
    drop(owner);
}

#[test]
fn wake_failure_is_advisory_after_successful_interpretation() {
    let wake = Arc::new(FailingWake);
    let owner = ProducerShardOwner::new(start(valid_limits()), wake);
    let port = owner.admission_port();
    let observer = admit(&port);

    let accepted = observer
        .try_cancel()
        .unwrap_or_else(|error| panic!("wake failure cannot revoke outcome: {error}"));

    assert_eq!(
        accepted.outcome(),
        ProducerCancellationOutcome::CancelledNotSent
    );
    assert_eq!(
        accepted
            .fault()
            .map(crate::producer::ProducerCancelFault::kind),
        Some(ProducerCancelFaultKind::Wake)
    );
    assert_cancelled(observer);
}

#[test]
fn cancellation_terminal_backpressure_preserves_abandon_reclaim() {
    let (owner, port, _wake) = setup();
    let observer = admit(&port);
    let operation_id = {
        let mut data = owner
            .try_data()
            .unwrap_or_else(|error| panic!("test should lock shard: {error:?}"));
        let operation_id = kafka_client_core::OperationId::from_raw(1);
        data.host
            .inject_terminal_publish_fault(CompletionRegistryError::NotificationBackpressure);
        operation_id
    };

    let accepted = observer
        .try_cancel()
        .unwrap_or_else(|error| panic!("backpressure should retain terminal: {error}"));
    assert_eq!(
        accepted.outcome(),
        ProducerCancellationOutcome::CancelledNotSent
    );
    drop(observer);

    let mut data = owner
        .try_data()
        .unwrap_or_else(|error| panic!("test should relock shard: {error:?}"));
    assert_eq!(data.host.stats().store.records, 0);
    assert_eq!(data.host.stats().terminal_backlog, 1);
    assert_eq!(data.host.retry_terminal_backlog(1), Ok(1));
    let join = data
        .host
        .completions
        .stop_notifier()
        .unwrap_or_else(|error| panic!("notifier stop failed: {error}"));
    assert_eq!(join.join_off_notifier(), Ok(()));
    assert!(matches!(
        data.host.reclaim_one(Moment::from_tick(2)),
        Ok(Some(CompletionReclaimOutcome::Reclaimed { owner, .. }))
            if owner == crate::producer::terminal_backlog::ProducerTerminalOwner::Record(operation_id)
    ));
}

#[test]
fn generation_exhaustion_has_a_stable_public_error_category() {
    let error = crate::ProducerCancelError::from_port(
        ProducerPortCancelError::ExecutionGenerationExhausted,
    );

    assert_eq!(
        error.kind(),
        ProducerCancelErrorKind::ExecutionGenerationExhausted
    );
}

fn setup() -> (ProducerShardOwner, ProducerAdmissionPort, Arc<CountingWake>) {
    let wake = Arc::new(CountingWake::default());
    let owner = ProducerShardOwner::new(start(valid_limits()), Arc::clone(&wake));
    let port = owner.admission_port();
    (owner, port, wake)
}

fn admit(port: &ProducerAdmissionPort) -> crate::ProducerDeliveryObserver {
    let accepted = port
        .try_admit_explicit(Moment::from_tick(0), deadline(), record("orders"))
        .unwrap_or_else(|error| panic!("admission failed: {error:?}"));
    let (observer, operation_id, fault) = accepted.into_parts();
    assert!(operation_id.is_some());
    assert!(matches!(
        fault,
        Ok(()) | Err(ProducerPortAcceptedFault::Wake(_))
    ));
    observer
}

fn assert_cancelled(observer: crate::ProducerDeliveryObserver) {
    let Err(ProducerDeliveryError::Failed(failure)) = observer.wait() else {
        panic!("cancelled observer should fail")
    };
    assert_eq!(failure.kind(), ProducerDeliveryFailureKind::Cancelled);
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(100), Instant::now())
}

struct FailingWake;

impl ProducerShardWake for FailingWake {
    fn wake(&self) -> Result<(), ProducerShardWakeError> {
        Err(ProducerShardWakeError::from_io(io::Error::other(
            "test wake failure",
        )))
    }
}
