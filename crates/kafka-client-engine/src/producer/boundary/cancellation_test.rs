//! Public cancellation type and missing-capability scenarios.

use crate::{
    ProducerCancelErrorKind, ProducerCancellationOutcome, ProducerDeliveryObserver,
    completion::CompletionRegistry, producer::ProducerTerminal,
};

#[test]
fn observer_without_an_operation_capability_reports_host_unavailable() {
    let mut registry = CompletionRegistry::<ProducerTerminal>::new(1, 1)
        .unwrap_or_else(|error| panic!("completion registry failed: {error}"));
    let (completion_id, inner) = registry
        .reserve()
        .unwrap_or_else(|error| panic!("completion reservation failed: {error}"));
    let observer = ProducerDeliveryObserver::from_completion(inner);

    let error = observer
        .try_cancel()
        .err()
        .unwrap_or_else(|| panic!("raw observer must not invent a host"));

    assert_eq!(error.kind(), ProducerCancelErrorKind::HostUnavailable);
    assert_eq!(registry.rollback_reservation(completion_id), Ok(()));
    drop(observer);
    let join = registry
        .stop_notifier()
        .unwrap_or_else(|error| panic!("notifier stop failed: {error}"));
    assert_eq!(join.join_off_notifier(), Ok(()));
}

#[test]
fn cancellation_outcomes_remain_three_distinct_closed_values() {
    assert_ne!(
        ProducerCancellationOutcome::CancelledNotSent,
        ProducerCancellationOutcome::TooLate
    );
    assert_ne!(
        ProducerCancellationOutcome::TooLate,
        ProducerCancellationOutcome::AlreadyTerminal
    );
}
