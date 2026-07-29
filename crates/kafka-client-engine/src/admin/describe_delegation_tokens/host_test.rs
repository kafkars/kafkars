//! Admission envelope, abandonment, capacity, and shutdown recovery scenarios.

use std::sync::Arc;

use kafka_client_core::DescribeDelegationTokensPlan;

use crate::{
    admin::AdminCompletionNotifier,
    clock::MonotonicClock,
    protocol::admin::describe_delegation_tokens::{
        DESCRIBE_DELEGATION_TOKENS_MAX_RETAINED_BYTES, DescribeDelegationTokensRequestRef,
        PreparedDescribeDelegationTokensRequest, describe_delegation_tokens_request,
    },
};

use super::{
    DESCRIBE_DELEGATION_TOKENS_CAPACITY, DescribeDelegationTokensAdmissionErrorKind,
    DescribeDelegationTokensDeliveryStatus, DescribeDelegationTokensFailureKind,
    DescribeDelegationTokensHost, DescribeDelegationTokensOutcome, DescribeDelegationTokensTurn,
    host::{DESCRIBE_DELEGATION_TOKENS_OPERATION_BYTES, DESCRIBE_DELEGATION_TOKENS_RETAINED_BYTES},
};

#[test]
fn admission_reserves_completion_and_four_mib_envelope_before_submission() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeDelegationTokensHost::new(ports.describe_delegation_tokens);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(),
            prepared(),
        )
        .unwrap_or_else(|error| panic!("admit token query: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(
        host.retained_bytes_for_test(),
        DESCRIBE_DELEGATION_TOKENS_OPERATION_BYTES
    );

    let DescribeDelegationTokensTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, submitted_request) =
        submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert!(matches!(
        submitted_plan.selection(),
        kafka_client_core::DescribeDelegationTokensSelection::All
    ));
    drop(submitted_request);

    drop(admission.observer);
    host.reject_handoff(operation_id)
        .unwrap_or_else(|error| panic!("reject inspected handoff: {error}"));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn dropping_observer_does_not_cancel_accepted_work() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeDelegationTokensHost::new(ports.describe_delegation_tokens);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(),
            prepared(),
        )
        .unwrap_or_else(|error| panic!("admit token query: {error:?}"));
    drop(admission.observer);
    let DescribeDelegationTokensTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, _plan, _request) = submission.into_parts();
    host.reject_handoff(operation_id)
        .unwrap_or_else(|error| panic!("reject abandoned observation handoff: {error}"));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover abandoned observation: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn sixteen_operations_are_reserved_before_capacity_rejection() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeDelegationTokensHost::new(ports.describe_delegation_tokens);
    let capture = deadline();
    let mut observers = Vec::new();
    for _ in 0..DESCRIBE_DELEGATION_TOKENS_CAPACITY {
        observers.push(
            host.try_admit(
                capture.now(),
                capture.operation_deadline(),
                plan(),
                prepared(),
            )
            .unwrap_or_else(|error| panic!("admit: {error:?}"))
            .observer,
        );
    }
    assert_eq!(
        host.retained_bytes_for_test(),
        DESCRIBE_DELEGATION_TOKENS_RETAINED_BYTES
    );
    assert!(matches!(
        host.try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(),
            prepared()
        ),
        Err(DescribeDelegationTokensAdmissionErrorKind::Capacity)
    ));
    drop(observers);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn untouched_shutdown_recovery_is_definitely_unsent() {
    recover_case(
        DescribeDelegationTokensFailureKind::DriverRejected,
        DescribeDelegationTokensDeliveryStatus::NotSent,
    );
}

fn recover_case(
    kind: DescribeDelegationTokensFailureKind,
    delivery: DescribeDelegationTokensDeliveryStatus,
) {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeDelegationTokensHost::new(ports.describe_delegation_tokens);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(),
            prepared(),
        )
        .unwrap_or_else(|error| panic!("admit recovery query: {error:?}"));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover query: {error}"));
    assert_failure(admission.observer.wait(), kind, delivery);
    drop(host);
    stop_notifier(&mut notifier);
}

fn assert_failure(
    outcome: Result<DescribeDelegationTokensOutcome, super::DescribeDelegationTokensObserverError>,
    kind: DescribeDelegationTokensFailureKind,
    delivery: DescribeDelegationTokensDeliveryStatus,
) {
    let DescribeDelegationTokensOutcome::Failed(failure) =
        outcome.unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
}

fn plan() -> DescribeDelegationTokensPlan {
    DescribeDelegationTokensPlan::all()
}

fn prepared() -> PreparedDescribeDelegationTokensRequest {
    describe_delegation_tokens_request(
        DescribeDelegationTokensRequestRef::all(),
        DESCRIBE_DELEGATION_TOKENS_MAX_RETAINED_BYTES,
    )
    .unwrap_or_else(|error| panic!("prepared request: {error:?}"))
}

fn deadline() -> crate::clock::DeadlineCapture {
    Arc::new(MonotonicClock::new())
        .capture_deadline_after(std::time::Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("deadline: {error}"))
}

fn stop_notifier(notifier: &mut AdminCompletionNotifier) {
    notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}
