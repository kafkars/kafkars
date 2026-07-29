//! Admission envelope, abandonment, capacity, and shutdown recovery scenarios.

use std::sync::Arc;

use kafka_client_core::{CreateDelegationTokenPlan, DelegationTokenPrincipal};

use crate::{
    admin::AdminCompletionNotifier,
    clock::MonotonicClock,
    protocol::admin::create_delegation_token::{
        CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES, CreateDelegationTokenRequestRef,
        DelegationTokenPrincipalRef, create_delegation_token_request,
    },
};

use super::{
    CREATE_DELEGATION_TOKEN_CAPACITY, CreateDelegationTokenAdmissionErrorKind,
    CreateDelegationTokenDeliveryStatus, CreateDelegationTokenFailureKind,
    CreateDelegationTokenHost, CreateDelegationTokenOutcome, CreateDelegationTokenTurn,
    host::{CREATE_DELEGATION_TOKEN_OPERATION_BYTES, CREATE_DELEGATION_TOKEN_RETAINED_BYTES},
};

#[test]
fn admission_reserves_completion_and_one_mib_envelope_before_submission() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = CreateDelegationTokenHost::new(ports.create_delegation_token);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(),
            prepared(),
        )
        .unwrap_or_else(|error| panic!("admit token creation: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(
        host.retained_bytes_for_test(),
        CREATE_DELEGATION_TOKEN_OPERATION_BYTES
    );

    let CreateDelegationTokenTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (_id, submitted_deadline, submitted_plan, submitted_request) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert_eq!(submitted_plan.renewers()[0].principal_name(), "renewer");
    assert_eq!(submitted_request.minimum_version(), 3);

    drop(admission.observer);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn dropping_observer_does_not_cancel_accepted_work() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = CreateDelegationTokenHost::new(ports.create_delegation_token);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(),
            prepared(),
        )
        .unwrap_or_else(|error| panic!("admit token creation: {error:?}"));
    drop(admission.observer);
    assert!(matches!(
        host.turn(capture.now()),
        Ok(CreateDelegationTokenTurn::Submit(_))
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover abandoned observation: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn sixteen_operations_are_reserved_before_capacity_rejection() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = CreateDelegationTokenHost::new(ports.create_delegation_token);
    let capture = deadline();
    let mut observers = Vec::new();
    for _ in 0..CREATE_DELEGATION_TOKEN_CAPACITY {
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
        CREATE_DELEGATION_TOKEN_RETAINED_BYTES
    );
    assert!(matches!(
        host.try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(),
            prepared()
        ),
        Err(CreateDelegationTokenAdmissionErrorKind::Capacity)
    ));
    drop(observers);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn untouched_shutdown_is_definitely_unsent_and_reclaimable() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = CreateDelegationTokenHost::new(ports.create_delegation_token);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(),
            prepared(),
        )
        .unwrap_or_else(|error| panic!("admit token creation: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover untouched request: {error}"));
    let CreateDelegationTokenOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(
        failure.kind(),
        CreateDelegationTokenFailureKind::DriverRejected
    );
    assert_eq!(
        failure.delivery(),
        CreateDelegationTokenDeliveryStatus::NotSent
    );
    let _progress = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("reclaim turn: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn handed_off_shutdown_is_possibly_sent() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = CreateDelegationTokenHost::new(ports.create_delegation_token);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(),
            prepared(),
        )
        .unwrap_or_else(|error| panic!("admit token creation: {error:?}"));
    assert!(matches!(
        host.turn(capture.now()),
        Ok(CreateDelegationTokenTurn::Submit(_))
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover handed-off request: {error}"));
    let CreateDelegationTokenOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("{error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), CreateDelegationTokenFailureKind::Transport);
    assert_eq!(
        failure.delivery(),
        CreateDelegationTokenDeliveryStatus::PossiblySent
    );
    drop(host);
    stop_notifier(&mut notifier);
}

fn plan() -> CreateDelegationTokenPlan {
    CreateDelegationTokenPlan::new(
        Some(core_principal("owner")),
        vec![core_principal("renewer")],
        Some(60_000),
    )
    .unwrap_or_else(|error| panic!("plan: {error}"))
}

fn prepared()
-> crate::protocol::admin::create_delegation_token::PreparedCreateDelegationTokenRequest {
    let owner = DelegationTokenPrincipalRef::new("User", "owner");
    let renewers = [DelegationTokenPrincipalRef::new("User", "renewer")];
    create_delegation_token_request(
        CreateDelegationTokenRequestRef::new(Some(owner), &renewers, 60_000),
        CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES,
    )
    .unwrap_or_else(|error| panic!("prepared request: {error:?}"))
}

fn core_principal(name: &str) -> DelegationTokenPrincipal {
    DelegationTokenPrincipal::new("User".to_owned(), name.to_owned())
        .unwrap_or_else(|error| panic!("principal: {error}"))
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
