//! Admission envelope, abandonment, capacity, and shutdown recovery scenarios.

use std::sync::Arc;

use kafka_client_core::{RenewDelegationTokenHmac as CoreHmac, RenewDelegationTokenPlan};

use crate::{
    admin::AdminCompletionNotifier,
    clock::MonotonicClock,
    protocol::admin::renew_delegation_token::{
        PreparedRenewDelegationTokenRequest, RENEW_DELEGATION_TOKEN_MAX_RETAINED_BYTES,
        RenewDelegationTokenRequestRef, renew_delegation_token_request,
    },
};

use super::{
    RENEW_DELEGATION_TOKEN_CAPACITY, RenewDelegationTokenAdmissionErrorKind,
    RenewDelegationTokenDeliveryStatus, RenewDelegationTokenFailureKind, RenewDelegationTokenHost,
    RenewDelegationTokenOutcome, RenewDelegationTokenTurn,
    host::{RENEW_DELEGATION_TOKEN_OPERATION_BYTES, RENEW_DELEGATION_TOKEN_RETAINED_BYTES},
};

#[test]
fn admission_reserves_one_mib_and_dropped_observer_does_not_cancel() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = RenewDelegationTokenHost::new(ports.renew_delegation_token);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(),
            prepared(),
        )
        .unwrap_or_else(|error| panic!("admit token renewal: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(
        host.retained_bytes_for_test(),
        RENEW_DELEGATION_TOKEN_OPERATION_BYTES
    );

    let RenewDelegationTokenTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (_id, submitted_deadline, plan, prepared) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert_eq!(plan.hmac().as_bytes(), b"renew-secret");
    assert_eq!(plan.renew_period_ms(), Some(60_000));
    let debug = format!("{prepared:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("renew-secret"));

    drop(admission.observer);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover abandoned observation: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn sixteen_operations_are_reserved_before_capacity_rejection() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = RenewDelegationTokenHost::new(ports.renew_delegation_token);
    let capture = deadline();
    let mut observers = Vec::new();
    for _ in 0..RENEW_DELEGATION_TOKEN_CAPACITY {
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
        RENEW_DELEGATION_TOKEN_RETAINED_BYTES
    );
    assert!(matches!(
        host.try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(),
            prepared()
        ),
        Err(RenewDelegationTokenAdmissionErrorKind::Capacity)
    ));
    drop(observers);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn shutdown_recovery_preserves_delivery_boundary() {
    recover_case(
        false,
        RenewDelegationTokenFailureKind::DriverRejected,
        RenewDelegationTokenDeliveryStatus::NotSent,
    );
    recover_case(
        true,
        RenewDelegationTokenFailureKind::Transport,
        RenewDelegationTokenDeliveryStatus::PossiblySent,
    );
}

fn recover_case(
    hand_off: bool,
    kind: RenewDelegationTokenFailureKind,
    delivery: RenewDelegationTokenDeliveryStatus,
) {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = RenewDelegationTokenHost::new(ports.renew_delegation_token);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(),
            prepared(),
        )
        .unwrap_or_else(|error| panic!("admit recovery: {error:?}"));
    if hand_off {
        assert!(matches!(
            host.turn(capture.now()),
            Ok(RenewDelegationTokenTurn::Submit(_))
        ));
    }
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover query: {error}"));
    let RenewDelegationTokenOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
    drop(host);
    stop_notifier(&mut notifier);
}

fn plan() -> RenewDelegationTokenPlan {
    RenewDelegationTokenPlan::new(
        CoreHmac::new(b"renew-secret".to_vec()).unwrap_or_else(|error| panic!("hmac: {error}")),
        Some(60_000),
    )
    .unwrap_or_else(|error| panic!("plan: {error}"))
}

fn prepared() -> PreparedRenewDelegationTokenRequest {
    renew_delegation_token_request(
        RenewDelegationTokenRequestRef::explicit(b"renew-secret", 60_000),
        RENEW_DELEGATION_TOKEN_MAX_RETAINED_BYTES,
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
