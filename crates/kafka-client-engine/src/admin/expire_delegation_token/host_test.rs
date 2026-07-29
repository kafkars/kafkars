//! Admission envelope, abandonment, capacity, and shutdown recovery scenarios.

use std::sync::Arc;

use kafka_client_core::{
    ExpireDelegationTokenHmac as CoreHmac, ExpireDelegationTokenMachineError,
    ExpireDelegationTokenPlan,
};

use crate::{
    EngineConfig,
    admin::AdminCompletionNotifier,
    clock::MonotonicClock,
    driver::{DriverOwner, ExpireDelegationTokenCall},
    protocol::admin::expire_delegation_token::{
        EXPIRE_DELEGATION_TOKEN_MAX_RETAINED_BYTES, ExpireDelegationTokenRequestRef,
        PreparedExpireDelegationTokenRequest, expire_delegation_token_request,
    },
};

use super::{
    EXPIRE_DELEGATION_TOKEN_CAPACITY, ExpireDelegationTokenAdmissionErrorKind,
    ExpireDelegationTokenDeliveryStatus, ExpireDelegationTokenFailureKind,
    ExpireDelegationTokenHost, ExpireDelegationTokenHostError, ExpireDelegationTokenOutcome,
    ExpireDelegationTokenTurn,
    host::{EXPIRE_DELEGATION_TOKEN_OPERATION_BYTES, EXPIRE_DELEGATION_TOKEN_RETAINED_BYTES},
};

#[test]
fn admission_reserves_one_mib_and_dropped_observer_does_not_cancel() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = ExpireDelegationTokenHost::new(ports.expire_delegation_token);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(),
            prepared(),
        )
        .unwrap_or_else(|error| panic!("admit token expiration: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(
        host.retained_bytes_for_test(),
        EXPIRE_DELEGATION_TOKEN_OPERATION_BYTES
    );

    let ExpireDelegationTokenTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, prepared) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert!(submitted_plan.hmac().as_bytes() == TOKEN_HMAC);
    assert_eq!(submitted_plan.expiry_period_ms(), Some(60_000));
    let debug = format!("{prepared:?}");
    assert!(debug.contains("[REDACTED]"));
    drop((submitted_plan, prepared));

    drop(admission.observer);
    host.reject_handoff(operation_id)
        .unwrap_or_else(|error| panic!("reject inspected handoff: {error}"));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover abandoned observation: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn sixteen_operations_are_reserved_before_capacity_rejection() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = ExpireDelegationTokenHost::new(ports.expire_delegation_token);
    let capture = deadline();
    let mut observers = Vec::new();
    for _ in 0..EXPIRE_DELEGATION_TOKEN_CAPACITY {
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
        EXPIRE_DELEGATION_TOKEN_RETAINED_BYTES
    );
    assert!(matches!(
        host.try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(),
            prepared()
        ),
        Err(ExpireDelegationTokenAdmissionErrorKind::Capacity)
    ));
    drop(observers);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn untouched_shutdown_recovery_is_definitely_unsent() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = ExpireDelegationTokenHost::new(ports.expire_delegation_token);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(),
            prepared(),
        )
        .unwrap_or_else(|error| panic!("admit recovery: {error:?}"));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover query: {error}"));
    let ExpireDelegationTokenOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(
        failure.kind(),
        ExpireDelegationTokenFailureKind::DriverRejected
    );
    assert_eq!(
        failure.delivery(),
        ExpireDelegationTokenDeliveryStatus::NotSent
    );
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn handed_off_without_a_returned_call_cannot_forge_recovery_evidence() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = ExpireDelegationTokenHost::new(ports.expire_delegation_token);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(),
            prepared(),
        )
        .unwrap_or_else(|error| panic!("admit recovery: {error:?}"));
    let ExpireDelegationTokenTurn::Submit(_submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(ExpireDelegationTokenHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_call_and_secret_correlation_survive_core_rejection() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = ExpireDelegationTokenHost::new(ports.expire_delegation_token);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(),
            prepared(),
        )
        .unwrap_or_else(|error| panic!("admit recovery: {error:?}"));
    host.retain_recovered_call_for_test(plan());

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(ExpireDelegationTokenHostError::Machine(
            ExpireDelegationTokenMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_ownership_matches_for_test(TOKEN_HMAC, Some(60_000)));
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(ExpireDelegationTokenHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_call_and_secret_correlation_until_recovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = ExpireDelegationTokenHost::new(ports.expire_delegation_token);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan(),
            prepared(),
        )
        .unwrap_or_else(|error| panic!("admit recovery: {error:?}"));
    let ExpireDelegationTokenTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, prepared) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = ExpireDelegationTokenCall::submit(
        &driver,
        submitted_plan,
        prepared,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(capture.now()),
        Err(ExpireDelegationTokenHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let ExpireDelegationTokenOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), ExpireDelegationTokenFailureKind::Transport);
    assert_eq!(
        failure.delivery(),
        ExpireDelegationTokenDeliveryStatus::PossiblySent
    );

    drop(host);
    stop_notifier(&mut notifier);
}

const TOKEN_HMAC: &[u8] = &[0xA5, 0x5A, 0xC3, 0x3C];

fn plan() -> ExpireDelegationTokenPlan {
    ExpireDelegationTokenPlan::new(
        CoreHmac::new(TOKEN_HMAC.to_vec()).unwrap_or_else(|error| panic!("hmac: {error}")),
        Some(60_000),
    )
    .unwrap_or_else(|error| panic!("plan: {error}"))
}

fn prepared() -> PreparedExpireDelegationTokenRequest {
    expire_delegation_token_request(
        ExpireDelegationTokenRequestRef::explicit(TOKEN_HMAC, 60_000),
        EXPIRE_DELEGATION_TOKEN_MAX_RETAINED_BYTES,
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
