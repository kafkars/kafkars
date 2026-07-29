//! Exact recovery evidence for one secret-bearing token-creation call.

use std::sync::Arc;

use kafka_client_core::{
    CreateDelegationTokenMachineError, CreateDelegationTokenPlan, DelegationTokenPrincipal,
};

use crate::{
    EngineConfig,
    admin::AdminCompletionNotifier,
    clock::MonotonicClock,
    driver::{CreateDelegationTokenCall, DriverOwner},
    protocol::admin::create_delegation_token::{
        CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES, CreateDelegationTokenRequestRef,
        DelegationTokenPrincipalRef, create_delegation_token_request,
    },
};

use super::super::super::{
    CreateDelegationTokenDeliveryStatus, CreateDelegationTokenFailureKind,
    CreateDelegationTokenHost, CreateDelegationTokenHostError, CreateDelegationTokenOutcome,
    CreateDelegationTokenTurn,
};

#[test]
fn handed_off_without_a_returned_call_cannot_forge_recovery_evidence() {
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

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(CreateDelegationTokenHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_call_remains_retained_when_core_rejects_terminal_fact() {
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
    host.retain_recovered_call_for_test();

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(CreateDelegationTokenHostError::Machine(
            CreateDelegationTokenMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_call_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(CreateDelegationTokenHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_call_until_post_driver_recovery() {
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
    let CreateDelegationTokenTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, _submitted_plan, submitted_request) =
        submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = CreateDelegationTokenCall::submit(
        &driver,
        submitted_request,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(capture.now()),
        Err(CreateDelegationTokenHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let CreateDelegationTokenOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("recovery observation: {error}"))
    else {
        panic!("recovery failure expected");
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
