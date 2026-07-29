//! Missing-call, core-rejection, and completion-fault recovery scenarios.

use std::sync::Arc;

use kafka_client_core::{
    DelegationTokenPrincipal, DescribeDelegationTokensMachineError, DescribeDelegationTokensPlan,
};

use crate::{
    EngineConfig,
    admin::{AdminCompletionNotifier, DescribeDelegationTokensHost},
    clock::MonotonicClock,
    driver::{DescribeDelegationTokensCall, DriverOwner},
    protocol::admin::describe_delegation_tokens::{
        DESCRIBE_DELEGATION_TOKENS_MAX_RETAINED_BYTES, DescribeDelegationTokenPrincipalRef,
        DescribeDelegationTokensRequestRef, PreparedDescribeDelegationTokensRequest,
        describe_delegation_tokens_request,
    },
};

use super::super::super::{
    DescribeDelegationTokensDeliveryStatus, DescribeDelegationTokensFailureKind,
    DescribeDelegationTokensHostError, DescribeDelegationTokensOutcome,
    DescribeDelegationTokensTurn,
};

#[test]
fn handed_off_without_a_returned_call_cannot_forge_recovery_evidence() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeDelegationTokensHost::new(ports.describe_delegation_tokens);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            owner_plan(),
            prepared_owner(),
        )
        .unwrap_or_else(|error| panic!("admit token query: {error:?}"));
    let DescribeDelegationTokensTurn::Submit(_submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(DescribeDelegationTokensHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_call_and_exact_owner_selection_survive_core_rejection() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeDelegationTokensHost::new(ports.describe_delegation_tokens);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            owner_plan(),
            prepared_owner(),
        )
        .unwrap_or_else(|error| panic!("admit token query: {error:?}"));
    host.retain_recovered_call_for_test(owner_plan());

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(DescribeDelegationTokensHostError::Machine(
            DescribeDelegationTokensMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_ownership_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(DescribeDelegationTokensHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_accepted_call_until_post_driver_recovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeDelegationTokensHost::new(ports.describe_delegation_tokens);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            owner_plan(),
            prepared_owner(),
        )
        .unwrap_or_else(|error| panic!("admit token query: {error:?}"));
    let DescribeDelegationTokensTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, _plan, request) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call =
        DescribeDelegationTokensCall::submit(&driver, request, submitted_deadline.transport())
            .unwrap_or_else(|error| panic!("accepted call: {error}"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(capture.now()),
        Err(DescribeDelegationTokensHostError::CallCompletion)
    ));
    assert!(host.accepted_call_and_correlation_are_retained_for_test());
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let DescribeDelegationTokensOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("recovery observation: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        failure.kind(),
        DescribeDelegationTokensFailureKind::Transport
    );
    assert_eq!(
        failure.delivery(),
        DescribeDelegationTokensDeliveryStatus::PossiblySent
    );

    drop(host);
    stop_notifier(&mut notifier);
}

fn owner_plan() -> DescribeDelegationTokensPlan {
    DescribeDelegationTokensPlan::for_owners(vec![
        DelegationTokenPrincipal::new("User".to_owned(), "alice".to_owned())
            .unwrap_or_else(|error| panic!("valid owner: {error}")),
    ])
    .unwrap_or_else(|error| panic!("valid owner selection: {error}"))
}

fn prepared_owner() -> PreparedDescribeDelegationTokensRequest {
    let owners = [DescribeDelegationTokenPrincipalRef::new("User", "alice")];
    describe_delegation_tokens_request(
        DescribeDelegationTokensRequestRef::selected(&owners),
        DESCRIBE_DELEGATION_TOKENS_MAX_RETAINED_BYTES,
    )
    .unwrap_or_else(|error| panic!("prepared owner query: {error:?}"))
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
