//! Missing-call, core-rejection, and completion-fault recovery scenarios.

use kafka_client_core::{
    AbortPartitionTransactionMachineError, AbortPartitionTransactionPlan, Moment,
};

use crate::{
    EngineConfig,
    admin::{AbortPartitionTransactionHost, AdminCompletionNotifier},
    driver::{AbortPartitionTransactionCall, DriverOwner},
};

use super::super::super::{
    AbortPartitionTransactionDeliveryStatus, AbortPartitionTransactionFailureKind,
    AbortPartitionTransactionHostError, AbortPartitionTransactionOutcome,
    AbortPartitionTransactionTurn,
};

#[test]
fn handed_off_without_a_returned_call_cannot_forge_recovery_evidence() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AbortPartitionTransactionHost::new(ports.abort_partition_transaction);
    let operation_deadline = deadline();
    let admission = host
        .try_admit(Moment::from_tick(0), operation_deadline, plan("orders"))
        .unwrap_or_else(|error| panic!("admit partition abort: {error:?}"));
    let AbortPartitionTransactionTurn::Submit(_submission) = host
        .turn(Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(AbortPartitionTransactionHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_call_and_exact_transaction_plan_survive_core_rejection() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AbortPartitionTransactionHost::new(ports.abort_partition_transaction);
    let expected = plan("orders");
    let admission = host
        .try_admit(Moment::from_tick(0), deadline(), expected.clone())
        .unwrap_or_else(|error| panic!("admit partition abort: {error:?}"));
    host.retain_recovered_call_for_test(expected.clone());

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(AbortPartitionTransactionHostError::Machine(
            AbortPartitionTransactionMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_plan_matches_for_test(&expected));
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AbortPartitionTransactionHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_accepted_call_until_post_driver_recovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AbortPartitionTransactionHost::new(ports.abort_partition_transaction);
    let operation_deadline = deadline();
    let admission = host
        .try_admit(Moment::from_tick(0), operation_deadline, plan("orders"))
        .unwrap_or_else(|error| panic!("admit partition abort: {error:?}"));
    let AbortPartitionTransactionTurn::Submit(submission) = host
        .turn(Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = AbortPartitionTransactionCall::submit(
        &driver,
        submitted_plan,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(0)),
        Err(AbortPartitionTransactionHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let AbortPartitionTransactionOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("recovery observation: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        failure.kind(),
        AbortPartitionTransactionFailureKind::Transport
    );
    assert_eq!(
        failure.delivery(),
        AbortPartitionTransactionDeliveryStatus::PossiblySent
    );

    drop(host);
    stop_notifier(&mut notifier);
}

fn plan(topic: &str) -> AbortPartitionTransactionPlan {
    AbortPartitionTransactionPlan::new(topic.to_owned(), 3, 41, 7, 11)
        .unwrap_or_else(|error| panic!("valid plan: {error}"))
}

fn deadline() -> crate::clock::OperationDeadline {
    std::sync::Arc::new(crate::clock::MonotonicClock::new())
        .capture_deadline_after(std::time::Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("deadline: {error}"))
        .operation_deadline()
}

fn stop_notifier(notifier: &mut AdminCompletionNotifier) {
    notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}
