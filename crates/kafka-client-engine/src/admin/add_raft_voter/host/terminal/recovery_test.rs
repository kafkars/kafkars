//! Missing-call, core-rejection, and completion-fault recovery scenarios.

use std::sync::Arc;

use kafka_client_core::{AddRaftVoterEndpoint, AddRaftVoterMachineError, AddRaftVoterPlan};

use crate::{
    EngineConfig,
    admin::{AddRaftVoterHost, AdminCompletionNotifier},
    clock::MonotonicClock,
    driver::{AddRaftVoterCall, DriverOwner},
};

use super::super::super::{
    AddRaftVoterDeliveryStatus, AddRaftVoterFailureKind, AddRaftVoterHostError,
    AddRaftVoterOutcome, AddRaftVoterTurn,
};

#[test]
fn handed_off_without_a_returned_call_cannot_forge_recovery_evidence() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AddRaftVoterHost::new(ports.add_raft_voter);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(7))
        .unwrap_or_else(|error| panic!("admit voter addition: {error:?}"));
    let AddRaftVoterTurn::Submit(_submission) = host
        .turn(capture.now(), None)
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(AddRaftVoterHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_call_and_exact_voter_plan_survive_core_rejection() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AddRaftVoterHost::new(ports.add_raft_voter);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(7))
        .unwrap_or_else(|error| panic!("admit voter addition: {error:?}"));
    host.retain_recovered_call_for_test(plan(7));

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(AddRaftVoterHostError::Machine(
            AddRaftVoterMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_ownership_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AddRaftVoterHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_accepted_call_until_post_driver_recovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AddRaftVoterHost::new(ports.add_raft_voter);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(7))
        .unwrap_or_else(|error| panic!("admit voter addition: {error:?}"));
    let AddRaftVoterTurn::Submit(submission) = host
        .turn(capture.now(), None)
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, _result_limit) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call =
        AddRaftVoterCall::submit(&driver, &submitted_plan, submitted_deadline, capture.now())
            .unwrap_or_else(|error| panic!("accepted call: {error}"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(capture.now(), None),
        Err(AddRaftVoterHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let AddRaftVoterOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("recovery observation: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(failure.kind(), AddRaftVoterFailureKind::Transport);
    assert_eq!(failure.delivery(), AddRaftVoterDeliveryStatus::PossiblySent);

    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn missing_driver_during_controller_refresh_preserves_terminal_for_shutdown_recovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AddRaftVoterHost::new(ports.add_raft_voter);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(7))
        .unwrap_or_else(|error| panic!("admit voter addition: {error:?}"));
    let AddRaftVoterTurn::Submit(submission) = host
        .turn(capture.now(), None)
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, _result_limit) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call =
        AddRaftVoterCall::submit(&driver, &submitted_plan, submitted_deadline, capture.now())
            .unwrap_or_else(|error| panic!("accepted call: {error}"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    host.retain_controller_refresh_for_test(submitted_plan);

    assert!(matches!(
        host.turn(capture.now(), None),
        Err(AddRaftVoterHostError::DriverMissing)
    ));
    assert!(host.raw_terminal_is_retained_for_test());

    drop(driver);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let AddRaftVoterOutcome::BrokerRejected(error) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("recovery observation: {error}"))
    else {
        panic!("exact broker rejection expected");
    };
    assert_eq!(error.code(), 41);

    drop(host);
    stop_notifier(&mut notifier);
}

fn plan(voter_id: i32) -> AddRaftVoterPlan {
    AddRaftVoterPlan::new(
        Some("cluster-a".to_owned()),
        voter_id,
        [7; 16],
        vec![AddRaftVoterEndpoint::new(
            "CONTROLLER".to_owned(),
            "controller-a".to_owned(),
            9093,
        )],
    )
    .unwrap_or_else(|error| panic!("plan: {error}"))
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
