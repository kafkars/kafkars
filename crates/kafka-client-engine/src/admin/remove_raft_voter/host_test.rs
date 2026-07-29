//! Admission, shutdown recovery, and retained-envelope scenarios.

use std::sync::Arc;

use kafka_client_core::RemoveRaftVoterMachineError;

use crate::{
    EngineConfig,
    admin::AdminCompletionNotifier,
    clock::MonotonicClock,
    driver::{DriverOwner, RemoveRaftVoterCall},
};

use super::{
    RemoveRaftVoterAdmissionErrorKind, RemoveRaftVoterDeliveryStatus, RemoveRaftVoterFailureKind,
    RemoveRaftVoterHost, RemoveRaftVoterHostError, RemoveRaftVoterOutcome, RemoveRaftVoterTurn,
    host::{REMOVE_RAFT_VOTER_RESULT_BYTES, REMOVE_RAFT_VOTER_RETAINED_BYTES},
};

#[test]
fn admission_reserves_terminal_request_copies_and_result_before_submission() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = RemoveRaftVoterHost::new(ports.remove_raft_voter);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(7))
        .unwrap_or_else(|error| panic!("admit voter removal: {error:?}"));
    assert!(admission.fault.is_none());
    let operation_bytes = host.retained_bytes_for_test();
    assert!(operation_bytes > REMOVE_RAFT_VOTER_RESULT_BYTES + (2 * "cluster-a".len()));
    assert!(operation_bytes < REMOVE_RAFT_VOTER_RETAINED_BYTES);

    let RemoveRaftVoterTurn::Submit(submission) = host
        .turn(capture.now(), None)
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert_eq!(submitted_plan.cluster_id(), Some("cluster-a"));
    assert_eq!(submitted_plan.voter_id(), 7);
    assert_eq!(submitted_plan.voter_directory_id(), [9; 16]);
    assert_eq!(result_limit, REMOVE_RAFT_VOTER_RESULT_BYTES);
    drop(submitted_plan);

    drop(admission.observer);
    host.reject_handoff(operation_id)
        .unwrap_or_else(|error| panic!("reject inspected handoff: {error}"));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn capacity_is_bounded_independently_of_the_aggregate_envelope() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = RemoveRaftVoterHost::new(ports.remove_raft_voter);
    let capture = deadline();
    let mut observers = Vec::new();
    for voter_id in 0..super::REMOVE_RAFT_VOTER_CAPACITY {
        let voter_id = i32::try_from(voter_id).unwrap_or_else(|_| panic!("bounded voter id"));
        observers.push(
            host.try_admit(capture.now(), capture.operation_deadline(), plan(voter_id))
                .unwrap_or_else(|error| panic!("admit {voter_id}: {error:?}"))
                .observer,
        );
    }
    assert!(matches!(
        host.try_admit(capture.now(), capture.operation_deadline(), plan(99)),
        Err(RemoveRaftVoterAdmissionErrorKind::Capacity)
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
    let mut host = RemoveRaftVoterHost::new(ports.remove_raft_voter);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(7))
        .unwrap_or_else(|error| panic!("admit voter removal: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover untouched request: {error}"));
    let RemoveRaftVoterOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), RemoveRaftVoterFailureKind::DriverRejected);
    assert_eq!(failure.delivery(), RemoveRaftVoterDeliveryStatus::NotSent);

    let _progress = host
        .turn(capture.now(), None)
        .unwrap_or_else(|error| panic!("reclaim turn: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn handed_off_without_a_returned_call_cannot_forge_recovery_evidence() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = RemoveRaftVoterHost::new(ports.remove_raft_voter);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(7))
        .unwrap_or_else(|error| panic!("admit voter removal: {error:?}"));
    let RemoveRaftVoterTurn::Submit(_submission) = host
        .turn(capture.now(), None)
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(RemoveRaftVoterHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_call_and_voter_plan_survive_core_rejection() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = RemoveRaftVoterHost::new(ports.remove_raft_voter);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(7))
        .unwrap_or_else(|error| panic!("admit voter removal: {error:?}"));
    host.retain_recovered_call_for_test(plan(7));

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(RemoveRaftVoterHostError::Machine(
            RemoveRaftVoterMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_plan_matches_for_test(&plan(7)));
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(RemoveRaftVoterHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_call_and_voter_plan_until_recovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = RemoveRaftVoterHost::new(ports.remove_raft_voter);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(7))
        .unwrap_or_else(|error| panic!("admit voter removal: {error:?}"));
    let RemoveRaftVoterTurn::Submit(submission) = host
        .turn(capture.now(), None)
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, _result_limit) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = RemoveRaftVoterCall::submit(&driver, submitted_plan, submitted_deadline)
        .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(capture.now(), None),
        Err(RemoveRaftVoterHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let RemoveRaftVoterOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), RemoveRaftVoterFailureKind::Transport);
    assert_eq!(
        failure.delivery(),
        RemoveRaftVoterDeliveryStatus::PossiblySent
    );

    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn missing_driver_during_controller_refresh_preserves_terminal_for_shutdown_recovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = RemoveRaftVoterHost::new(ports.remove_raft_voter);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(7))
        .unwrap_or_else(|error| panic!("admit voter removal: {error:?}"));
    let RemoveRaftVoterTurn::Submit(submission) = host
        .turn(capture.now(), None)
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, _result_limit) = submission.into_parts();
    let retained_plan = submitted_plan.clone();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = RemoveRaftVoterCall::submit(&driver, submitted_plan, submitted_deadline)
        .unwrap_or_else(|error| panic!("accepted call: {error}"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    host.retain_controller_refresh_for_test(retained_plan);

    assert!(matches!(
        host.turn(capture.now(), None),
        Err(RemoveRaftVoterHostError::DriverMissing)
    ));
    assert!(host.raw_terminal_is_retained_for_test());

    drop(driver);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let RemoveRaftVoterOutcome::BrokerRejected(error) = admission
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

fn plan(voter_id: i32) -> kafka_client_core::RemoveRaftVoterPlan {
    kafka_client_core::RemoveRaftVoterPlan::new(Some("cluster-a".to_owned()), voter_id, [9; 16])
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
