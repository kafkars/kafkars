//! Admission, observation abandonment, and shutdown recovery scenarios.

use std::sync::Arc;

use kafka_client_core::{AddRaftVoterEndpoint, AddRaftVoterPlan};

use crate::{admin::AdminCompletionNotifier, clock::MonotonicClock};

use super::{
    AddRaftVoterAdmissionErrorKind, AddRaftVoterDeliveryStatus, AddRaftVoterFailureKind,
    AddRaftVoterHost, AddRaftVoterOutcome, AddRaftVoterTurn,
    host::{ADD_RAFT_VOTER_RESULT_BYTES, ADD_RAFT_VOTER_RETAINED_BYTES},
};

#[test]
fn admission_reserves_terminal_and_request_result_bytes_before_submission() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AddRaftVoterHost::new(ports.add_raft_voter);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(7))
        .unwrap_or_else(|error| panic!("admit voter addition: {error:?}"));
    assert!(admission.fault.is_none());
    let operation_bytes = host.retained_bytes_for_test();
    assert!(operation_bytes > ADD_RAFT_VOTER_RESULT_BYTES);
    assert!(operation_bytes < ADD_RAFT_VOTER_RETAINED_BYTES);

    let AddRaftVoterTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (_id, submitted_deadline, submitted_plan, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert_eq!(submitted_plan.voter_id(), 7);
    assert_eq!(result_limit, ADD_RAFT_VOTER_RESULT_BYTES);

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
    let mut host = AddRaftVoterHost::new(ports.add_raft_voter);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(7))
        .unwrap_or_else(|error| panic!("admit voter addition: {error:?}"));
    drop(admission.observer);

    assert!(matches!(
        host.turn(capture.now()),
        Ok(AddRaftVoterTurn::Submit(_))
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover abandoned observation: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn capacity_is_bounded_independently_of_the_aggregate_envelope() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AddRaftVoterHost::new(ports.add_raft_voter);
    let capture = deadline();
    let mut observers = Vec::new();
    for voter_id in 0..super::ADD_RAFT_VOTER_CAPACITY {
        let voter_id = i32::try_from(voter_id).unwrap_or_else(|_| panic!("bounded voter id"));
        observers.push(
            host.try_admit(capture.now(), capture.operation_deadline(), plan(voter_id))
                .unwrap_or_else(|error| panic!("admit {voter_id}: {error:?}"))
                .observer,
        );
    }
    assert!(matches!(
        host.try_admit(capture.now(), capture.operation_deadline(), plan(99)),
        Err(AddRaftVoterAdmissionErrorKind::Capacity)
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
    let mut host = AddRaftVoterHost::new(ports.add_raft_voter);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(7))
        .unwrap_or_else(|error| panic!("admit voter addition: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover untouched request: {error}"));
    let AddRaftVoterOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), AddRaftVoterFailureKind::DriverRejected);
    assert_eq!(failure.delivery(), AddRaftVoterDeliveryStatus::NotSent);

    let _progress = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("reclaim turn: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);
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
