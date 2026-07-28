//! Exact grouped rejection, accepted-call, raw-terminal, and recovery evidence.

use std::sync::Arc;

use kafka_client_core::{AlterReplicaLogDirAssignment, AlterReplicaLogDirsPlan};

use crate::{
    EngineConfig,
    admin::{AdminCompletionNotifier, AlterReplicaLogDirsHost, AlterReplicaLogDirsTurn},
    clock::MonotonicClock,
    driver::{AlterReplicaLogDirsCall, DriverOwner},
};

use super::super::AlterReplicaLogDirsHostError;

#[test]
fn rejection_requires_exact_route_group_order_and_bounds() {
    let (mut host, mut notifier, clock) = host();
    let capture = deadline(&clock);
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit: {error:?}"));
    let AlterReplicaLogDirsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, broker_id, assignments, request_scratch_limit, result_limit) =
        submission.into_parts();
    let reversed = assignments.iter().cloned().rev().collect::<Vec<_>>();

    assert!(matches!(
        host.reject_handoff(
            operation_id,
            broker_id,
            reversed,
            request_scratch_limit,
            result_limit,
        ),
        Err(AlterReplicaLogDirsHostError::SubmissionMismatch)
    ));
    assert!(matches!(
        host.reject_handoff(
            operation_id,
            broker_id + 1,
            assignments.clone(),
            request_scratch_limit,
            result_limit,
        ),
        Err(AlterReplicaLogDirsHostError::SubmissionMismatch)
    ));
    assert!(matches!(
        host.reject_handoff(
            operation_id,
            broker_id,
            assignments.clone(),
            request_scratch_limit - 1,
            result_limit,
        ),
        Err(AlterReplicaLogDirsHostError::SubmissionMismatch)
    ));
    host.reject_handoff(
        operation_id,
        broker_id,
        assignments,
        request_scratch_limit,
        result_limit,
    )
    .unwrap_or_else(|error| panic!("reject exact evidence: {error}"));

    drop(admission.observer);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn mismatched_accepted_group_survives_driver_shutdown_as_evidence() {
    let (mut host, mut notifier, clock) = host();
    let capture = deadline(&clock);
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit: {error:?}"));
    let AlterReplicaLogDirsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, deadline, broker_id, assignments, request_scratch_limit, result_limit) =
        submission.into_parts();
    let reversed = assignments.iter().cloned().rev().collect::<Vec<_>>();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = AlterReplicaLogDirsCall::submit(
        &driver,
        broker_id,
        reversed.clone(),
        request_scratch_limit,
        result_limit,
        deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));

    assert!(matches!(
        host.accept_call(operation_id, call),
        Err(AlterReplicaLogDirsHostError::SubmissionMismatch)
    ));
    drop(driver);
    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(AlterReplicaLogDirsHostError::SubmissionMismatch)
    ));
    assert!(host.recovered_matches_for_test(
        broker_id,
        &reversed,
        request_scratch_limit,
        result_limit,
    ));
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AlterReplicaLogDirsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn mismatched_raw_group_blocks_core_settlement_and_publication() {
    let (mut host, mut notifier, clock) = host();
    let capture = deadline(&clock);
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit: {error:?}"));
    let AlterReplicaLogDirsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, deadline, broker_id, assignments, request_scratch_limit, result_limit) =
        submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = AlterReplicaLogDirsCall::submit(
        &driver,
        broker_id,
        assignments.clone(),
        request_scratch_limit,
        result_limit,
        deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);
    host.replace_call_with_raw_for_test(
        broker_id,
        assignments,
        request_scratch_limit,
        result_limit - 1,
    );

    assert!(matches!(
        host.settle_raw_for_test(),
        Err(AlterReplicaLogDirsHostError::SubmissionMismatch)
    ));
    assert!(host.raw_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AlterReplicaLogDirsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

fn host() -> (
    AlterReplicaLogDirsHost,
    AdminCompletionNotifier,
    Arc<MonotonicClock>,
) {
    let (notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    (
        AlterReplicaLogDirsHost::new(ports.alter_replica_log_dirs),
        notifier,
        Arc::new(MonotonicClock::new()),
    )
}

fn deadline(clock: &Arc<MonotonicClock>) -> crate::clock::DeadlineCapture {
    clock
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

fn plan() -> AlterReplicaLogDirsPlan {
    AlterReplicaLogDirsPlan::new(vec![
        assignment(9, "orders", 0, "/data-a"),
        assignment(2, "orders", 1, "/data-b"),
        assignment(9, "orders", 2, "/data-c"),
    ])
    .unwrap_or_else(|error| panic!("plan: {error}"))
}

fn assignment(
    broker_id: i32,
    topic: &str,
    partition: i32,
    log_dir: &str,
) -> AlterReplicaLogDirAssignment {
    AlterReplicaLogDirAssignment::new(broker_id, topic.to_owned(), partition, log_dir.to_owned())
}
