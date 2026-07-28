//! Admission, first-appearance grouping, and shutdown-recovery scenarios.

use std::sync::Arc;

use kafka_client_core::{
    AlterReplicaLogDirAssignment, AlterReplicaLogDirsMachineError, AlterReplicaLogDirsPlan, Moment,
};

use crate::{
    EngineConfig,
    admin::{
        AdminCompletionNotifier, AlterReplicaLogDirEngineResult, AlterReplicaLogDirsDeliveryStatus,
        AlterReplicaLogDirsFailureKind, AlterReplicaLogDirsHost, AlterReplicaLogDirsOutcome,
        AlterReplicaLogDirsTurn,
    },
    clock::MonotonicClock,
    driver::{AlterReplicaLogDirsCall, DriverOwner},
};

use super::AlterReplicaLogDirsHostError;

#[test]
fn admission_preserves_first_broker_appearance_and_group_relative_order() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AlterReplicaLogDirsHost::new(ports.alter_replica_log_dirs);
    let clock = Arc::new(MonotonicClock::new());
    let capture = clock
        .capture_deadline_after(std::time::Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("deadline: {error}"));
    let plan = AlterReplicaLogDirsPlan::new(vec![
        assignment(9, "orders", 0, "/data-a"),
        assignment(2, "orders", 1, "/data-b"),
        assignment(9, "orders", 2, "/data-c"),
    ])
    .unwrap_or_else(|error| panic!("plan: {error}"));
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan)
        .unwrap_or_else(|error| panic!("admit: {error:?}"));
    assert!(admission.fault.is_none());
    assert!(host.retained_bytes_for_test() > 0);

    let AlterReplicaLogDirsTurn::Submit(submission) = host
        .turn(Moment::from_tick(capture.now().tick()))
        .unwrap_or_else(|error| panic!("turn: {error}"))
    else {
        panic!("expected first submission");
    };
    let (operation_id, _, broker_id, assignments, request_scratch_limit, result_limit) =
        submission.into_parts();
    assert_eq!(broker_id, 9);
    assert_eq!(assignments.len(), 2);
    assert_eq!(assignments[0].partition(), 0);
    assert_eq!(assignments[1].partition(), 2);

    host.reject_handoff(
        operation_id,
        broker_id,
        assignments,
        request_scratch_limit,
        result_limit,
    )
    .unwrap_or_else(|error| panic!("reject handoff: {error}"));
    drop(admission.observer);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover: {error}"));
    drop(host);
    notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}

#[test]
fn handed_off_without_a_returned_call_cannot_forge_recovery_evidence() {
    let (mut host, mut notifier, clock) = host();
    let capture = clock
        .capture_deadline_after(std::time::Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("deadline: {error}"));
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit: {error:?}"));
    let AlterReplicaLogDirsTurn::Submit(_submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("turn: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(AlterReplicaLogDirsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_call_and_assignments_remain_retained_when_core_rejects_terminal_fact() {
    let (mut host, mut notifier, clock) = host();
    let capture = clock
        .capture_deadline_after(std::time::Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("deadline: {error}"));
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit: {error:?}"));
    let AlterReplicaLogDirsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (_, _, broker_id, assignments, request_scratch_limit, result_limit) =
        submission.into_parts();
    host.retain_recovered_call_for_test(
        broker_id,
        assignments,
        request_scratch_limit,
        result_limit,
    );

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(AlterReplicaLogDirsHostError::Machine(
            AlterReplicaLogDirsMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_ownership_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AlterReplicaLogDirsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_call_until_post_driver_recovery() {
    let (mut host, mut notifier, clock) = host();
    let capture = clock
        .capture_deadline_after(std::time::Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("deadline: {error}"));
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
        assignments,
        request_scratch_limit,
        result_limit,
        deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(capture.now()),
        Err(AlterReplicaLogDirsHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let AlterReplicaLogDirsOutcome::Altered(batch) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("recovery observation: {error}"))
    else {
        panic!("broker-scoped recovery results expected");
    };
    let (_throttle, outcomes) = batch.into_parts();
    assert_eq!(outcomes.len(), 3);
    let (_, _, _, AlterReplicaLogDirEngineResult::OperationFailed(first)) =
        outcomes[0].clone().into_parts()
    else {
        panic!("first broker transport failure expected");
    };
    assert_eq!(first.kind(), AlterReplicaLogDirsFailureKind::Transport);
    assert_eq!(
        first.delivery(),
        AlterReplicaLogDirsDeliveryStatus::PossiblySent
    );
    let (_, _, _, AlterReplicaLogDirEngineResult::OperationFailed(second)) =
        outcomes[1].clone().into_parts()
    else {
        panic!("unattempted broker failure expected");
    };
    assert_eq!(
        (second.kind(), second.delivery()),
        (
            AlterReplicaLogDirsFailureKind::NotAttempted,
            AlterReplicaLogDirsDeliveryStatus::NotSent,
        )
    );

    drop(host);
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
