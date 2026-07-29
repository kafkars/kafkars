//! Admission, exact-broker sequencing, recovery, and byte ownership scenarios.

use std::sync::Arc;

use kafka_client_core::{
    DescribeReplicaLogDirsMachineError, DescribeReplicaLogDirsPlan, DescribeReplicaLogDirsReplica,
    Moment,
};

use crate::{
    EngineConfig,
    admin::{AdminCompletionNotifier, DescribeReplicaLogDirsHost, DescribeReplicaLogDirsTurn},
    clock::MonotonicClock,
    driver::{DescribeReplicaLogDirsCall, DriverOwner},
};

use super::{
    DescribeReplicaLogDirsDeliveryStatus, DescribeReplicaLogDirsEngineReplicaResult,
    DescribeReplicaLogDirsFailureKind, DescribeReplicaLogDirsHostError,
    DescribeReplicaLogDirsOutcome,
};

#[test]
fn admission_reserves_before_machine_creation_and_preserves_broker_order() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeReplicaLogDirsHost::new(ports.describe_replica_log_dirs);
    let clock = Arc::new(MonotonicClock::new());
    let capture = clock
        .capture_deadline_after(std::time::Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("deadline: {error}"));
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            DescribeReplicaLogDirsPlan::new(vec![
                replica("orders", 0, 9),
                replica("audit", 1, 2),
                replica("orders", 2, 9),
            ])
            .unwrap_or_else(|error| panic!("plan: {error}")),
        )
        .unwrap_or_else(|error| panic!("admit: {error:?}"));
    assert!(admission.fault.is_none());
    assert!(host.retained_bytes_for_test() > 0);
    let DescribeReplicaLogDirsTurn::Submit(submission) = host
        .turn(Moment::from_tick(capture.now().tick()))
        .unwrap_or_else(|error| panic!("turn: {error}"))
    else {
        panic!("expected first submission");
    };
    let (operation_id, _, broker_id, replicas, retained_limit) = submission.into_parts();
    assert_eq!(broker_id, 9);
    assert_eq!(replicas.len(), 2);
    assert!(retained_limit > 0);

    host.reject_handoff(operation_id)
        .unwrap_or_else(|error| panic!("reject handoff: {error}"));
    drop(admission.observer);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover: {error}"));
    assert_eq!(host.unsettled(), 0);
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
    let DescribeReplicaLogDirsTurn::Submit(_submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("turn: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(DescribeReplicaLogDirsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_call_remains_retained_when_core_rejects_terminal_fact() {
    let (mut host, mut notifier, clock) = host();
    let capture = clock
        .capture_deadline_after(std::time::Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("deadline: {error}"));
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit: {error:?}"));
    host.retain_recovered_call_for_test();

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(DescribeReplicaLogDirsHostError::Machine(
            DescribeReplicaLogDirsMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_call_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(DescribeReplicaLogDirsHostError::InvalidHandoff)
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
    let DescribeReplicaLogDirsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, deadline, broker_id, replicas, retained_limit) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = DescribeReplicaLogDirsCall::submit(
        &driver,
        broker_id,
        &replicas,
        retained_limit,
        deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, replicas, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(capture.now()),
        Err(DescribeReplicaLogDirsHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let DescribeReplicaLogDirsOutcome::Described(batch) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("recovery observation: {error}"));
    let (_throttle, outcomes) = batch.into_parts();
    let (_target, result) = outcomes
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("first outcome"))
        .into_parts();
    let DescribeReplicaLogDirsEngineReplicaResult::OperationFailed(failure) = result else {
        panic!("transport failure expected");
    };
    assert_eq!(failure.kind(), DescribeReplicaLogDirsFailureKind::Transport);
    assert_eq!(
        failure.delivery(),
        DescribeReplicaLogDirsDeliveryStatus::PossiblySent
    );

    drop(host);
    stop_notifier(&mut notifier);
}

fn host() -> (
    DescribeReplicaLogDirsHost,
    AdminCompletionNotifier,
    Arc<MonotonicClock>,
) {
    let (notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    (
        DescribeReplicaLogDirsHost::new(ports.describe_replica_log_dirs),
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

fn plan() -> DescribeReplicaLogDirsPlan {
    DescribeReplicaLogDirsPlan::new(vec![
        replica("orders", 0, 9),
        replica("audit", 1, 2),
        replica("orders", 2, 9),
    ])
    .unwrap_or_else(|error| panic!("plan: {error}"))
}

fn replica(topic: &str, partition: i32, broker_id: i32) -> DescribeReplicaLogDirsReplica {
    DescribeReplicaLogDirsReplica::new(topic.to_owned(), partition, broker_id)
}
