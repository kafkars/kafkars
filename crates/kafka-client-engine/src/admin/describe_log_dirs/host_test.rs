//! Admission, sequential submission, and accepted-call ownership scenarios.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use kafka_client_core::{
    AdminDescribeLogDirsMachineError, AdminDescribeLogDirsPartition, AdminDescribeLogDirsPlan,
    AdminDescribeLogDirsSelection, Moment,
};

use crate::{
    EngineConfig,
    admin::{
        AdminCompletionNotifier, DescribeLogDirsBrokerFailureKind, DescribeLogDirsDeliveryStatus,
        DescribeLogDirsEngineBrokerResult, DescribeLogDirsHost, DescribeLogDirsOutcome,
        DescribeLogDirsTurn,
    },
    clock::{MonotonicClock, OperationDeadline},
    driver::{DescribeLogDirsCall, DriverOwner},
};

#[test]
fn admission_reserves_before_machine_creation_and_preserves_broker_order() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeLogDirsHost::new(ports.describe_log_dirs);
    let clock = Arc::new(MonotonicClock::new());
    let capture = clock
        .capture_deadline_after(std::time::Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("deadline: {error}"));
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            AdminDescribeLogDirsPlan::new(vec![9, 2])
                .unwrap_or_else(|error| panic!("plan: {error}")),
        )
        .unwrap_or_else(|error| panic!("admit: {error:?}"));
    assert!(admission.fault.is_none());
    assert!(host.retained_bytes_for_test() > 0);
    let DescribeLogDirsTurn::Submit(submission) = host
        .turn(Moment::from_tick(capture.now().tick()))
        .unwrap_or_else(|error| panic!("turn: {error}"))
    else {
        panic!("expected first submission");
    };
    let (_, _, broker_id, selection, request_scratch_limit, result_limit) = submission.into_parts();
    assert_eq!(broker_id, 9);
    assert_eq!(
        selection,
        kafka_client_core::AdminDescribeLogDirsSelection::AllTopics
    );
    assert_eq!(request_scratch_limit, 0);
    assert!(result_limit > 0);
    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(super::DescribeLogDirsHostError::InvalidHandoff)
    ));
    drop(admission.observer);
    drop(host);
    notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}

#[test]
fn recovered_call_remains_retained_when_core_rejects_terminal_fact() {
    let (mut host, mut notifier) = host();
    let admission = host
        .try_admit(
            Moment::from_tick(1),
            deadline(),
            AdminDescribeLogDirsPlan::new(vec![9]).unwrap_or_else(|error| panic!("plan: {error}")),
        )
        .unwrap_or_else(|error| panic!("admit: {error:?}"));
    host.retain_recovered_call_for_test();

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(super::DescribeLogDirsHostError::Machine(
            AdminDescribeLogDirsMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_call_is_retained_for_test());

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_call_until_post_driver_recovery() {
    let (mut host, mut notifier) = host();
    let admission = host
        .try_admit(
            Moment::from_tick(1),
            deadline(),
            AdminDescribeLogDirsPlan::new(vec![9]).unwrap_or_else(|error| panic!("plan: {error}")),
        )
        .unwrap_or_else(|error| panic!("admit: {error:?}"));
    let DescribeLogDirsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (
        operation_id,
        submitted_deadline,
        broker_id,
        selection,
        request_scratch_limit,
        result_limit,
    ) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = DescribeLogDirsCall::submit(
        &driver,
        broker_id,
        selection,
        request_scratch_limit,
        result_limit,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(super::DescribeLogDirsHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let DescribeLogDirsOutcome::Described(batch) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("broker-scoped recovery result expected");
    };
    let (_throttle, outcomes) = batch.into_parts();
    let (recovered_broker, DescribeLogDirsEngineBrokerResult::OperationFailed(failure)) = outcomes
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("one broker outcome"))
        .into_parts()
    else {
        panic!("broker-scoped recovery failure expected");
    };
    assert_eq!(recovered_broker, 9);
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            DescribeLogDirsBrokerFailureKind::Transport,
            DescribeLogDirsDeliveryStatus::PossiblySent,
        )
    );

    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn mismatched_selection_order_remains_owned_and_blocks_recovery() {
    let (mut host, mut notifier) = host();
    let expected = vec![partition("orders", 0), partition("audit", 1)];
    let admission = host
        .try_admit(
            Moment::from_tick(1),
            deadline(),
            AdminDescribeLogDirsPlan::selected(vec![9], expected)
                .unwrap_or_else(|error| panic!("plan: {error}")),
        )
        .unwrap_or_else(|error| panic!("admit: {error:?}"));
    let DescribeLogDirsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, broker_id, _, request_limit, result_limit) =
        submission.into_parts();
    let mismatched = AdminDescribeLogDirsSelection::Selected(vec![
        partition("audit", 1),
        partition("orders", 0),
    ]);
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = DescribeLogDirsCall::submit(
        &driver,
        broker_id,
        mismatched,
        request_limit,
        result_limit,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted mismatched call"));

    assert_eq!(
        host.accept_call(operation_id, call),
        Err(super::DescribeLogDirsHostError::SubmissionMismatch)
    );
    drop(driver);
    assert_eq!(
        host.recover_after_driver_shutdown(),
        Err(super::DescribeLogDirsHostError::SubmissionMismatch)
    );
    assert_eq!(host.unsettled(), 1);

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn successful_first_route_installs_the_next_exact_broker_with_reduced_result_capacity() {
    let (mut host, mut notifier) = host();
    let admission = host
        .try_admit(
            Moment::from_tick(1),
            deadline(),
            AdminDescribeLogDirsPlan::new(vec![9, 2])
                .unwrap_or_else(|error| panic!("plan: {error}")),
        )
        .unwrap_or_else(|error| panic!("admit: {error:?}"));
    let DescribeLogDirsTurn::Submit(first) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("first turn: {error}"))
    else {
        panic!("first submission expected");
    };
    let (_, _, first_broker, first_selection, request_limit, first_result_limit) =
        first.into_parts();
    assert_eq!(first_broker, 9);
    host.settle_matching_raw_for_test()
        .unwrap_or_else(|error| panic!("settle first broker: {error}"));

    let DescribeLogDirsTurn::Submit(second) = host
        .turn(Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("second turn: {error}"))
    else {
        panic!("second submission expected");
    };
    let (_, _, second_broker, second_selection, second_request_limit, second_result_limit) =
        second.into_parts();
    assert_eq!(second_broker, 2);
    assert_eq!(second_selection, first_selection);
    assert_eq!(second_request_limit, request_limit);
    assert!(second_result_limit < first_result_limit);

    drop((admission, host));
    stop_notifier(&mut notifier);
}

fn host() -> (DescribeLogDirsHost, AdminCompletionNotifier) {
    let (notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    (DescribeLogDirsHost::new(ports.describe_log_dirs), notifier)
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(20),
        Instant::now() + Duration::from_secs(1),
    )
}

fn partition(topic: &str, partition: i32) -> AdminDescribeLogDirsPartition {
    AdminDescribeLogDirsPartition::new(topic.to_owned(), partition)
}

fn stop_notifier(notifier: &mut AdminCompletionNotifier) {
    notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}
