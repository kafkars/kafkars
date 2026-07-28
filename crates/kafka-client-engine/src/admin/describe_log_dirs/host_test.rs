//! Admission, sequential submission, and accepted-call ownership scenarios.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use kafka_client_core::{AdminDescribeLogDirsMachineError, AdminDescribeLogDirsPlan, Moment};

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
    let (_, _, broker_id) = submission.into_parts();
    assert_eq!(broker_id, 9);
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
    let (operation_id, submitted_deadline, broker_id, selection, retained_limit) =
        submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = DescribeLogDirsCall::submit(
        &driver,
        broker_id,
        &selection,
        retained_limit,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, selection, call)
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

fn stop_notifier(notifier: &mut AdminCompletionNotifier) {
    notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}
