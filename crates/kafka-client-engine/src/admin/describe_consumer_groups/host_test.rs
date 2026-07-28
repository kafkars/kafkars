//! Completion-error ownership remains installed across both group-description calls.

use std::time::{Duration, Instant};

use kafka_client_core::{AdminDescribeConsumerGroupsPlan, Deadline, Moment};

use crate::{
    EngineConfig,
    admin::{AdminCompletionNotifier, DescribeConsumerGroupsHost},
    clock::OperationDeadline,
    driver::{DescribeConsumerGroupsCall, DriverOwner},
};

use super::{
    ConsumerGroupDescriptionError, DescribeConsumerGroupsDeliveryStatus,
    DescribeConsumerGroupsFailureKind, DescribeConsumerGroupsHostError,
    DescribeConsumerGroupsOutcome, DescribeConsumerGroupsTurn,
};

#[test]
fn modern_completion_fault_recovers_after_driver_shutdown() {
    let (mut host, notifier) = host();
    let deadline = deadline();
    let plan = AdminDescribeConsumerGroupsPlan::new(vec!["workers".to_owned()], false)
        .unwrap_or_else(|error| panic!("plan: {error}"));
    let admission = host
        .try_admit(Moment::from_tick(1), deadline, plan)
        .unwrap_or_else(|error| panic!("admission: {error:?}"));
    let DescribeConsumerGroupsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, group_id, authorized, call_kind, scratch, result_limit) =
        submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = DescribeConsumerGroupsCall::submit(
        &driver,
        call_kind,
        group_id,
        authorized,
        scratch,
        result_limit,
        submitted_deadline,
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(DescribeConsumerGroupsHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    assert_transport_failure(
        admission
            .observer
            .wait()
            .unwrap_or_else(|error| panic!("recovery observation: {error}")),
    );

    drop(host);
    stop_notifier(notifier);
}

fn assert_transport_failure(outcome: DescribeConsumerGroupsOutcome) {
    let DescribeConsumerGroupsOutcome::Groups(batch) = outcome else {
        panic!("caller-correlated group result expected");
    };
    let (_throttle, groups) = batch.into_parts();
    let (_group_id, result) = groups
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("group result expected"))
        .into_parts();
    let Err(ConsumerGroupDescriptionError::Operation(failure)) = result else {
        panic!("operation failure expected");
    };
    assert_eq!(failure.kind(), DescribeConsumerGroupsFailureKind::Transport);
    assert_eq!(
        failure.delivery(),
        DescribeConsumerGroupsDeliveryStatus::PossiblySent
    );
}

fn host() -> (DescribeConsumerGroupsHost, AdminCompletionNotifier) {
    let (notifier, ports) = AdminCompletionNotifier::start()
        .unwrap_or_else(|error| panic!("start shared admin notifier: {error}"));
    (
        DescribeConsumerGroupsHost::new(ports.describe_consumer_groups),
        notifier,
    )
}

fn stop_notifier(mut notifier: AdminCompletionNotifier) {
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop admin notifier: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("join admin notifier: {error}"));
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        Deadline::from_tick(10),
        Instant::now() + Duration::from_secs(1),
    )
}
