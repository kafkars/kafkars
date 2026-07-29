//! Missing-call, completion-fault, and mismatched-correlation recovery scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{AdminGroupListingFilters, AdminGroupListingScope, Moment};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::{DriverOwner, ListConsumerGroupsCall},
};

use super::super::super::{
    ListConsumerGroupsDeliveryStatus, ListConsumerGroupsFailureKind, ListConsumerGroupsHost,
    ListConsumerGroupsHostError, ListConsumerGroupsOutcome, ListConsumerGroupsTurn,
};

#[test]
fn handed_off_without_returned_call_cannot_forge_recovery_ownership() {
    let (mut notifier, ports) = crate::admin::test_support::completion_owner();
    let mut host = ListConsumerGroupsHost::new(ports.list_consumer_groups);
    let admission = admit(&mut host);
    let ListConsumerGroupsTurn::Submit(_submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take discovery: {error}"))
    else {
        panic!("discovery submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(ListConsumerGroupsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_discovery_until_post_driver_settlement() {
    let (mut notifier, ports) = crate::admin::test_support::completion_owner();
    let mut host = ListConsumerGroupsHost::new(ports.list_consumer_groups);
    let admission = admit(&mut host);
    let ListConsumerGroupsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take discovery: {error}"))
    else {
        panic!("discovery submission expected");
    };
    let (operation_id, deadline, _kind) = submission.into_parts();
    let driver = driver();
    let call = ListConsumerGroupsCall::submit_discovery(&driver, deadline.transport())
        .unwrap_or_else(|_error| panic!("accepted discovery"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("retain accepted call: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(ListConsumerGroupsHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover discovery: {error}"));
    let ListConsumerGroupsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("transport failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            ListConsumerGroupsFailureKind::Transport,
            ListConsumerGroupsDeliveryStatus::PossiblySent,
        )
    );

    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn mismatched_recovered_attempt_blocks_settlement_and_publication() {
    let (mut notifier, ports) = crate::admin::test_support::completion_owner();
    let mut host = ListConsumerGroupsHost::new(ports.list_consumer_groups);
    let admission = admit(&mut host);
    let ListConsumerGroupsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take discovery: {error}"))
    else {
        panic!("discovery submission expected");
    };
    let (operation_id, _deadline, _kind) = submission.into_parts();
    let driver = driver();
    let call = ListConsumerGroupsCall::submit_broker(
        &driver,
        7,
        AdminGroupListingFilters::empty(),
        4_096,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|_error| panic!("accepted mismatched broker call"));
    assert!(matches!(
        host.accept_call(operation_id, call),
        Err(ListConsumerGroupsHostError::SubmissionMismatch)
    ));
    drop(driver);

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(ListConsumerGroupsHostError::SubmissionMismatch)
    ));
    assert!(host.recovered_call_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(ListConsumerGroupsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

fn admit(host: &mut ListConsumerGroupsHost) -> super::super::ListConsumerGroupsAdmission {
    host.try_admit(
        Moment::from_tick(1),
        OperationDeadline::from_parts_for_test(
            kafka_client_core::Deadline::from_tick(10),
            Instant::now() + Duration::from_secs(1),
        ),
        AdminGroupListingScope::ConsumerOnly,
        AdminGroupListingFilters::empty(),
    )
    .unwrap_or_else(|error| panic!("admit listing: {error:?}"))
}

fn driver() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"))
}

fn stop_notifier(notifier: &mut crate::admin::AdminCompletionNotifier) {
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"));
    assert_eq!(join.join_off_notifier(), Ok(()));
}
