//! Synchronous handoff correlation and definitely-unsent settlement scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{
    AdminGroupListingFilters, AdminGroupListingScope, AdminListConsumerGroupsInput, Moment,
};

use crate::clock::OperationDeadline;

use super::{
    ListConsumerGroupsHost, ListConsumerGroupsHostError, ListConsumerGroupsSubmissionKind,
    ListConsumerGroupsTurn,
};
use crate::admin::list_consumer_groups::{
    ListConsumerGroupsDeliveryStatus, ListConsumerGroupsFailureKind, ListConsumerGroupsOutcome,
};

#[test]
fn synchronous_discovery_rejection_preserves_not_sent_settlement() {
    let (mut notifier, ports) = crate::admin::test_support::completion_owner();
    let mut host = ListConsumerGroupsHost::new(ports.list_consumer_groups);
    let admission = admit(&mut host, AdminGroupListingFilters::empty());
    let ListConsumerGroupsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take discovery: {error}"))
    else {
        panic!("discovery submission expected");
    };
    let (operation_id, _deadline, kind) = submission.into_parts();

    host.reject_handoff(operation_id, kind)
        .unwrap_or_else(|error| panic!("reject discovery: {error}"));
    let ListConsumerGroupsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe rejection: {error}"))
    else {
        panic!("driver rejection expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            ListConsumerGroupsFailureKind::DriverRejected,
            ListConsumerGroupsDeliveryStatus::NotSent,
        )
    );

    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn mismatched_broker_filters_retain_rejection_evidence_and_block_publication() {
    let (mut notifier, ports) = crate::admin::test_support::completion_owner();
    let mut host = ListConsumerGroupsHost::new(ports.list_consumer_groups);
    let admission = admit(&mut host, filters());
    let ListConsumerGroupsTurn::Submit(discovery) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take discovery: {error}"))
    else {
        panic!("discovery submission expected");
    };
    let (operation_id, _deadline, _kind) = discovery.into_parts();
    host.apply_input_for_test(operation_id, AdminListConsumerGroupsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept discovery: {error}"));
    host.apply_input_for_test(
        operation_id,
        AdminListConsumerGroupsInput::BrokersDiscovered {
            broker_ids: vec![7],
        },
    )
    .unwrap_or_else(|error| panic!("install broker attempt: {error}"));
    let ListConsumerGroupsTurn::Submit(broker) = host
        .turn(Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("take broker: {error}"))
    else {
        panic!("broker submission expected");
    };
    let (operation_id, _deadline, kind) = broker.into_parts();
    let ListConsumerGroupsSubmissionKind::Broker {
        broker_id,
        retained_limit,
        ..
    } = kind
    else {
        panic!("exact broker submission expected");
    };
    let mismatch = ListConsumerGroupsSubmissionKind::Broker {
        broker_id,
        filters: AdminGroupListingFilters::empty(),
        retained_limit,
    };

    assert!(matches!(
        host.reject_handoff(operation_id, mismatch),
        Err(ListConsumerGroupsHostError::SubmissionMismatch)
    ));
    assert!(host.rejected_submission_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(ListConsumerGroupsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

fn admit(
    host: &mut ListConsumerGroupsHost,
    filters: AdminGroupListingFilters,
) -> super::ListConsumerGroupsAdmission {
    host.try_admit(
        Moment::from_tick(1),
        OperationDeadline::from_parts_for_test(
            kafka_client_core::Deadline::from_tick(10),
            Instant::now() + Duration::from_secs(1),
        ),
        AdminGroupListingScope::ConsumerOnly,
        filters,
    )
    .unwrap_or_else(|error| panic!("admit listing: {error:?}"))
}

fn filters() -> AdminGroupListingFilters {
    AdminGroupListingFilters::new(
        vec!["Stable".to_owned()],
        vec!["consumer".to_owned()],
        vec!["consumer".to_owned()],
    )
    .unwrap_or_else(|error| panic!("valid filters: {error}"))
}

fn stop_notifier(notifier: &mut crate::admin::AdminCompletionNotifier) {
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"));
    assert_eq!(join.join_off_notifier(), Ok(()));
}
