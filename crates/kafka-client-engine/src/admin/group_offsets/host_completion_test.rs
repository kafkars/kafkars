//! Accepted-call completion and post-driver recovery ownership scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{
    ListConsumerGroupOffsetsMachineError, ListConsumerGroupOffsetsPlan, Moment,
};

use crate::clock::OperationDeadline;

use super::{
    ListConsumerGroupOffsetsDeliveryStatus, ListConsumerGroupOffsetsFailureKind,
    ListConsumerGroupOffsetsHostError, ListConsumerGroupOffsetsOutcome,
    ListConsumerGroupOffsetsTurn,
};

#[test]
fn handed_off_without_a_returned_call_cannot_forge_recovery_evidence() {
    let (mut host, notifier) = crate::admin::test_support::list_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(), plan())
        .unwrap_or_else(|error| panic!("admit group offsets: {error:?}"));
    let ListConsumerGroupOffsetsTurn::Submit(_submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(ListConsumerGroupOffsetsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn recovered_call_remains_retained_when_core_rejects_terminal_fact() {
    let (mut host, notifier) = crate::admin::test_support::list_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(), plan())
        .unwrap_or_else(|error| panic!("admit group offsets: {error:?}"));
    host.retain_recovered_call_for_test();

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(ListConsumerGroupOffsetsHostError::Machine(
            ListConsumerGroupOffsetsMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_call_is_retained_for_test());

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn completion_fault_retains_call_until_post_driver_recovery() {
    let (mut host, notifier) = crate::admin::test_support::list_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(), plan())
        .unwrap_or_else(|error| panic!("admit group offsets: {error:?}"));
    let ListConsumerGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _submitted_deadline, _submitted_plan, _result_limit) =
        submission.into_parts();
    let driver = host.install_live_call_for_test(operation_id);
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(ListConsumerGroupOffsetsHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let ListConsumerGroupOffsetsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("recovery observation: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            ListConsumerGroupOffsetsFailureKind::Transport,
            ListConsumerGroupOffsetsDeliveryStatus::PossiblySent,
        )
    );

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

fn plan() -> ListConsumerGroupOffsetsPlan {
    ListConsumerGroupOffsetsPlan::new("payments".to_owned(), true)
        .unwrap_or_else(|error| panic!("valid group-offset plan: {error}"))
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(20),
        Instant::now() + Duration::from_secs(1),
    )
}
