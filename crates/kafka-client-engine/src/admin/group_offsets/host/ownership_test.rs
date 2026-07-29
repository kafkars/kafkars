//! Exact rejection, accepted-call, and recovered-call correlation scenarios.

use kafka_client_core::{ListConsumerGroupOffsetsPlan, Moment};

use crate::{
    EngineConfig,
    driver::{DriverOwner, GroupOffsetsCall},
};

use super::super::{
    ListConsumerGroupOffsetsHostError, ListConsumerGroupOffsetsTurn, host_test::deadline,
};

#[test]
fn mismatched_rejection_evidence_remains_owned_and_blocks_publication() {
    let (mut host, notifier) = crate::admin::test_support::list_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan("readers", true))
        .unwrap_or_else(|error| panic!("admit offsets: {error:?}"));
    let ListConsumerGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, _submitted_plan, result_limit) = submission.into_parts();

    assert!(matches!(
        host.reject_handoff(operation_id, plan("other", true), result_limit),
        Err(ListConsumerGroupOffsetsHostError::SubmissionMismatch)
    ));
    assert!(host.rejected_submission_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(ListConsumerGroupOffsetsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn mismatched_accepted_call_remains_owned_and_blocks_recovery() {
    let (mut host, notifier) = crate::admin::test_support::list_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan("readers", true))
        .unwrap_or_else(|error| panic!("admit offsets: {error:?}"));
    let ListConsumerGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, _submitted_plan, result_limit) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = GroupOffsetsCall::submit(
        &driver,
        plan("other", false),
        result_limit,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));

    assert!(matches!(
        host.accept_call(operation_id, call),
        Err(ListConsumerGroupOffsetsHostError::SubmissionMismatch)
    ));
    assert!(host.call_is_retained_for_test());
    drop(driver);
    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(ListConsumerGroupOffsetsHostError::SubmissionMismatch)
    ));
    assert!(host.recovered_call_is_retained_for_test());

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn mismatched_recovered_evidence_blocks_terminal_settlement() {
    let (mut host, notifier) = crate::admin::test_support::list_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan("readers", true))
        .unwrap_or_else(|error| panic!("admit offsets: {error:?}"));
    host.retain_mismatched_recovered_call_for_test();

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(ListConsumerGroupOffsetsHostError::SubmissionMismatch)
    ));
    assert!(host.recovered_call_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(ListConsumerGroupOffsetsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

fn plan(group: &str, require_stable: bool) -> ListConsumerGroupOffsetsPlan {
    ListConsumerGroupOffsetsPlan::new(group.to_owned(), require_stable)
        .unwrap_or_else(|error| panic!("valid plan: {error}"))
}
