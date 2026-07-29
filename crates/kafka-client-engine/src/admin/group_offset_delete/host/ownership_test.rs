//! Exact rejection, accepted-call, and recovered-call correlation scenarios.

use kafka_client_core::{DeleteConsumerGroupOffsetTarget, DeleteConsumerGroupOffsetsPlan, Moment};

use crate::{
    EngineConfig,
    driver::{DriverOwner, GroupOffsetDeleteCall},
};

use super::super::{
    DeleteConsumerGroupOffsetsHostError, DeleteConsumerGroupOffsetsTurn,
    host_test::{deadline, plan},
};

#[test]
fn mismatched_rejection_evidence_blocks_settlement() {
    let (mut host, notifier) = crate::admin::test_support::delete_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit deletion: {error:?}"));
    let DeleteConsumerGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, submitted_plan, result_limit) = submission.into_parts();

    assert!(matches!(
        host.reject_handoff(operation_id, other_plan(), result_limit),
        Err(DeleteConsumerGroupOffsetsHostError::SubmissionMismatch)
    ));
    host.reject_handoff(operation_id, submitted_plan, result_limit)
        .unwrap_or_else(|error| panic!("exact rejection: {error}"));

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn mismatched_accepted_call_remains_owned_and_blocks_recovery() {
    let (mut host, notifier) = crate::admin::test_support::delete_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit deletion: {error:?}"));
    let DeleteConsumerGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, _submitted_plan, result_limit) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = GroupOffsetDeleteCall::submit(
        &driver,
        other_plan(),
        result_limit,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));

    assert!(matches!(
        host.accept_call(operation_id, call),
        Err(DeleteConsumerGroupOffsetsHostError::SubmissionMismatch)
    ));
    assert!(host.call_ownership_is_retained_for_test());
    drop(driver);
    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(DeleteConsumerGroupOffsetsHostError::SubmissionMismatch)
    ));
    assert!(host.recovered_call_is_retained_for_test());

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn mismatched_recovered_evidence_blocks_terminal_settlement() {
    let (mut host, notifier) = crate::admin::test_support::delete_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit deletion: {error:?}"));
    host.retain_mismatched_recovered_call_for_test();

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(DeleteConsumerGroupOffsetsHostError::SubmissionMismatch)
    ));
    assert!(host.recovered_call_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(DeleteConsumerGroupOffsetsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

fn other_plan() -> DeleteConsumerGroupOffsetsPlan {
    DeleteConsumerGroupOffsetsPlan::new(
        "other-group".to_owned(),
        vec![DeleteConsumerGroupOffsetTarget::new(
            "other-topic".to_owned(),
            7,
        )],
    )
    .unwrap_or_else(|error| panic!("other deletion plan: {error}"))
}
