//! Exact rejection, accepted-call, and recovered-call correlation scenarios.

use kafka_client_core::{AlterConsumerGroupOffsetsPlan, Moment};

use crate::{
    EngineConfig,
    driver::{DriverOwner, GroupOffsetAlterCall},
};

use super::super::{
    AlterConsumerGroupOffsetsDeliveryStatus, AlterConsumerGroupOffsetsFailureKind,
    AlterConsumerGroupOffsetsOutcome,
    host_test::{deadline, plan},
};
use super::{AlterConsumerGroupOffsetsHostError, AlterConsumerGroupOffsetsTurn};

#[test]
fn exact_synchronous_rejection_is_definitely_not_sent() {
    let (mut host, notifier) = crate::admin::test_support::alter_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit alteration: {error:?}"));
    let (operation_id, _deadline, plan, request_scratch_limit, result_limit) =
        take_submission(&mut host);

    host.reject_handoff(operation_id, plan, request_scratch_limit, result_limit)
        .unwrap_or_else(|error| panic!("reject exact submission: {error}"));
    let AlterConsumerGroupOffsetsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe rejection: {error}"))
    else {
        panic!("driver rejection expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            AlterConsumerGroupOffsetsFailureKind::DriverRejected,
            AlterConsumerGroupOffsetsDeliveryStatus::NotSent,
        )
    );

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn mismatched_plan_or_capacity_retains_rejection_and_blocks_publication() {
    for mismatch in [
        RejectionMismatch::Group,
        RejectionMismatch::Order,
        RejectionMismatch::RequestCapacity,
        RejectionMismatch::ResultCapacity,
    ] {
        assert_rejection_mismatch_is_retained(mismatch);
    }
}

#[test]
fn mismatched_accepted_call_survives_recovery_and_blocks_publication() {
    let (mut host, notifier) = crate::admin::test_support::alter_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit alteration: {error:?}"));
    let (operation_id, submitted_deadline, submitted_plan, request_scratch_limit, result_limit) =
        take_submission(&mut host);
    let mismatch = reversed(submitted_plan);
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = GroupOffsetAlterCall::submit(
        &driver,
        mismatch.clone(),
        request_scratch_limit,
        result_limit,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|error| panic!("accepted mismatched call: {error}"));

    assert!(matches!(
        host.accept_call(operation_id, call),
        Err(AlterConsumerGroupOffsetsHostError::SubmissionMismatch)
    ));
    assert!(host.call_matches_for_test(&mismatch, request_scratch_limit, result_limit));
    drop(driver);
    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(AlterConsumerGroupOffsetsHostError::SubmissionMismatch)
    ));
    assert!(host.recovered_call_matches_for_test(&mismatch, request_scratch_limit, result_limit));
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AlterConsumerGroupOffsetsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[derive(Clone, Copy)]
enum RejectionMismatch {
    Group,
    Order,
    RequestCapacity,
    ResultCapacity,
}

fn assert_rejection_mismatch_is_retained(mismatch: RejectionMismatch) {
    let (mut host, notifier) = crate::admin::test_support::alter_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit alteration: {error:?}"));
    let (operation_id, _deadline, plan, request_scratch_limit, result_limit) =
        take_submission(&mut host);
    let (plan, request_scratch_limit, result_limit) = match mismatch {
        RejectionMismatch::Group => (
            with_group(plan, "other-group"),
            request_scratch_limit,
            result_limit,
        ),
        RejectionMismatch::Order => (reversed(plan), request_scratch_limit, result_limit),
        RejectionMismatch::RequestCapacity => (plan, request_scratch_limit + 1, result_limit),
        RejectionMismatch::ResultCapacity => (plan, request_scratch_limit, result_limit - 1),
    };

    assert!(matches!(
        host.reject_handoff(operation_id, plan, request_scratch_limit, result_limit),
        Err(AlterConsumerGroupOffsetsHostError::SubmissionMismatch)
    ));
    assert!(host.rejected_submission_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AlterConsumerGroupOffsetsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

fn take_submission(
    host: &mut super::AlterConsumerGroupOffsetsHost,
) -> (
    kafka_client_core::OperationId,
    crate::clock::OperationDeadline,
    AlterConsumerGroupOffsetsPlan,
    usize,
    usize,
) {
    let AlterConsumerGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    submission.into_parts()
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "the mismatch fixture takes ownership of the source plan"
)]
fn with_group(plan: AlterConsumerGroupOffsetsPlan, group: &str) -> AlterConsumerGroupOffsetsPlan {
    AlterConsumerGroupOffsetsPlan::new(group.to_owned(), plan.targets().to_vec())
        .unwrap_or_else(|error| panic!("alternate group plan: {error}"))
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "the mismatch fixture takes ownership of the source plan"
)]
fn reversed(plan: AlterConsumerGroupOffsetsPlan) -> AlterConsumerGroupOffsetsPlan {
    let mut targets = plan.targets().to_vec();
    targets.reverse();
    AlterConsumerGroupOffsetsPlan::new(plan.group_id().to_owned(), targets)
        .unwrap_or_else(|error| panic!("reordered plan: {error}"))
}
