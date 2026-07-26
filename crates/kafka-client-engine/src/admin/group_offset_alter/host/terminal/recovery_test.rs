//! Raw caller-order correlation mismatch retention before core settlement.

use kafka_client_core::{AlterConsumerGroupOffsetsInput, AlterConsumerGroupOffsetsPlan, Moment};

use super::super::super::{
    AlterConsumerGroupOffsetsHostError, AlterConsumerGroupOffsetsTurn,
    host_test::{deadline, plan},
};

#[test]
fn reordered_raw_terminal_cannot_settle_core_or_publish() {
    let (mut host, notifier) = crate::admin::test_support::alter_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit alteration: {error:?}"));
    let AlterConsumerGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, plan, request_scratch_limit, result_limit) =
        submission.into_parts();
    host.apply_input_for_test(operation_id, AlterConsumerGroupOffsetsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept synthetic driver ownership: {error}"));
    host.retain_raw_terminal_for_test(reversed(plan), request_scratch_limit, result_limit);

    assert!(matches!(
        host.settle_raw_for_test(),
        Err(AlterConsumerGroupOffsetsHostError::SubmissionMismatch)
    ));
    assert!(host.raw_terminal_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AlterConsumerGroupOffsetsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
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
