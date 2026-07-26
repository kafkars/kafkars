//! Recovered call ownership remains retained across rejected core settlement.

use kafka_client_core::Moment;

use super::super::{
    AlterConsumerGroupOffsetsHostError, AlterConsumerGroupOffsetsTurn,
    host_test::{deadline, plan},
};

#[test]
fn recovered_call_remains_retained_when_core_rejects_terminal_fact() {
    let (mut host, notifier) = crate::admin::test_support::alter_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit offset alteration: {error:?}"));
    let AlterConsumerGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (_id, _deadline, plan, request_scratch_limit, result_limit) = submission.into_parts();
    host.retain_recovered_call_for_test(plan.clone(), request_scratch_limit, result_limit);

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(AlterConsumerGroupOffsetsHostError::Machine(
            kafka_client_core::AlterConsumerGroupOffsetsMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_call_matches_for_test(&plan, request_scratch_limit, result_limit));

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn recovered_call_blocks_terminal_publication_until_settlement() {
    let (mut host, notifier) = crate::admin::test_support::alter_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit offset alteration: {error:?}"));
    let AlterConsumerGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (_id, _deadline, plan, request_scratch_limit, result_limit) = submission.into_parts();
    host.retain_recovered_call_for_test(plan.clone(), request_scratch_limit, result_limit);

    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AlterConsumerGroupOffsetsHostError::InvalidHandoff)
    ));
    assert!(host.recovered_call_matches_for_test(&plan, request_scratch_limit, result_limit));

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}
