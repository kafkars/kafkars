//! Scenarios for exact consumer-group member-removal result values.

use core::num::NonZeroI16;

use super::{
    ConsumerGroupMemberRemovalBrokerError, ConsumerGroupMemberRemovalOutcome,
    ConsumerGroupMemberRemovalResult, RemoveConsumerGroupMembersBatch,
};

#[test]
fn member_failure_retains_identity_and_exact_signed_code() {
    let code = NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("code is nonzero"));
    let outcome = ConsumerGroupMemberRemovalOutcome::failed(
        "instance-a".to_owned(),
        ConsumerGroupMemberRemovalBrokerError::new(code),
    );

    assert_eq!(outcome.group_instance_id(), "instance-a");
    let ConsumerGroupMemberRemovalResult::Failed(error) = outcome.result() else {
        panic!("member must retain its broker failure");
    };
    assert_eq!(error.code(), -31_999);
}

#[test]
fn response_batch_retains_throttle_and_caller_order() {
    let batch = RemoveConsumerGroupMembersBatch::new(
        73,
        vec![
            ConsumerGroupMemberRemovalOutcome::removed("instance-b".to_owned()),
            ConsumerGroupMemberRemovalOutcome::removed("instance-a".to_owned()),
        ],
    );

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.outcomes()[0].group_instance_id(), "instance-b");
    let (throttle_time_ms, outcomes) = batch.into_parts();
    assert_eq!(throttle_time_ms, 73);
    assert_eq!(outcomes.len(), 2);
}
