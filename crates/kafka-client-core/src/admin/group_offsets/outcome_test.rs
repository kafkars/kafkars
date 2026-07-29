//! Scenarios for exact group-offset result representations.

use core::num::NonZeroI16;

use super::outcome::{ListConsumerGroupBatchOutcome, ListConsumerGroupsOffsetsBatch};
use super::{
    GroupOffsetBrokerError, GroupOffsetDescription, GroupOffsetOutcome, GroupOffsetResult,
    ListConsumerGroupOffsetsBatch,
};

#[test]
fn committed_offset_preserves_missing_epoch_and_nullable_metadata() {
    let missing = GroupOffsetDescription::new(None, None, None);
    assert_eq!(missing.offset(), None);
    assert_eq!(missing.leader_epoch(), None);
    assert_eq!(missing.metadata(), None);

    let explicit_empty = GroupOffsetDescription::new(Some(41), Some(7), Some(String::new()));
    assert_eq!(explicit_empty.offset(), Some(41));
    assert_eq!(explicit_empty.leader_epoch(), Some(7));
    assert_eq!(explicit_empty.metadata(), Some(""));
    assert_eq!(
        explicit_empty.into_parts(),
        (Some(41), Some(7), Some(String::new()))
    );
}

#[test]
fn response_batch_retains_throttle_and_order_without_reclassification() {
    let batch = ListConsumerGroupOffsetsBatch::new(
        73,
        vec![GroupOffsetOutcome::described(
            "audit".to_owned(),
            0,
            GroupOffsetDescription::new(Some(9), None, None),
        )],
    );

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.outcomes()[0].topic(), "audit");
    let (throttle_time_ms, outcomes) = batch.into_parts();
    assert_eq!(throttle_time_ms, 73);
    assert_eq!(outcomes.len(), 1);
}

#[test]
fn partition_failure_retains_identity_and_exact_signed_code() {
    let code = NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("code is nonzero"));
    let outcome =
        GroupOffsetOutcome::failed("audit".to_owned(), 3, GroupOffsetBrokerError::new(code));

    assert_eq!(outcome.topic(), "audit");
    assert_eq!(outcome.partition(), 3);
    let GroupOffsetResult::Failed(error) = outcome.result() else {
        panic!("partition must retain its broker failure");
    };
    assert_eq!(error.code(), -31_999);
}

#[test]
fn multi_group_batch_retains_caller_order_and_maximum_throttle() {
    assert!(
        core::mem::size_of::<ListConsumerGroupBatchOutcome>() <= 8 * core::mem::size_of::<usize>()
    );
    let batch = ListConsumerGroupsOffsetsBatch::new(
        83,
        vec![
            ListConsumerGroupBatchOutcome::broker_rejected(
                "z-readers".to_owned(),
                NonZeroI16::new(-719).unwrap_or_else(|| panic!("nonzero")),
                83,
            ),
            ListConsumerGroupBatchOutcome::offsets(
                "a-readers".to_owned(),
                ListConsumerGroupOffsetsBatch::new(7, Vec::new()),
            ),
        ],
    );

    assert_eq!(batch.throttle_time_ms(), 83);
    assert_eq!(batch.outcomes()[0].group_id(), "z-readers");
    assert_eq!(batch.outcomes()[1].group_id(), "a-readers");
    let (throttle_time_ms, outcomes) = batch.into_parts();
    assert_eq!(throttle_time_ms, 83);
    assert_eq!(outcomes.len(), 2);
}
