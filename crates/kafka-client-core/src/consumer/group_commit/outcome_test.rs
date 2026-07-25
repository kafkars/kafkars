//! Exact partition-level broker-code evidence.

use core::num::NonZeroI16;

use crate::{
    GroupOffsetCommitBatch, GroupOffsetCommitBrokerError, GroupOffsetCommitPartitionOutcome,
    GroupOffsetCommitPartitionResult, PartitionIndex, TopicId,
};

#[test]
fn signed_partition_broker_code_remains_exact_without_invented_diagnostics() {
    let code = NonZeroI16::new(-32_123).unwrap_or_else(|| panic!("nonzero code"));
    let outcome = GroupOffsetCommitPartitionOutcome::rejected(
        TopicId::from_raw(11),
        PartitionIndex::from_raw(4),
        GroupOffsetCommitBrokerError::new(code),
    );

    let GroupOffsetCommitPartitionResult::Rejected(error) = outcome.result() else {
        panic!("rejected partition");
    };
    assert_eq!(error.code(), -32_123);
}

#[test]
fn response_batch_retains_nonnegative_throttle_without_scheduling_policy() {
    let batch = GroupOffsetCommitBatch::new(
        u32::MAX,
        vec![GroupOffsetCommitPartitionOutcome::committed(
            TopicId::from_raw(11),
            PartitionIndex::from_raw(4),
        )],
    );
    assert_eq!(batch.throttle_time_ms(), u32::MAX);
    assert_eq!(batch.outcomes().len(), 1);
}
