//! Atomic missing-offset and partition-rejection terminal ownership.

use core::num::NonZeroI16;

use crate::{GroupAssignmentPartition, PartitionIndex, TopicId};

use super::{
    GroupPositionBatch, GroupPositionBootstrapMissingOffsets,
    GroupPositionBootstrapPartitionRejection, GroupPositionBrokerError, GroupPositionPartitionFact,
    GroupPositionPartitionResult,
};

#[test]
fn missing_terminal_retains_throttle_all_facts_and_first_missing() {
    let batch = GroupPositionBatch::new(
        37,
        vec![
            GroupPositionPartitionFact::missing(assigned(3, 0)),
            GroupPositionPartitionFact::missing(assigned(3, 2)),
        ],
    );
    let missing = GroupPositionBootstrapMissingOffsets::new(batch, 0);

    assert_eq!(missing.batch().throttle_time_ms(), 37);
    assert_eq!(missing.batch().facts().len(), 2);
    assert_eq!(
        missing.first_missing().result(),
        GroupPositionPartitionResult::Missing
    );
    assert_eq!(missing.into_batch().facts()[1].partition(), assigned(3, 2));
}

#[test]
fn partition_rejection_retains_exact_signed_code_and_complete_batch() {
    let error = GroupPositionBrokerError::new(nonzero(i16::MIN));
    let batch = GroupPositionBatch::new(
        43,
        vec![
            GroupPositionPartitionFact::missing(assigned(3, 0)),
            GroupPositionPartitionFact::rejected(assigned(3, 2), error),
        ],
    );
    let rejection = GroupPositionBootstrapPartitionRejection::new(batch, 1);

    let GroupPositionPartitionResult::Rejected(actual) = rejection.first_rejected().result() else {
        panic!("second fact must remain rejected");
    };
    assert_eq!(actual.code(), i16::MIN);
    assert_eq!(rejection.batch().throttle_time_ms(), 43);
    assert_eq!(rejection.into_batch().facts().len(), 2);
}

fn assigned(topic: u64, partition: u32) -> GroupAssignmentPartition {
    GroupAssignmentPartition::new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(partition),
    )
}

fn nonzero(value: i16) -> NonZeroI16 {
    NonZeroI16::new(value).unwrap_or_else(|| panic!("nonzero broker code"))
}
