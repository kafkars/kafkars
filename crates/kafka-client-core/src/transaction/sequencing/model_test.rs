//! Unit evidence for closed transactional sequencing facts.

use crate::{PartitionIndex, TopicId};

use super::{TransactionPartition, TransactionSequenceMachineError};

#[test]
fn partition_preserves_exact_catalog_route() {
    let partition = TransactionPartition::new(TopicId::from_raw(41), PartitionIndex::from_raw(3));

    assert_eq!(partition.topic_id(), TopicId::from_raw(41));
    assert_eq!(partition.partition(), PartitionIndex::from_raw(3));
}

#[test]
fn zero_partition_capacity_is_rejected() {
    let Err(error) = super::TransactionSequenceMachine::try_new(0) else {
        panic!("zero capacity unexpectedly created a sequence machine");
    };

    assert_eq!(error, TransactionSequenceMachineError::ZeroCapacity,);
}
