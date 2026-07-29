//! Public partition-transaction abort specification scenarios.

use crate::{StartPosition, TopicPartition};

use super::AbortTransactionSpec;

#[test]
fn specification_preserves_every_signed_identity_without_early_validation() {
    let partition = TopicPartition::new("orders", -1).start_at(StartPosition::Beginning);
    let spec = AbortTransactionSpec::new(partition.clone(), -2, -3, -4);

    assert_eq!(spec.topic_partition(), &partition);
    assert_eq!(spec.producer_id(), -2);
    assert_eq!(spec.producer_epoch(), -3);
    assert_eq!(spec.coordinator_epoch(), -4);
}
