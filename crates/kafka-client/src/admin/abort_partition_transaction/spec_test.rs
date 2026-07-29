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
    assert_eq!(spec.requested_transaction_version(), 0);
}

#[test]
fn specification_preserves_explicit_transaction_version_without_early_validation() {
    let spec = AbortTransactionSpec::new(TopicPartition::new("orders", 3), 41, 7, 11)
        .transaction_version(-1);

    assert_eq!(spec.requested_transaction_version(), -1);
}
