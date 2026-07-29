//! Public-to-engine partition-transaction abort translation scenarios.

use crate::{StartPosition, TopicPartition, admin::AbortTransactionSpec};

use super::AbortPartitionTransactionAdminRequest;

#[test]
fn translation_preserves_the_complete_broker_issued_identity() {
    let request = AbortPartitionTransactionAdminRequest::new(
        AbortTransactionSpec::new(TopicPartition::new("orders", 3), 41, 7, 11)
            .transaction_version(2),
    );

    let engine = format!("{:?}", request.into_engine());
    for expected in [
        "orders",
        "partition: 3",
        "producer_id: 41",
        "producer_epoch: 7",
        "coordinator_epoch: 11",
        "transaction_version: 2",
    ] {
        assert!(engine.contains(expected), "missing {expected} in {engine}");
    }
}

#[test]
fn assignment_only_start_position_is_preserved_as_invalid_input() {
    let request = AbortPartitionTransactionAdminRequest::new(AbortTransactionSpec::new(
        TopicPartition::new("orders", 3).start_at(StartPosition::End),
        41,
        7,
        11,
    ));

    let engine = format!("{:?}", request.into_engine());
    assert!(engine.contains(&i32::MIN.to_string()));
}
