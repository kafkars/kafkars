//! Deterministic public reassignment alteration result tests.

use std::time::Duration;

use crate::{TopicPartition, admin::BatchResult};

use super::AlterPartitionReassignmentsResult;

#[test]
fn result_preserves_throttle_and_caller_order() {
    let result = AlterPartitionReassignmentsResult::new(
        Duration::from_millis(13),
        BatchResult::new(vec![(TopicPartition::new("orders", 2), Ok(()))]),
    );
    assert_eq!(result.throttle_time(), Duration::from_millis(13));
    assert_eq!(result.partitions().entries()[0].0.topic(), "orders");
    assert_eq!(result.into_partitions().into_entries()[0].0.partition(), 2);
}
