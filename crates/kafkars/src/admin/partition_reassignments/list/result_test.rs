//! Deterministic public reassignment result tests.

use std::time::Duration;

use crate::TopicPartition;

use super::{ListPartitionReassignmentsResult, PartitionReassignment};

#[test]
fn result_preserves_throttle_and_row_order() {
    let result = ListPartitionReassignmentsResult::new(
        Duration::from_millis(9),
        vec![(
            TopicPartition::new("z", 2),
            PartitionReassignment::new(vec![1], vec![], vec![]),
        )],
    );
    assert_eq!(result.throttle_time(), Duration::from_millis(9));
    assert_eq!(result.reassignments()[0].0.topic(), "z");
}
