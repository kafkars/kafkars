//! Public caller-ordered DescribeProducers result scenarios.

use std::time::Duration;

use crate::TopicPartition;

use super::{super::BatchResult, DescribeProducersResult, ProducerState};

#[test]
fn result_preserves_maximum_throttle_and_caller_partition_order() {
    let states = vec![ProducerState::new(3, 1, -1, -1, 0, None)];
    let result = DescribeProducersResult::new(
        Duration::from_millis(17),
        BatchResult::new(vec![
            (TopicPartition::new("orders", 2), Ok(states.clone())),
            (TopicPartition::new("audit", 0), Ok(Vec::new())),
        ]),
    );

    assert_eq!(result.throttle_time(), Duration::from_millis(17));
    assert_eq!(result.partitions().entries()[0].0.topic(), "orders");
    assert_eq!(result.partitions().entries()[1].0.topic(), "audit");

    let entries = result.into_partitions().into_entries();
    assert_eq!(entries[0].1.as_ref(), Ok(&states));
    assert!(
        entries[1]
            .1
            .as_ref()
            .expect("empty partition success")
            .is_empty()
    );
}
