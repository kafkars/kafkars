//! Multi-Streams-group result conversion scenarios.

use std::time::Duration;

use crate::admin::{BatchResult, ListConsumerGroupOffsetsResult, ListConsumerGroupsOffsetsResult};

use super::ListStreamsGroupsOffsetsResult;

#[test]
fn conversion_preserves_group_order_and_aggregate_throttle() {
    let consumer = ListConsumerGroupsOffsetsResult::new(
        Duration::from_millis(17),
        BatchResult::new(vec![(
            "streams-orders".to_owned(),
            Ok(ListConsumerGroupOffsetsResult::new(
                Duration::from_millis(11),
                BatchResult::new(Vec::new()),
            )),
        )]),
    );
    let streams = ListStreamsGroupsOffsetsResult::from_consumer_groups(consumer);
    assert_eq!(streams.throttle_time(), Duration::from_millis(17));
    assert_eq!(streams.groups().entries()[0].0, "streams-orders");
}
