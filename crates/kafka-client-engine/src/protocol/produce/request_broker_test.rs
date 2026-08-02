//! Broker-routed Produce assembly order and partition grouping evidence.

#![allow(clippy::unwrap_used)]

use std::time::Instant;

use bytes::Bytes;
use kafka_client_core::{Deadline, Moment};

use crate::clock::OperationDeadline;

use super::MaterializedProduce;

const TOPIC: &str = "orders";

#[test]
fn broker_group_request_contains_one_distinct_entry_per_partition() {
    let batches = vec![
        MaterializedProduce::from_broker_routed_test_parts(
            TOPIC,
            0,
            7,
            Bytes::from_static(b"first"),
        ),
        MaterializedProduce::from_broker_routed_test_parts(
            TOPIC,
            1,
            7,
            Bytes::from_static(b"second"),
        ),
    ];
    let request = MaterializedProduce::into_broker_routed_request(
        batches,
        Moment::from_tick(10),
        operation_deadline(30_000_000_010),
    )
    .unwrap_or_else(|_| panic!("bounded broker request assembly"));

    assert_eq!(request.topic_data.len(), 1);
    assert_eq!(request.topic_data[0].name.as_str(), TOPIC);
    assert_eq!(
        request.topic_data[0]
            .partition_data
            .iter()
            .map(|partition| partition.index)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
}

#[test]
fn broker_group_request_preserves_first_seen_order_across_many_topics() {
    let batches = (0_i32..64)
        .map(|index| {
            MaterializedProduce::from_broker_routed_test_parts(
                format!("topic-{index:02}"),
                index,
                7,
                Bytes::from_static(b"encoded"),
            )
        })
        .collect();
    let request = MaterializedProduce::into_broker_routed_request(
        batches,
        Moment::from_tick(10),
        operation_deadline(30_000_000_010),
    )
    .unwrap_or_else(|_| panic!("bounded broker request assembly"));

    assert_eq!(request.topic_data.len(), 64);
    for (index, topic) in request.topic_data.iter().enumerate() {
        assert_eq!(topic.name.as_str(), format!("topic-{index:02}"));
        assert_eq!(topic.partition_data.len(), 1);
        assert_eq!(topic.partition_data[0].index, i32::try_from(index).unwrap());
    }
}

fn operation_deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(tick), Instant::now())
}
