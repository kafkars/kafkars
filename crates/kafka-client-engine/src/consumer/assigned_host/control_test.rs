//! Public position-control target validation and accessor scenarios.

use super::{AssignedConsumerPartition, AssignedConsumerPartitionInputErrorKind};

#[test]
fn valid_control_target_round_trips_without_core_types() {
    let partition = AssignedConsumerPartition::try_new("orders", 7)
        .unwrap_or_else(|error| panic!("valid control target: {error}"));

    assert_eq!(partition.topic(), "orders");
    assert_eq!(partition.partition(), 7);
}

#[test]
fn invalid_control_target_scalar_domains_are_rejected() {
    let long_topic = "x".repeat(250);
    let cases = [
        (
            AssignedConsumerPartition::try_new("", 0),
            AssignedConsumerPartitionInputErrorKind::EmptyTopic,
        ),
        (
            AssignedConsumerPartition::try_new(long_topic, 0),
            AssignedConsumerPartitionInputErrorKind::TopicTooLong,
        ),
        (
            AssignedConsumerPartition::try_new("orders", -1),
            AssignedConsumerPartitionInputErrorKind::NegativePartition,
        ),
    ];

    for (result, expected) in cases {
        let error = result.err().unwrap_or_else(|| panic!("input must fail"));
        assert_eq!(error.kind(), expected);
    }
}
