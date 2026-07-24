//! Lossless facade-to-engine assignment conversion tests.

use kafka_client_engine::AssignedConsumerStartPosition as EngineStart;

use super::consumer_assignment::into_engine_assignment;
use crate::{
    ErrorKind,
    consumer::{StartPosition, TopicPartition},
};

#[test]
fn conversion_preserves_topic_partition_and_every_start_position() {
    for (start, expected) in [
        (StartPosition::Beginning, EngineStart::Beginning),
        (StartPosition::End, EngineStart::End),
        (StartPosition::Offset(42), EngineStart::Offset(42)),
    ] {
        let entry = into_engine_assignment(TopicPartition::new("orders", 3).start_at(start))
            .unwrap_or_else(|error| panic!("convert assignment: {error}"));

        assert_eq!(entry.topic(), "orders");
        assert_eq!(entry.partition(), 3);
        assert_eq!(entry.start(), expected);
    }
}

#[test]
fn incomplete_or_unrepresentable_values_remain_configuration_errors() {
    for entry in [
        TopicPartition::new("orders", 0),
        TopicPartition::new("", 0).start_at(StartPosition::Beginning),
        TopicPartition::new("orders", -1).start_at(StartPosition::Beginning),
        TopicPartition::new("orders", 0).start_at(StartPosition::Offset(-1)),
    ] {
        let error = into_engine_assignment(entry)
            .err()
            .unwrap_or_else(|| panic!("invalid assignment must fail"));
        assert_eq!(error.kind(), ErrorKind::Configuration);
    }
}
