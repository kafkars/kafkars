//! Lossless facade-to-engine one-partition control conversion.

use kafka_client_engine::AssignedConsumerPartition as EnginePartition;

use super::control::engine_partition;
use crate::{ErrorKind, consumer::TopicPartition};

#[test]
fn conversion_reuses_only_topic_partition_identity() {
    let facade =
        TopicPartition::new("orders", 7).start_at(crate::consumer::StartPosition::Offset(41));
    let engine =
        engine_partition(&facade).unwrap_or_else(|error| panic!("convert control target: {error}"));

    assert_eq!(
        engine,
        EnginePartition::try_new("orders", 7)
            .unwrap_or_else(|error| panic!("expected engine target: {error}"))
    );
}

#[test]
fn every_invalid_scalar_target_is_a_configuration_error() {
    for target in [
        TopicPartition::new("", 0),
        TopicPartition::new("orders", -1),
        TopicPartition::new("x".repeat(250), 0),
    ] {
        let error = engine_partition(&target)
            .err()
            .unwrap_or_else(|| panic!("invalid target must fail"));
        assert_eq!(error.kind(), ErrorKind::Configuration);
    }
}
