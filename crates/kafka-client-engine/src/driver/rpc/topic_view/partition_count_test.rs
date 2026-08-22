//! Topic-view adapter type and exact failure-domain smoke scenarios.

use std::time::{Duration, Instant};

use kafka_driver::{KafkaTopicId, SubmitError};

use crate::{
    EngineConfig, driver::DriverOwner,
    driver::rpc::fetch::routed_response_broker_test::RoutedBroker,
};

use super::{
    TopicPartitionCountAdmissionFailure, TopicPartitionCountAdmissionFailureKind,
    TopicPartitionCountFact, TopicPartitionCountFailure, partition_count::topic_id_bytes,
};

#[test]
fn scalar_fact_retains_generation_and_total_logical_count() {
    let fact = TopicPartitionCountFact {
        metadata_generation: 11,
        logical_partition_count: 7,
        kafka_topic_id: Some([5; 16]),
    };

    assert_eq!(fact.metadata_generation, 11);
    assert_eq!(fact.logical_partition_count, 7);
    assert_eq!(fact.kafka_topic_id, Some([5; 16]));
}

#[test]
fn driver_topic_identity_projection_preserves_exact_bytes_and_absence() {
    let bytes = [5; 16];
    let topic_id =
        KafkaTopicId::from_bytes(bytes).unwrap_or_else(|| panic!("nonzero Kafka topic identity"));

    assert_eq!(topic_id_bytes(Some(topic_id)), Some(bytes));
    assert_eq!(topic_id_bytes(None), None);
}

#[test]
fn live_topic_view_retains_broker_issued_topic_identity() {
    let mut broker = RoutedBroker::new();
    let mut driver = DriverOwner::build(&EngineConfig::new(vec![broker.endpoint()]))
        .unwrap_or_else(|error| panic!("build topic-view driver: {error}"));
    RoutedBroker::await_seed(&mut driver);
    broker.install_cluster(&mut driver);
    let mut call = super::TopicPartitionCountCall::submit(
        &driver,
        "events",
        Instant::now() + Duration::from_secs(60),
    )
    .unwrap_or_else(|error| panic!("submit topic view: {error}"));
    broker.install_topic(&mut driver);

    let fact = (0..32)
        .find_map(|_| {
            call.try_terminal().or_else(|| {
                driver
                    .turn(Duration::from_millis(100))
                    .unwrap_or_else(|error| panic!("settle topic view: {error}"));
                call.try_terminal()
            })
        })
        .unwrap_or_else(|| panic!("topic view must settle after Metadata response"))
        .unwrap_or_else(|error| panic!("topic view: {error:?}"));
    assert_eq!(fact.kafka_topic_id, Some([7; 16]));
    assert_eq!(fact.logical_partition_count, 4);
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown topic-view driver: {error}"));
}

#[test]
fn completion_fault_stays_distinct_from_metadata_unavailability() {
    assert_ne!(
        TopicPartitionCountFailure::Completion,
        TopicPartitionCountFailure::Unavailable
    );
}

#[test]
fn only_full_driver_admission_is_retryable_backpressure() {
    assert_eq!(
        TopicPartitionCountAdmissionFailure::Driver(SubmitError::Full).kind(),
        TopicPartitionCountAdmissionFailureKind::Full
    );
    for source in [
        SubmitError::Closed,
        SubmitError::Wake(std::io::Error::other("wake failed")),
        SubmitError::IdentityExhausted,
        SubmitError::ForeignDriver,
    ] {
        assert_eq!(
            TopicPartitionCountAdmissionFailure::Driver(source).kind(),
            TopicPartitionCountAdmissionFailureKind::Terminal
        );
    }
}
