//! Hosted classic-group batch-control conversion and category scenarios.

use kafka_client_engine::{
    GroupConsumerControlErrorKind as Kind, GroupConsumerResumeCaptureErrorKind as CaptureKind,
};

use super::{
    group_consumer::GroupConsumerEngine,
    group_consumer_control::{
        engine_partitions, translate_group_consumer_control_kind,
        translate_group_consumer_resume_capture_kind,
    },
};
use crate::{
    ErrorKind, KafkaError,
    consumer::{StartPosition, TopicPartition},
};

#[test]
fn bridge_controls_preserve_the_public_borrowed_batch_shape() {
    fn require(
        _control: fn(&mut GroupConsumerEngine, &[TopicPartition]) -> Result<(), KafkaError>,
    ) {
    }

    require(GroupConsumerEngine::pause);
    require(GroupConsumerEngine::resume);
}

#[test]
fn borrowed_targets_are_copied_in_order_without_position_policy() {
    let targets = [
        TopicPartition::new("orders", 7),
        TopicPartition::new("payments", 3),
    ];

    let converted =
        engine_partitions(&targets).unwrap_or_else(|error| panic!("convert targets: {error}"));

    assert_eq!(converted.len(), 2);
    assert_eq!(converted[0].topic(), "orders");
    assert_eq!(converted[0].partition(), 7);
    assert_eq!(converted[1].topic(), "payments");
    assert_eq!(converted[1].partition(), 3);
}

#[test]
fn direct_assignment_start_position_is_rejected_instead_of_discarded() {
    let targets = [TopicPartition::new("orders", 7).start_at(StartPosition::Offset(41))];

    let error = engine_partitions(&targets)
        .err()
        .unwrap_or_else(|| panic!("group control must reject a start position"));

    assert_eq!(error.kind(), ErrorKind::Configuration);
}

#[test]
fn empty_target_batch_is_an_inert_conversion() {
    assert!(
        engine_partitions(&[])
            .unwrap_or_else(|error| panic!("convert empty targets: {error}"))
            .is_empty()
    );
}

#[test]
fn every_invalid_scalar_target_is_configuration() {
    for target in [
        TopicPartition::new("", 0),
        TopicPartition::new("orders", -1),
        TopicPartition::new("x".repeat(250), 0),
    ] {
        let error = engine_partitions(&[target])
            .err()
            .unwrap_or_else(|| panic!("invalid target must fail"));
        assert_eq!(error.kind(), ErrorKind::Configuration);
    }
}

#[test]
fn every_control_error_kind_has_one_stable_facade_category() {
    for (kind, expected) in [
        (Kind::Contended, ErrorKind::Backpressure),
        (Kind::Pending, ErrorKind::Backpressure),
        (Kind::Closed, ErrorKind::State),
        (Kind::GroupUnavailable, ErrorKind::State),
        (Kind::NoAssignment, ErrorKind::State),
        (Kind::UnknownPartition, ErrorKind::State),
        (Kind::PositionNotRetained, ErrorKind::State),
        (Kind::DuplicatePartition, ErrorKind::Configuration),
        (Kind::HostUnavailable, ErrorKind::Internal),
        (Kind::ResourceExhausted, ErrorKind::Internal),
        (Kind::InternalInvariant, ErrorKind::Internal),
    ] {
        assert_eq!(translate_group_consumer_control_kind(kind).kind(), expected);
    }
}

#[test]
fn resume_deadline_capture_failure_is_internal() {
    assert_eq!(
        translate_group_consumer_resume_capture_kind(CaptureKind::HostUnavailable).kind(),
        ErrorKind::Internal
    );
}
