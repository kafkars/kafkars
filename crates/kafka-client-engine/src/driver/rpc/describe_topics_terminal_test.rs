//! Semantic normalization scenarios for transient Metadata call terminals.

use kafka_client_core::{DeliveryStatus, DescribeTopicsInput, DescribeTopicsPlan};
use kafka_driver::{ApiKey, CallFailure, Delivery, RequestError};
use kafka_wire::{MetadataResponse, metadata_response::MetadataResponseTopic};
use kafka_wire_core::{ApiVersion, EncodeError};

use super::describe_topics_terminal::normalize_terminal;

#[test]
fn valid_generated_response_becomes_ordered_core_results() {
    let mut topic = MetadataResponseTopic::default();
    topic.name = Some("orders".into());
    let mut response = MetadataResponse::default();
    response.topics = vec![topic];

    assert!(matches!(
        normalize_terminal(&plan(), 128 * 1024, Ok(response)),
        DescribeTopicsInput::BrokerResponded { .. }
    ));
}

#[test]
fn malformed_and_valid_over_budget_responses_remain_distinct() {
    let response = MetadataResponse::default();
    assert_eq!(
        normalize_terminal(&plan(), 128 * 1024, Ok(response.clone())),
        DescribeTopicsInput::InvalidResponse
    );

    let mut topic = MetadataResponseTopic::default();
    topic.name = Some("orders".into());
    let mut response = response;
    response.topics = vec![topic];
    assert_eq!(
        normalize_terminal(&plan(), 1, Ok(response)),
        DescribeTopicsInput::ResponseTooLarge
    );
}

#[test]
fn driver_deadline_remains_timeout_with_authoritative_certainty() {
    let input = normalize_terminal(
        &plan(),
        128 * 1024,
        Err(RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::PossiblySent,
        }),
    );
    assert_eq!(
        input,
        DescribeTopicsInput::DriverDeadlineElapsed {
            delivery: DeliveryStatus::PossiblySent,
        }
    );
}

#[test]
fn old_broker_auto_creation_field_failure_is_local_compatibility() {
    let input = normalize_terminal(
        &plan(),
        128 * 1024,
        Err(RequestError::Encode(EncodeError::FieldNotRepresentable {
            message: "MetadataRequest",
            field: "AllowAutoTopicCreation",
            version: ApiVersion::new(3),
        })),
    );
    assert_eq!(input, DescribeTopicsInput::ProtocolIncompatible);
}

#[test]
fn all_topic_read_only_policy_failure_is_local_compatibility() {
    let input = normalize_terminal(
        &DescribeTopicsPlan::all(false),
        4 * 1024 * 1024,
        Err(RequestError::Encode(EncodeError::FieldNotRepresentable {
            message: "MetadataRequest",
            field: "AllowAutoTopicCreation",
            version: ApiVersion::new(3),
        })),
    );
    assert_eq!(input, DescribeTopicsInput::ProtocolIncompatible);
}

#[test]
fn version_floor_and_bounds_fail_before_metadata_transport() {
    let api_key = ApiKey::new(3);
    for failure in [
        RequestError::VersionFloorUnavailable {
            api_key,
            minimum: ApiVersion::new(4),
            negotiated_maximum: ApiVersion::new(3),
        },
        RequestError::VersionBoundsInvalid {
            api_key,
            minimum: ApiVersion::new(4),
            maximum: ApiVersion::new(3),
        },
    ] {
        assert_eq!(
            normalize_terminal(&plan(), 128 * 1024, Err(failure)),
            DescribeTopicsInput::ProtocolIncompatible
        );
    }
}

#[test]
fn old_broker_cannot_silently_drop_requested_authorized_operations() {
    let plan = plan().with_authorized_operations(true);
    let input = normalize_terminal(
        &plan,
        128 * 1024,
        Err(RequestError::VersionFloorUnavailable {
            api_key: ApiKey::new(3),
            minimum: ApiVersion::new(8),
            negotiated_maximum: ApiVersion::new(7),
        }),
    );
    assert_eq!(input, DescribeTopicsInput::ProtocolIncompatible);
}

fn plan() -> DescribeTopicsPlan {
    DescribeTopicsPlan::new(vec!["orders".to_owned()])
        .unwrap_or_else(|error| panic!("valid DescribeTopics plan: {error}"))
}
