//! Deterministic backoff and original-deadline bounds for visibility probes.

use kafka_client_core::{Deadline, Moment};
use kafka_driver::{CallFailure, Delivery, RequestError, TopicName};
use kafka_wire::{
    MetadataResponse,
    metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
};
use kafka_wire_core::StrBytes;

use super::{
    super::topic_view::TopicPartitionCountFailure,
    retry::{
        VisibilityRetry, exact_topic_metadata_request, normalize_direct_request_error,
        normalize_direct_response, visibility_failure_is_transient,
    },
};

#[test]
fn only_visibility_lag_failures_are_retryable() {
    for failure in [
        TopicPartitionCountFailure::Unavailable,
        TopicPartitionCountFailure::Refresh,
        TopicPartitionCountFailure::Broker(3),
        TopicPartitionCountFailure::Broker(5),
    ] {
        assert!(visibility_failure_is_transient(failure));
    }
    for failure in [
        TopicPartitionCountFailure::Deadline,
        TopicPartitionCountFailure::Broker(29),
        TopicPartitionCountFailure::Malformed,
        TopicPartitionCountFailure::Completion,
    ] {
        assert!(!visibility_failure_is_transient(failure));
    }
}

#[test]
fn visibility_retry_backoff_grows_then_caps() {
    let now = Moment::from_tick(1_000);
    let deadline = Deadline::from_tick(1_000_000_000);

    let first = VisibilityRetry::schedule(now, deadline, 1)
        .unwrap_or_else(|| panic!("first retry must fit"));
    let second = VisibilityRetry::schedule(now, deadline, 2)
        .unwrap_or_else(|| panic!("second retry must fit"));
    let capped = VisibilityRetry::schedule(now, deadline, 3)
        .unwrap_or_else(|| panic!("capped retry must fit"));
    let still_capped = VisibilityRetry::schedule(now, deadline, 255)
        .unwrap_or_else(|| panic!("last retry must fit"));

    assert_eq!(first.not_before(), Deadline::from_tick(25_001_000));
    assert_eq!(second.not_before(), Deadline::from_tick(50_001_000));
    assert_eq!(capped.not_before(), Deadline::from_tick(100_001_000));
    assert_eq!(still_capped.not_before(), capped.not_before());
}

#[test]
fn visibility_retry_never_crosses_original_deadline() {
    assert!(
        VisibilityRetry::schedule(
            Moment::from_tick(75_000_000),
            Deadline::from_tick(100_000_000),
            1,
        )
        .is_none()
    );
}

#[test]
fn visibility_retry_preserves_due_time() {
    let retry =
        VisibilityRetry::schedule(Moment::from_tick(10), Deadline::from_tick(1_000_000_000), 1)
            .unwrap_or_else(|| panic!("retry must fit"));

    assert!(!retry.is_due(Moment::from_tick(retry.not_before().tick() - 1)));
    assert!(retry.is_due(Moment::from_tick(retry.not_before().tick())));
}

#[test]
fn direct_probe_is_exact_and_cannot_auto_create() {
    let topic = topic();
    let request = exact_topic_metadata_request(&topic);
    let [requested] = request
        .topics
        .as_deref()
        .unwrap_or_else(|| panic!("exact topic selection"))
    else {
        panic!("one exact topic must be requested");
    };
    assert_eq!(requested.name.as_deref(), Some("fresh"));
    assert!(!request.allow_auto_topic_creation);
    assert!(!request.include_cluster_authorized_operations);
    assert!(!request.include_topic_authorized_operations);
}

#[test]
fn direct_probe_requires_one_contiguous_logical_domain() {
    let topic = topic();
    let response = metadata_response("fresh", 0, &[1, 0]);
    assert_eq!(normalize_direct_response(&topic, &response), Ok(2));

    for malformed in [
        metadata_response("other", 0, &[0, 1]),
        metadata_response("fresh", 0, &[]),
        metadata_response("fresh", 0, &[0, 0]),
        metadata_response("fresh", 0, &[0, 2]),
    ] {
        assert!(matches!(
            normalize_direct_response(&topic, &malformed),
            Err(TopicPartitionCountFailure::Malformed | TopicPartitionCountFailure::TopicMismatch)
        ));
    }
    assert_eq!(
        normalize_direct_response(&topic, &metadata_response("fresh", 3, &[])),
        Err(TopicPartitionCountFailure::Broker(3))
    );
}

#[test]
fn direct_probe_only_retries_transport_unavailability() {
    assert_eq!(
        normalize_direct_request_error(&RequestError::RouteUnavailable),
        TopicPartitionCountFailure::Refresh
    );
    assert_eq!(
        normalize_direct_request_error(&RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::PossiblySent,
        }),
        TopicPartitionCountFailure::Deadline
    );
}

fn topic() -> TopicName {
    TopicName::new("fresh").unwrap_or_else(|error| panic!("valid topic: {error}"))
}

fn metadata_response(name: &str, error_code: i16, partitions: &[i32]) -> MetadataResponse {
    let mut topic = MetadataResponseTopic::default();
    topic.name = Some(StrBytes::from(name.to_owned()));
    topic.error_code = error_code;
    topic.partitions = partitions
        .iter()
        .map(|partition_index| {
            let mut partition = MetadataResponsePartition::default();
            partition.partition_index = *partition_index;
            partition
        })
        .collect();
    let mut response = MetadataResponse::default();
    response.topics.push(topic);
    response
}
