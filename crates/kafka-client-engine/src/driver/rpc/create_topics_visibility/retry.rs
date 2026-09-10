//! Bounded direct Metadata retries for states that can lag a successful creation.

use std::time::Instant;

use kafka_client_core::{Deadline, Moment};
use kafka_driver::{Call, CallFailure, RequestError, TopicName};
use kafka_wire::{MetadataRequest, MetadataResponse, metadata_request::MetadataRequestTopic};

use super::super::{
    super::DriverOwner, describe_topics_submission::DESCRIBE_TOPICS_MIN_VERSION,
    topic_view::TopicPartitionCountFailure,
};

const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const LEADER_NOT_AVAILABLE: i16 = 5;
const INITIAL_VISIBILITY_RETRY_DELAY_TICKS: u64 = 25_000_000;
const MAX_VISIBILITY_RETRY_DELAY_TICKS: u64 = 100_000_000;

// At the capped delay this spans more than 25 seconds, so the usual 30-second
// Admin deadline remains the effective bound without admitting an unbounded loop.
pub(super) const MAX_VISIBILITY_ATTEMPTS_PER_TOPIC: usize = 256;

/// One exact-topic Metadata probe that deliberately does not mutate or consult
/// the driver's cached topic view. A delete/recreate changes the Kafka topic ID,
/// so the replacement topic's reset leader epochs are not regressions.
pub(super) struct DirectTopicPartitionCountCall {
    topic: TopicName,
    call: Option<Call<Result<MetadataResponse, RequestError>>>,
}

impl DirectTopicPartitionCountCall {
    pub(super) fn submit(
        driver: &DriverOwner,
        topic: TopicName,
        deadline: Instant,
    ) -> Result<Self, ()> {
        let request = exact_topic_metadata_request(&topic);
        let call = driver
            .submit_describe_topics(request, deadline, DESCRIBE_TOPICS_MIN_VERSION)
            .map_err(|_error| ())?;
        Ok(Self {
            topic,
            call: Some(call),
        })
    }

    pub(super) fn try_terminal(&mut self) -> Option<Result<u32, TopicPartitionCountFailure>> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        Some(match result {
            Err(_error) => Err(TopicPartitionCountFailure::Completion),
            Ok(Err(error)) => Err(normalize_direct_request_error(&error)),
            Ok(Ok(response)) => normalize_direct_response(&self.topic, &response),
        })
    }
}

pub(super) fn exact_topic_metadata_request(topic: &TopicName) -> MetadataRequest {
    let mut requested = MetadataRequestTopic::default();
    requested.name = Some(topic.as_str().into());
    let mut request = MetadataRequest::default();
    request.topics = Some(vec![requested]);
    request.allow_auto_topic_creation = false;
    request.include_cluster_authorized_operations = false;
    request.include_topic_authorized_operations = false;
    request
}

pub(super) fn normalize_direct_response(
    expected: &TopicName,
    response: &MetadataResponse,
) -> Result<u32, TopicPartitionCountFailure> {
    if response.error_code != 0 {
        return Err(TopicPartitionCountFailure::Broker(response.error_code));
    }
    let [topic] = response.topics.as_slice() else {
        return Err(TopicPartitionCountFailure::Malformed);
    };
    if topic.name.as_deref() != Some(expected.as_str()) {
        return Err(TopicPartitionCountFailure::TopicMismatch);
    }
    if topic.error_code != 0 {
        return Err(TopicPartitionCountFailure::Broker(topic.error_code));
    }
    let logical_partition_count = u32::try_from(topic.partitions.len())
        .ok()
        .filter(|count| *count != 0)
        .ok_or(TopicPartitionCountFailure::Malformed)?;
    let mut partition_indexes = Vec::new();
    partition_indexes
        .try_reserve_exact(topic.partitions.len())
        .map_err(|_error| TopicPartitionCountFailure::Allocation)?;
    partition_indexes.extend(
        topic
            .partitions
            .iter()
            .map(|partition| partition.partition_index),
    );
    partition_indexes.sort_unstable();
    if partition_indexes
        .iter()
        .enumerate()
        .any(|(expected, actual)| i32::try_from(expected) != Ok(*actual))
    {
        return Err(TopicPartitionCountFailure::Malformed);
    }
    Ok(logical_partition_count)
}

#[allow(
    unreachable_patterns,
    reason = "the published driver RC exposes a non-exhaustive request error"
)]
pub(super) fn normalize_direct_request_error(error: &RequestError) -> TopicPartitionCountFailure {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => TopicPartitionCountFailure::Deadline,
        RequestError::RouteUnavailable
        | RequestError::NameResolutionFailed { .. }
        | RequestError::ConnectionClosed(_)
        | RequestError::Rejected {
            failure: CallFailure::NotReady | CallFailure::ConnectionClosed { .. },
            ..
        } => TopicPartitionCountFailure::Refresh,
        RequestError::Decode(_) => TopicPartitionCountFailure::Malformed,
        _ => TopicPartitionCountFailure::UnrecognizedDriverFailure,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct VisibilityRetry {
    not_before: Deadline,
}

impl VisibilityRetry {
    pub(super) fn schedule(
        now: Moment,
        public_deadline: Deadline,
        attempts_completed: usize,
    ) -> Option<Self> {
        let not_before = now.checked_deadline_after(retry_delay_ticks(attempts_completed))?;
        if not_before.tick() >= public_deadline.tick() {
            return None;
        }
        Some(Self { not_before })
    }

    pub(super) const fn not_before(self) -> Deadline {
        self.not_before
    }

    pub(super) const fn is_due(self, now: Moment) -> bool {
        self.not_before.is_elapsed_at(now)
    }
}

const fn retry_delay_ticks(attempts_completed: usize) -> u64 {
    match attempts_completed {
        0 | 1 => INITIAL_VISIBILITY_RETRY_DELAY_TICKS,
        2 => INITIAL_VISIBILITY_RETRY_DELAY_TICKS * 2,
        _ => MAX_VISIBILITY_RETRY_DELAY_TICKS,
    }
}

pub(in super::super) const fn visibility_failure_is_transient(
    failure: TopicPartitionCountFailure,
) -> bool {
    matches!(
        failure,
        TopicPartitionCountFailure::Unavailable
            | TopicPartitionCountFailure::Refresh
            | TopicPartitionCountFailure::Broker(UNKNOWN_TOPIC_OR_PARTITION | LEADER_NOT_AVAILABLE)
    )
}
