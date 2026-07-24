//! Ordered bounded normalization of generated v0/v1 resource results.

use core::num::NonZeroI16;

use kafka_client_core::{
    IncrementalAlterConfigBrokerError, IncrementalAlterConfigOutcome, IncrementalAlterConfigsBatch,
    IncrementalAlterConfigsPlan,
};
use kafka_wire::{
    IncrementalAlterConfigsResponse,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
};

use super::{resource::TOPIC_RESOURCE_TYPE, retention::ensure_result_fits};
use crate::{
    admin::retention::RESULT_DIAGNOSTIC_BYTES_PER_TOPIC,
    protocol::admin::result_budget::bounded_message,
};

/// Invalid generated response shape or retained terminal charge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IncrementalAlterConfigsProtocolFailure {
    /// Ordered results cannot fit the accepted retained-byte reservation.
    RetainedBytes,
    /// Kafka returned a negative throttle duration.
    ThrottleTime,
    /// Kafka returned a different number of resource results.
    ResourceCount,
    /// Kafka returned a resource type outside the topic-only slice.
    UnexpectedResourceType,
    /// Kafka returned a topic that was not requested.
    UnexpectedTopic,
    /// Kafka omitted one requested topic.
    MissingTopic,
    /// Kafka returned one requested topic more than once.
    DuplicateTopic,
}

/// Converts generated results into caller-ordered, wire-free core facts.
pub(crate) fn normalize_incremental_alter_configs_response_bounded(
    plan: &IncrementalAlterConfigsPlan,
    response: &IncrementalAlterConfigsResponse,
    retained_bytes: usize,
) -> Result<IncrementalAlterConfigsBatch, IncrementalAlterConfigsProtocolFailure> {
    let throttle_time_ms = u32::try_from(response.throttle_time_ms)
        .map_err(|_| IncrementalAlterConfigsProtocolFailure::ThrottleTime)?;
    validate_response_shape(plan, response)?;
    ensure_result_fits(plan, retained_bytes)?;
    let topics = plan
        .topics()
        .iter()
        .map(|topic| {
            let result = matching_result(topic.topic(), response)?;
            Ok(normalize_topic(topic.topic(), result))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(IncrementalAlterConfigsBatch::new(throttle_time_ms, topics))
}

fn validate_response_shape(
    plan: &IncrementalAlterConfigsPlan,
    response: &IncrementalAlterConfigsResponse,
) -> Result<(), IncrementalAlterConfigsProtocolFailure> {
    if plan.topics().len() != response.responses.len() {
        return Err(IncrementalAlterConfigsProtocolFailure::ResourceCount);
    }
    if response
        .responses
        .iter()
        .any(|result| result.resource_type != TOPIC_RESOURCE_TYPE)
    {
        return Err(IncrementalAlterConfigsProtocolFailure::UnexpectedResourceType);
    }
    if response.responses.iter().any(|result| {
        !plan
            .topics()
            .iter()
            .any(|topic| topic.topic() == result.resource_name.as_str())
    }) {
        return Err(IncrementalAlterConfigsProtocolFailure::UnexpectedTopic);
    }
    Ok(())
}

fn matching_result<'a>(
    requested_topic: &str,
    response: &'a IncrementalAlterConfigsResponse,
) -> Result<&'a AlterConfigsResourceResponse, IncrementalAlterConfigsProtocolFailure> {
    let mut matches = response
        .responses
        .iter()
        .filter(|result| result.resource_name.as_str() == requested_topic);
    let Some(result) = matches.next() else {
        return Err(IncrementalAlterConfigsProtocolFailure::MissingTopic);
    };
    if matches.next().is_some() {
        return Err(IncrementalAlterConfigsProtocolFailure::DuplicateTopic);
    }
    Ok(result)
}

fn normalize_topic(
    topic: &str,
    result: &AlterConfigsResourceResponse,
) -> IncrementalAlterConfigOutcome {
    let Some(code) = NonZeroI16::new(result.error_code) else {
        return IncrementalAlterConfigOutcome::altered(topic);
    };
    #[allow(
        clippy::redundant_closure_for_method_calls,
        reason = "the generated string type is intentionally not a direct engine dependency"
    )]
    let message = result.error_message.as_ref().map(|value| value.as_str());
    let (message, truncated) = bounded_message(message, RESULT_DIAGNOSTIC_BYTES_PER_TOPIC);
    IncrementalAlterConfigOutcome::failed(
        topic,
        IncrementalAlterConfigBrokerError::new(code, message, truncated),
    )
}
