//! Generated `CreateTopics` construction and ordered response normalization.

use core::num::NonZeroI16;

use kafka_client_core::{CreateTopicBrokerError, CreateTopicOutcome, CreateTopicsPlan};
use kafka_wire::{
    CreateTopicsRequest, CreateTopicsResponse,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    create_topics_response::CreatableTopicResult,
};

/// Request construction failure before any driver ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CreateTopicsRequestError {
    /// A deadline adapter supplied an impossible negative broker timeout.
    NegativeTimeout,
}

/// Invalid generated response shape for the requested topic set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CreateTopicsProtocolFailure {
    /// The broker returned a different number of per-topic results.
    TopicCount {
        /// Number of requested topics.
        expected: usize,
        /// Number of returned topic results.
        actual: usize,
    },
    /// The broker returned a result not present in the request.
    UnexpectedTopic {
        /// Unexpected topic name.
        topic: String,
    },
    /// The broker omitted one requested topic result.
    MissingTopic {
        /// Missing topic name.
        topic: String,
    },
    /// The broker returned one requested topic more than once.
    DuplicateTopic {
        /// Duplicated topic name.
        topic: String,
    },
}

/// Builds the generated request without recreating deadline or retry policy.
///
/// The future host join must derive `timeout_ms` from the original absolute
/// deadline immediately before driver submission.
pub(crate) fn create_topics_request(
    plan: &CreateTopicsPlan,
    timeout_ms: i32,
) -> Result<CreateTopicsRequest, CreateTopicsRequestError> {
    if timeout_ms < 0 {
        return Err(CreateTopicsRequestError::NegativeTimeout);
    }
    let topics = plan
        .topics()
        .iter()
        .map(|topic| {
            let configs = topic
                .configs()
                .iter()
                .map(|config| {
                    let mut wire_config = CreatableTopicConfig::default();
                    wire_config.name = config.name().into();
                    wire_config.value = config.value().map(Into::into);
                    wire_config
                })
                .collect();
            let mut wire_topic = CreatableTopic::default();
            wire_topic.name = topic.name().into();
            wire_topic.num_partitions = topic.partitions();
            wire_topic.replication_factor = topic.replication_factor();
            wire_topic.configs = configs;
            wire_topic
        })
        .collect();
    let mut request = CreateTopicsRequest::default();
    request.topics = topics;
    request.timeout_ms = timeout_ms;
    request.validate_only = plan.validate_only();
    Ok(request)
}

/// Normalizes generated per-topic results into original request order.
///
/// Numeric broker errors remain exact even when the current client does not
/// recognize their meaning. Any uncorrelatable response fails structurally
/// rather than assigning a result to the wrong requested topic.
pub(crate) fn normalize_create_topics_response(
    plan: &CreateTopicsPlan,
    response: &CreateTopicsResponse,
) -> Result<Vec<CreateTopicOutcome>, CreateTopicsProtocolFailure> {
    if plan.topics().len() != response.topics.len() {
        return Err(CreateTopicsProtocolFailure::TopicCount {
            expected: plan.topics().len(),
            actual: response.topics.len(),
        });
    }
    if let Some(topic) = response.topics.iter().find(|result| {
        !plan
            .topics()
            .iter()
            .any(|topic| topic.name() == result.name.as_str())
    }) {
        return Err(CreateTopicsProtocolFailure::UnexpectedTopic {
            topic: topic.name.as_str().to_owned(),
        });
    }

    plan.topics()
        .iter()
        .map(|topic| matching_outcome(topic.name(), &response.topics))
        .collect()
}

fn matching_outcome(
    requested_topic: &str,
    results: &[CreatableTopicResult],
) -> Result<CreateTopicOutcome, CreateTopicsProtocolFailure> {
    let mut matches = results
        .iter()
        .filter(|result| result.name.as_str() == requested_topic);
    let Some(result) = matches.next() else {
        return Err(CreateTopicsProtocolFailure::MissingTopic {
            topic: requested_topic.to_owned(),
        });
    };
    if matches.next().is_some() {
        return Err(CreateTopicsProtocolFailure::DuplicateTopic {
            topic: requested_topic.to_owned(),
        });
    }
    let Some(code) = NonZeroI16::new(result.error_code) else {
        return Ok(CreateTopicOutcome::created(requested_topic));
    };
    let message = result
        .error_message
        .as_ref()
        .map(|value| value.as_str().to_owned());
    Ok(CreateTopicOutcome::failed(
        requested_topic,
        CreateTopicBrokerError::new(code, message),
    ))
}
