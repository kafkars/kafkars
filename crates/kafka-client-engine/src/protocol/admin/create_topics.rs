//! Generated `CreateTopics` construction and ordered response normalization.

use kafka_client_core::{CreateTopicOutcome, CreateTopicsPlan};
use kafka_wire::{
    CreateTopicsRequest, CreateTopicsResponse,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    create_topics_response::CreatableTopicResult,
};

pub(crate) use super::request_timeout_error::{
    AdminRequestDeadlineError as CreateTopicsRequestError, remaining_timeout_ms,
};

/// Invalid generated response shape for the requested topic set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CreateTopicsProtocolFailure {
    /// Fixed ordered results cannot fit the accepted retained-result reservation.
    RetainedBytes,
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

pub(crate) fn normalize_create_topics_response_bounded(
    plan: &CreateTopicsPlan,
    response: &CreateTopicsResponse,
    retained_bytes: usize,
) -> Result<Vec<CreateTopicOutcome>, CreateTopicsProtocolFailure> {
    super::result_budget::normalize(plan, response, retained_bytes)
}

pub(super) fn validate_response_shape(
    plan: &CreateTopicsPlan,
    response: &CreateTopicsResponse,
) -> Result<(), CreateTopicsProtocolFailure> {
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
    Ok(())
}

pub(super) fn matching_result<'a>(
    requested_topic: &str,
    results: &'a [CreatableTopicResult],
) -> Result<&'a CreatableTopicResult, CreateTopicsProtocolFailure> {
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
    Ok(result)
}
