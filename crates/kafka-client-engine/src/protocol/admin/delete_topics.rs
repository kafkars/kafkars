//! Generated name-based `DeleteTopics` construction and response correlation.

use kafka_client_core::{DeleteTopicOutcome, DeleteTopicsPlan};
use kafka_wire::{
    DeleteTopicsRequest, DeleteTopicsResponse, delete_topics_response::DeletableTopicResult,
};

pub(crate) use super::request_timeout_error::{
    AdminRequestDeadlineError as DeleteTopicsRequestError, remaining_timeout_ms,
};

/// Invalid generated response shape for the requested topic set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeleteTopicsProtocolFailure {
    /// Ordered results cannot fit the accepted retained-result reservation.
    RetainedBytes,
    /// The broker returned a different number of results.
    TopicCount {
        /// Number of requested topics.
        expected: usize,
        /// Number of returned topic results.
        actual: usize,
    },
    /// The broker returned a nullable name on the name-only v5 seam.
    MissingResponseName,
    /// The broker returned a result not present in the request.
    UnexpectedTopic,
    /// The broker omitted one requested topic result.
    MissingTopic,
    /// The broker returned one requested topic more than once.
    DuplicateTopic,
}

/// Builds the generated v1-v5 name-based request without metadata ownership.
pub(crate) fn delete_topics_request(
    plan: &DeleteTopicsPlan,
    timeout_ms: i32,
) -> Result<DeleteTopicsRequest, DeleteTopicsRequestError> {
    if timeout_ms < 0 {
        return Err(DeleteTopicsRequestError::NegativeTimeout);
    }
    let mut request = DeleteTopicsRequest::default();
    request.topic_names = plan
        .topics()
        .iter()
        .map(|topic| topic.as_str().into())
        .collect();
    request.timeout_ms = timeout_ms;
    Ok(request)
}

pub(crate) fn normalize_delete_topics_response_bounded(
    plan: &DeleteTopicsPlan,
    response: &DeleteTopicsResponse,
    retained_bytes: usize,
) -> Result<Vec<DeleteTopicOutcome>, DeleteTopicsProtocolFailure> {
    super::delete_topics_budget::normalize(plan, response, retained_bytes)
}

pub(super) fn validate_response_shape(
    plan: &DeleteTopicsPlan,
    response: &DeleteTopicsResponse,
) -> Result<(), DeleteTopicsProtocolFailure> {
    if plan.topics().len() != response.responses.len() {
        return Err(DeleteTopicsProtocolFailure::TopicCount {
            expected: plan.topics().len(),
            actual: response.responses.len(),
        });
    }
    for result in &response.responses {
        let Some(name) = result.name.as_ref() else {
            return Err(DeleteTopicsProtocolFailure::MissingResponseName);
        };
        if !plan.topics().iter().any(|topic| topic == name.as_str()) {
            return Err(DeleteTopicsProtocolFailure::UnexpectedTopic);
        }
    }
    Ok(())
}

pub(super) fn matching_result<'a>(
    requested_topic: &str,
    results: &'a [DeletableTopicResult],
) -> Result<&'a DeletableTopicResult, DeleteTopicsProtocolFailure> {
    let mut matches = results.iter().filter(|result| {
        result
            .name
            .as_ref()
            .is_some_and(|name| name.as_str() == requested_topic)
    });
    let Some(result) = matches.next() else {
        return Err(DeleteTopicsProtocolFailure::MissingTopic);
    };
    if matches.next().is_some() {
        return Err(DeleteTopicsProtocolFailure::DuplicateTopic);
    }
    Ok(result)
}
