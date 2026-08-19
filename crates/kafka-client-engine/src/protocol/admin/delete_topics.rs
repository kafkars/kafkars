//! Generated name-or-topic-ID `DeleteTopics` construction and response correlation.

use kafka_client_core::{DeleteTopicIdOutcome, DeleteTopicOutcome, DeleteTopicsPlan};
use kafka_wire::{
    DeleteTopicsRequest, DeleteTopicsResponse, delete_topics_request::DeleteTopicState,
    delete_topics_response::DeletableTopicResult,
};
use kafka_wire_core::Uuid;

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
    /// The broker returned a topic ID not present in the request.
    UnexpectedTopicId,
    /// The broker omitted one requested topic-ID result.
    MissingTopicId,
    /// The broker returned one requested topic ID more than once.
    DuplicateTopicId,
}

/// Builds a generated v1-v5 name request or exact-v6 topic-ID request.
pub(crate) fn delete_topics_request(
    plan: &DeleteTopicsPlan,
    timeout_ms: i32,
) -> Result<DeleteTopicsRequest, DeleteTopicsRequestError> {
    if timeout_ms < 0 {
        return Err(DeleteTopicsRequestError::NegativeTimeout);
    }
    let mut request = DeleteTopicsRequest::default();
    if plan.topic_ids().is_empty() {
        request.topic_names = plan
            .topics()
            .iter()
            .map(|topic| topic.as_str().into())
            .collect();
    } else {
        request.topics = plan
            .topic_ids()
            .iter()
            .map(|topic_id| {
                let mut topic = DeleteTopicState::default();
                topic.name = None;
                topic.topic_id = Uuid::from_bytes(*topic_id);
                topic
            })
            .collect();
    }
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

pub(crate) fn normalize_delete_topic_ids_response_bounded(
    plan: &DeleteTopicsPlan,
    response: &DeleteTopicsResponse,
    retained_bytes: usize,
) -> Result<Vec<DeleteTopicIdOutcome>, DeleteTopicsProtocolFailure> {
    super::delete_topics_budget::normalize_ids(plan, response, retained_bytes)
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

pub(super) fn validate_topic_id_response_shape(
    plan: &DeleteTopicsPlan,
    response: &DeleteTopicsResponse,
) -> Result<(), DeleteTopicsProtocolFailure> {
    if plan.topic_ids().len() != response.responses.len() {
        return Err(DeleteTopicsProtocolFailure::TopicCount {
            expected: plan.topic_ids().len(),
            actual: response.responses.len(),
        });
    }
    for result in &response.responses {
        let topic_id = *result.topic_id.as_bytes();
        if !plan.topic_ids().contains(&topic_id) {
            return Err(DeleteTopicsProtocolFailure::UnexpectedTopicId);
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

pub(super) fn matching_topic_id_result<'a>(
    requested_topic_id: &[u8; 16],
    results: &'a [DeletableTopicResult],
) -> Result<&'a DeletableTopicResult, DeleteTopicsProtocolFailure> {
    let mut matches = results
        .iter()
        .filter(|result| result.topic_id.as_bytes() == requested_topic_id);
    let Some(result) = matches.next() else {
        return Err(DeleteTopicsProtocolFailure::MissingTopicId);
    };
    if matches.next().is_some() {
        return Err(DeleteTopicsProtocolFailure::DuplicateTopicId);
    }
    Ok(result)
}
