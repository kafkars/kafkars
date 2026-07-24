//! Generated automatic-assignment `CreatePartitions` construction and correlation.

use kafka_client_core::{CreatePartitionsPlan, PartitionIncreaseOutcome};
use kafka_wire::{
    CreatePartitionsRequest, CreatePartitionsResponse,
    create_partitions_request::CreatePartitionsTopic,
    create_partitions_response::CreatePartitionsTopicResult,
};

pub(crate) use super::request_timeout_error::{
    AdminRequestDeadlineError as CreatePartitionsRequestError, remaining_timeout_ms,
};

/// Invalid generated response shape for the requested topic set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CreatePartitionsProtocolFailure {
    /// Ordered results cannot fit the accepted retained-result reservation.
    RetainedBytes,
    /// The broker returned a different number of results.
    TopicCount {
        /// Number of requested topics.
        expected: usize,
        /// Number of returned topic results.
        actual: usize,
    },
    /// The broker returned a result not present in the request.
    UnexpectedTopic,
    /// The broker omitted one requested topic result.
    MissingTopic,
    /// The broker returned one requested topic more than once.
    DuplicateTopic,
}

/// Builds generated API-key 37 input using broker-chosen assignments.
pub(crate) fn create_partitions_request(
    plan: &CreatePartitionsPlan,
    timeout_ms: i32,
) -> Result<CreatePartitionsRequest, CreatePartitionsRequestError> {
    if timeout_ms < 0 {
        return Err(CreatePartitionsRequestError::NegativeTimeout);
    }
    let mut request = CreatePartitionsRequest::default();
    request.topics = plan
        .topics()
        .iter()
        .map(|topic| {
            let mut generated = CreatePartitionsTopic::default();
            generated.name = topic.topic().into();
            generated.count = topic.total_count();
            generated.assignments = None;
            generated
        })
        .collect();
    request.timeout_ms = timeout_ms;
    request.validate_only = plan.validate_only();
    Ok(request)
}

pub(crate) fn normalize_create_partitions_response_bounded(
    plan: &CreatePartitionsPlan,
    response: &CreatePartitionsResponse,
    retained_bytes: usize,
) -> Result<Vec<PartitionIncreaseOutcome>, CreatePartitionsProtocolFailure> {
    super::create_partitions_budget::normalize(plan, response, retained_bytes)
}

pub(super) fn validate_response_shape(
    plan: &CreatePartitionsPlan,
    response: &CreatePartitionsResponse,
) -> Result<(), CreatePartitionsProtocolFailure> {
    if plan.topics().len() != response.results.len() {
        return Err(CreatePartitionsProtocolFailure::TopicCount {
            expected: plan.topics().len(),
            actual: response.results.len(),
        });
    }
    if response.results.iter().any(|result| {
        !plan
            .topics()
            .iter()
            .any(|topic| topic.topic() == result.name.as_str())
    }) {
        return Err(CreatePartitionsProtocolFailure::UnexpectedTopic);
    }
    Ok(())
}

pub(super) fn matching_result<'a>(
    requested_topic: &str,
    results: &'a [CreatePartitionsTopicResult],
) -> Result<&'a CreatePartitionsTopicResult, CreatePartitionsProtocolFailure> {
    let mut matches = results
        .iter()
        .filter(|result| result.name.as_str() == requested_topic);
    let Some(result) = matches.next() else {
        return Err(CreatePartitionsProtocolFailure::MissingTopic);
    };
    if matches.next().is_some() {
        return Err(CreatePartitionsProtocolFailure::DuplicateTopic);
    }
    Ok(result)
}
