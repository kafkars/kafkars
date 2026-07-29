//! Ordered bounded normalization of generated Metadata topic results.

use core::num::NonZeroI16;

use kafka_client_core::{DescribeTopicsInput, DescribeTopicsPlan, DescribeTopicsSelection};
use kafka_wire::{MetadataResponse, metadata_response::MetadataResponseTopic};

use super::{
    describe_topic_value::{normalize_topic, normalize_topic_by_id},
    describe_topics_budget::{ensure_result_fits, ensure_topic_id_result_fits},
    list_topics_response::normalize_list_topics_response,
};

/// Invalid or over-budget generated response shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DescribeTopicsProtocolFailure {
    RetainedBytes,
    TopicCount,
    MissingTopicName,
    EmptyTopicName,
    UnexpectedTopic,
    MissingTopic,
    DuplicateTopic,
    MissingTopicId,
    ZeroTopicId,
    UnexpectedTopicId,
    DuplicateTopicId,
    PartitionIndex,
    DuplicatePartition,
    LeaderId,
    LeaderEpoch,
    BrokerId,
    DuplicateBrokerId,
    ReplicaMembership,
    UnexpectedAuthorizedOperations,
}

/// Converts one generated Metadata response into a deterministic core fact.
pub(crate) fn normalize_describe_topics_response(
    plan: &DescribeTopicsPlan,
    response: &MetadataResponse,
    retained_bytes: usize,
) -> Result<DescribeTopicsInput, DescribeTopicsProtocolFailure> {
    if let Some(code) = NonZeroI16::new(response.error_code) {
        return Ok(DescribeTopicsInput::BrokerRejected { code });
    }
    match plan.selection() {
        DescribeTopicsSelection::Named(topics) => normalize_named_topics_response(
            topics,
            response,
            retained_bytes,
            plan.include_authorized_operations(),
        ),
        DescribeTopicsSelection::Ids(topic_ids) => normalize_topic_ids_response(
            topic_ids,
            response,
            retained_bytes,
            plan.include_authorized_operations(),
        ),
        DescribeTopicsSelection::All { .. } => {
            normalize_list_topics_response(response, retained_bytes)
        }
    }
}

fn normalize_topic_ids_response(
    topic_ids: &[[u8; 16]],
    response: &MetadataResponse,
    retained_bytes: usize,
    include_authorized_operations: bool,
) -> Result<DescribeTopicsInput, DescribeTopicsProtocolFailure> {
    validate_topic_id_shape(topic_ids, response)?;
    if !ensure_topic_id_result_fits(topic_ids, response, retained_bytes) {
        return Err(DescribeTopicsProtocolFailure::RetainedBytes);
    }
    let mut outcomes = Vec::with_capacity(topic_ids.len());
    for requested in topic_ids {
        let topic = matching_topic_id(*requested, &response.topics)?;
        outcomes.push(normalize_topic_by_id(
            *requested,
            topic,
            include_authorized_operations,
        )?);
    }
    Ok(DescribeTopicsInput::BrokerRespondedById { outcomes })
}

fn normalize_named_topics_response(
    topics: &[String],
    response: &MetadataResponse,
    retained_bytes: usize,
    include_authorized_operations: bool,
) -> Result<DescribeTopicsInput, DescribeTopicsProtocolFailure> {
    validate_topic_shape(topics, response)?;
    if !ensure_result_fits(topics, response, retained_bytes) {
        return Err(DescribeTopicsProtocolFailure::RetainedBytes);
    }
    let mut outcomes = Vec::with_capacity(topics.len());
    for requested in topics {
        let topic = matching_topic(requested, &response.topics)?;
        outcomes.push(normalize_topic(
            requested,
            topic,
            include_authorized_operations,
        )?);
    }
    Ok(DescribeTopicsInput::BrokerResponded { outcomes })
}

fn validate_topic_shape(
    topics: &[String],
    response: &MetadataResponse,
) -> Result<(), DescribeTopicsProtocolFailure> {
    if topics.len() != response.topics.len() {
        return Err(DescribeTopicsProtocolFailure::TopicCount);
    }
    for topic in &response.topics {
        let Some(name) = &topic.name else {
            return Err(DescribeTopicsProtocolFailure::MissingTopicName);
        };
        if !topics.iter().any(|requested| requested == name.as_str()) {
            return Err(DescribeTopicsProtocolFailure::UnexpectedTopic);
        }
    }
    Ok(())
}

fn matching_topic<'a>(
    requested: &str,
    topics: &'a [MetadataResponseTopic],
) -> Result<&'a MetadataResponseTopic, DescribeTopicsProtocolFailure> {
    let mut matches = topics.iter().filter(|topic| {
        topic
            .name
            .as_ref()
            .is_some_and(|name| name.as_str() == requested)
    });
    let Some(topic) = matches.next() else {
        return Err(DescribeTopicsProtocolFailure::MissingTopic);
    };
    if matches.next().is_some() {
        return Err(DescribeTopicsProtocolFailure::DuplicateTopic);
    }
    Ok(topic)
}

fn validate_topic_id_shape(
    topic_ids: &[[u8; 16]],
    response: &MetadataResponse,
) -> Result<(), DescribeTopicsProtocolFailure> {
    if topic_ids.len() != response.topics.len() {
        return Err(DescribeTopicsProtocolFailure::TopicCount);
    }
    for topic in &response.topics {
        if topic.topic_id.is_zero() {
            return Err(DescribeTopicsProtocolFailure::ZeroTopicId);
        }
        let returned = topic.topic_id.to_bytes();
        if !topic_ids.contains(&returned) {
            return Err(DescribeTopicsProtocolFailure::UnexpectedTopicId);
        }
    }
    Ok(())
}

fn matching_topic_id(
    requested: [u8; 16],
    topics: &[MetadataResponseTopic],
) -> Result<&MetadataResponseTopic, DescribeTopicsProtocolFailure> {
    let mut matches = topics
        .iter()
        .filter(|topic| topic.topic_id.to_bytes() == requested);
    let Some(topic) = matches.next() else {
        return Err(DescribeTopicsProtocolFailure::MissingTopicId);
    };
    if matches.next().is_some() {
        return Err(DescribeTopicsProtocolFailure::DuplicateTopicId);
    }
    Ok(topic)
}
