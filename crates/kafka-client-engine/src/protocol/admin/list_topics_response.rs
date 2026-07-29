//! Ordered bounded normalization for an all-topic generated Metadata response.

use kafka_client_core::DescribeTopicsInput;
use kafka_wire::{MetadataResponse, metadata_response::MetadataResponseTopic};

use super::{
    describe_topic_value::normalize_topic, describe_topics_budget::all_result_fits,
    describe_topics_response::DescribeTopicsProtocolFailure,
};

pub(super) fn normalize_list_topics_response(
    response: &MetadataResponse,
    retained_bytes: usize,
) -> Result<DescribeTopicsInput, DescribeTopicsProtocolFailure> {
    validate_names(response)?;
    if !all_result_fits(response, retained_bytes) {
        return Err(DescribeTopicsProtocolFailure::RetainedBytes);
    }
    let mut topics = response.topics.iter().collect::<Vec<_>>();
    topics.sort_unstable_by(|left, right| topic_bytes(left).cmp(topic_bytes(right)));
    validate_unique_names(&topics)?;
    let outcomes = topics
        .into_iter()
        .map(|topic| {
            let name = topic
                .name
                .as_ref()
                .ok_or(DescribeTopicsProtocolFailure::MissingTopicName)?;
            normalize_topic(name.as_str(), topic, false)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DescribeTopicsInput::BrokerResponded { outcomes })
}

fn validate_names(response: &MetadataResponse) -> Result<(), DescribeTopicsProtocolFailure> {
    for topic in &response.topics {
        let Some(name) = &topic.name else {
            return Err(DescribeTopicsProtocolFailure::MissingTopicName);
        };
        if name.as_str().is_empty() {
            return Err(DescribeTopicsProtocolFailure::EmptyTopicName);
        }
    }
    Ok(())
}

fn validate_unique_names(
    topics: &[&MetadataResponseTopic],
) -> Result<(), DescribeTopicsProtocolFailure> {
    if topics
        .windows(2)
        .any(|pair| topic_bytes(pair[0]) == topic_bytes(pair[1]))
    {
        return Err(DescribeTopicsProtocolFailure::DuplicateTopic);
    }
    Ok(())
}

fn topic_bytes(topic: &MetadataResponseTopic) -> &[u8] {
    topic
        .name
        .as_ref()
        .map_or(&[], |name| name.as_str().as_bytes())
}
