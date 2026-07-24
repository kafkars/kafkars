//! Name-based generated Metadata request construction for transient topic descriptions.

use kafka_wire::{MetadataRequest, metadata_request::MetadataRequestTopic};

/// Builds one batched name query without acquiring metadata-cache authority.
///
/// Disabling auto-creation is deliberate. On brokers whose Metadata API is
/// older than v4, generated encoding rejects this unrepresentable policy before
/// any bytes are written rather than allowing an observation to create a topic.
pub(crate) fn describe_topics_request<'a>(
    topics: impl IntoIterator<Item = &'a str>,
) -> MetadataRequest {
    let topics = topics
        .into_iter()
        .map(|topic| {
            let mut requested = MetadataRequestTopic::default();
            requested.name = Some(topic.into());
            requested
        })
        .collect();
    let mut request = MetadataRequest::default();
    request.topics = Some(topics);
    request.allow_auto_topic_creation = false;
    request.include_cluster_authorized_operations = false;
    request.include_topic_authorized_operations = false;
    request
}
