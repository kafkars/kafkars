//! Generated Metadata request construction for transient topic descriptions.

use kafka_client_core::{DescribeTopicsPlan, DescribeTopicsSelection};
use kafka_wire::{MetadataRequest, metadata_request::MetadataRequestTopic};
use kafka_wire_core::Uuid;

/// Builds one explicit topic selection without acquiring metadata-cache authority.
///
/// Disabling auto-creation is deliberate. On brokers whose Metadata API is
/// older than v4, generated encoding rejects this unrepresentable policy before
/// any bytes are written rather than allowing an observation to create a topic.
pub(crate) fn describe_topics_request(plan: &DescribeTopicsPlan) -> MetadataRequest {
    let topics = match plan.selection() {
        DescribeTopicsSelection::Named(topics) => Some(
            topics
                .iter()
                .map(|topic| {
                    let mut requested = MetadataRequestTopic::default();
                    requested.name = Some(topic.as_str().into());
                    requested
                })
                .collect(),
        ),
        DescribeTopicsSelection::Ids(topic_ids) => Some(
            topic_ids
                .iter()
                .map(|topic_id| {
                    let mut requested = MetadataRequestTopic::default();
                    requested.topic_id = Uuid::from_bytes(*topic_id);
                    requested.name = None;
                    requested
                })
                .collect(),
        ),
        DescribeTopicsSelection::All { .. } => None,
    };
    let mut request = MetadataRequest::default();
    request.topics = topics;
    request.allow_auto_topic_creation = false;
    request.include_cluster_authorized_operations = false;
    request.include_topic_authorized_operations = plan.include_authorized_operations();
    request
}
