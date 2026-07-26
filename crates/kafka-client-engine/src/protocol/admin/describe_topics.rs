//! Generated Metadata request construction for transient topic descriptions.

use kafka_client_core::{DescribeTopicsPlan, DescribeTopicsSelection};
use kafka_wire::{MetadataRequest, metadata_request::MetadataRequestTopic};

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
        DescribeTopicsSelection::All { .. } => None,
    };
    let mut request = MetadataRequest::default();
    request.topics = topics;
    request.allow_auto_topic_creation = false;
    request.include_cluster_authorized_operations = false;
    request.include_topic_authorized_operations = false;
    request
}
