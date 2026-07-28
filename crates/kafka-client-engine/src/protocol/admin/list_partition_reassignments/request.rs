//! Nullable-selection request construction for `ListPartitionReassignments` v0.

use kafka_client_core::{ListPartitionReassignmentsPlan, ListPartitionReassignmentsSelection};
use kafka_wire::{
    ListPartitionReassignmentsRequest,
    list_partition_reassignments_request::ListPartitionReassignmentsTopics,
};

/// Builds one controller query without acquiring routing or retry policy.
pub(crate) fn list_partition_reassignments_request(
    plan: &ListPartitionReassignmentsPlan,
    timeout_ms: i32,
) -> ListPartitionReassignmentsRequest {
    let topics = match plan.selection() {
        ListPartitionReassignmentsSelection::AllActive => None,
        ListPartitionReassignmentsSelection::Selected(targets) => {
            let mut topics: Vec<ListPartitionReassignmentsTopics> = Vec::new();
            for target in targets {
                if let Some(topic) = topics
                    .iter_mut()
                    .find(|topic| topic.name.as_str() == target.topic())
                {
                    topic.partition_indexes.push(target.partition());
                } else {
                    let mut topic = ListPartitionReassignmentsTopics::default();
                    topic.name = target.topic().into();
                    topic.partition_indexes.push(target.partition());
                    topics.push(topic);
                }
            }
            Some(topics)
        }
    };
    let mut request = ListPartitionReassignmentsRequest::default();
    request.timeout_ms = timeout_ms;
    request.topics = topics;
    request
}
