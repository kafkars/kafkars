//! Exact generated construction for one metadata-quorum voter removal.

use kafka_client_core::RemoveRaftVoterPlan;
use kafka_wire::RemoveRaftVoterRequest;
use kafka_wire_core::{StrBytes, Uuid};

/// Builds the sole v0 request without inventing a Kafka-side timeout.
pub(crate) fn remove_raft_voter_request(plan: &RemoveRaftVoterPlan) -> RemoveRaftVoterRequest {
    let mut request = RemoveRaftVoterRequest::default();
    request.cluster_id = plan.cluster_id().map(StrBytes::from);
    request.voter_id = plan.voter_id();
    request.voter_directory_id = Uuid::from_bytes(plan.voter_directory_id());
    request
}
