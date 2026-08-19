//! Engine-owned inert metadata-quorum voter-removal intent.

use kafka_client_core::RemoveRaftVoterPlan;

/// One inert exactly identified voter-removal request.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(
    clippy::struct_field_names,
    reason = "the field names preserve Kafka's distinct cluster, voter, and directory identities"
)]
pub struct RemoveRaftVoterRequest {
    cluster_id: Option<String>,
    voter_id: i32,
    voter_directory_id: [u8; 16],
}

impl RemoveRaftVoterRequest {
    /// Creates inert request data validated only after deadline capture.
    pub const fn new(
        cluster_id: Option<String>,
        voter_id: i32,
        voter_directory_id: [u8; 16],
    ) -> Self {
        Self {
            cluster_id,
            voter_id,
            voter_directory_id,
        }
    }

    /// Returns Kafka's nullable cluster identity.
    pub fn cluster_id(&self) -> Option<&str> {
        self.cluster_id.as_deref()
    }

    /// Returns Kafka's signed voter identity.
    pub const fn voter_id(&self) -> i32 {
        self.voter_id
    }

    /// Returns the exact storage-directory UUID bytes.
    pub const fn voter_directory_id(&self) -> [u8; 16] {
        self.voter_directory_id
    }

    pub(crate) fn into_plan(self) -> Result<RemoveRaftVoterPlan, ()> {
        RemoveRaftVoterPlan::new(self.cluster_id, self.voter_id, self.voter_directory_id)
            .map_err(|_error| ())
    }
}
