//! Exact generated construction for one committed metadata-quorum voter addition.

use kafka_client_core::AddRaftVoterPlan;
use kafka_wire::{AddRaftVoterRequest, add_raft_voter_request::Listener};
use kafka_wire_core::{StrBytes, Uuid};

/// Local request materialization failure before driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AddRaftVoterRequestFailure {
    /// A generated Kafka request timeout must remain strictly positive.
    NonPositiveTimeout {
        /// Exact rejected timeout value.
        actual: i32,
    },
    /// Listener storage could not be reserved within the accepted operation.
    Allocation {
        /// Exact listener count requested.
        requested: usize,
    },
}

/// Builds one request that reports success only after the new voter set is committed.
pub(crate) fn add_raft_voter_request(
    plan: &AddRaftVoterPlan,
    timeout_ms: i32,
) -> Result<AddRaftVoterRequest, AddRaftVoterRequestFailure> {
    if timeout_ms <= 0 {
        return Err(AddRaftVoterRequestFailure::NonPositiveTimeout { actual: timeout_ms });
    }

    let mut listeners = Vec::new();
    listeners
        .try_reserve_exact(plan.listeners().len())
        .map_err(|_| AddRaftVoterRequestFailure::Allocation {
            requested: plan.listeners().len(),
        })?;
    listeners.extend(plan.listeners().iter().map(|endpoint| {
        let mut listener = Listener::default();
        listener.name = StrBytes::from(endpoint.name());
        listener.host = StrBytes::from(endpoint.host());
        listener.port = endpoint.port();
        listener
    }));

    let mut request = AddRaftVoterRequest::default();
    request.cluster_id = plan.cluster_id().map(StrBytes::from);
    request.timeout_ms = timeout_ms;
    request.voter_id = plan.voter_id();
    request.voter_directory_id = Uuid::from_bytes(plan.voter_directory_id());
    request.listeners = listeners;
    request.ack_when_committed = true;
    Ok(request)
}
