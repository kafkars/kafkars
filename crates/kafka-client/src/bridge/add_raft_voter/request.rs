//! Capture-after public request storage and public-to-engine voter conversion.

use crate::admin::{RaftVoterEndpoint, RaftVoterIdentity};

use super::engine::{Endpoint, Request};

/// Inert facade values retained without engine conversion before submission.
#[derive(Debug)]
pub(crate) struct AddRaftVoterAdminRequest {
    cluster_id: Option<String>,
    identity: RaftVoterIdentity,
    endpoints: Vec<RaftVoterEndpoint>,
    ack_when_committed: bool,
}

impl AddRaftVoterAdminRequest {
    pub(crate) const fn new(
        identity: RaftVoterIdentity,
        endpoints: Vec<RaftVoterEndpoint>,
    ) -> Self {
        Self {
            cluster_id: None,
            identity,
            endpoints,
            ack_when_committed: true,
        }
    }

    pub(crate) fn with_cluster_id(mut self, cluster_id: String) -> Self {
        self.cluster_id = Some(cluster_id);
        self
    }

    pub(crate) const fn with_ack_when_committed(mut self, ack_when_committed: bool) -> Self {
        self.ack_when_committed = ack_when_committed;
        self
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Option<String>,
        RaftVoterIdentity,
        Vec<RaftVoterEndpoint>,
        bool,
    ) {
        (
            self.cluster_id,
            self.identity,
            self.endpoints,
            self.ack_when_committed,
        )
    }
}

/// Converts after the engine has captured the sole public absolute deadline.
pub(crate) fn translate_request(request: AddRaftVoterAdminRequest) -> Request {
    let (cluster_id, identity, endpoints, ack_when_committed) = request.into_parts();
    let (voter_id, directory_id) = identity.into_parts();
    let endpoints = endpoints
        .into_iter()
        .map(|endpoint| {
            let (listener, host, port) = endpoint.into_parts();
            Endpoint::new(listener, host, port)
        })
        .collect();
    Request::new(cluster_id, voter_id, directory_id, endpoints)
        .ack_when_committed(ack_when_committed)
}
