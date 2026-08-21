//! Capture-after public request storage and public-to-engine voter conversion.

use crate::admin::RaftVoterIdentity;

use super::engine::Request;

/// Inert facade values retained without engine conversion before submission.
#[derive(Debug)]
pub(crate) struct RemoveRaftVoterAdminRequest {
    cluster_id: Option<String>,
    identity: RaftVoterIdentity,
}

impl RemoveRaftVoterAdminRequest {
    pub(crate) const fn new(identity: RaftVoterIdentity) -> Self {
        Self {
            cluster_id: None,
            identity,
        }
    }

    pub(crate) fn with_cluster_id(mut self, cluster_id: String) -> Self {
        self.cluster_id = Some(cluster_id);
        self
    }

    pub(crate) fn into_parts(self) -> (Option<String>, RaftVoterIdentity) {
        (self.cluster_id, self.identity)
    }
}

/// Converts after the engine has captured the sole public absolute deadline.
pub(crate) fn translate_request(request: RemoveRaftVoterAdminRequest) -> Request {
    let (cluster_id, identity) = request.into_parts();
    let (voter_id, directory_id) = identity.into_parts();
    Request::new(cluster_id, voter_id, directory_id)
}
