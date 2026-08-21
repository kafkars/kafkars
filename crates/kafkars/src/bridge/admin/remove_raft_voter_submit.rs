//! Capture-first admission handoff for public Admin `RemoveRaftVoter`.

use std::time::Duration;

use super::AdminEngine;
use crate::bridge::remove_raft_voter::{
    AdminRemoveRaftVoter, RemoveRaftVoterAdminRequest, translate_request,
};

impl AdminEngine {
    pub(crate) fn submit_remove_raft_voter(
        &self,
        request: RemoveRaftVoterAdminRequest,
        timeout: Duration,
    ) -> AdminRemoveRaftVoter {
        let capture = match self.handle.capture_remove_raft_voter(timeout) {
            Ok(capture) => capture,
            Err(error) => return AdminRemoveRaftVoter::from_admission(Err(error)),
        };
        let request = translate_request(request);
        AdminRemoveRaftVoter::from_admission(capture.try_submit(request))
    }
}
