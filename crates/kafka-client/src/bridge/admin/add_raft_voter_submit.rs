//! Capture-first admission handoff for public Admin `AddRaftVoter`.

use std::time::Duration;

use super::AdminEngine;
use crate::bridge::add_raft_voter::{
    AddRaftVoterAdminRequest, AdminAddRaftVoter, translate_request,
};

impl AdminEngine {
    pub(crate) fn submit_add_raft_voter(
        &self,
        request: AddRaftVoterAdminRequest,
        timeout: Duration,
    ) -> AdminAddRaftVoter {
        let capture = match self.handle.capture_add_raft_voter(timeout) {
            Ok(capture) => capture,
            Err(error) => return AdminAddRaftVoter::from_admission(Err(error)),
        };
        let request = translate_request(request);
        AdminAddRaftVoter::from_admission(capture.try_submit(request))
    }
}
