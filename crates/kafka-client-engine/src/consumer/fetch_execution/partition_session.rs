//! Per-partition Fetch-session binding retained for legacy execution paths.

use kafka_client_core::{FetchFence, PositionFence};

use crate::protocol::fetch::{FetchSessionRequest, FetchSessionUpdate};

use super::executor::DirectFetchExecutor;

#[derive(Clone, Copy)]
pub(super) struct DirectFetchSession {
    pub(super) position: PositionFence,
    pub(super) metadata: FetchSessionRequest,
}

impl DirectFetchExecutor {
    pub(super) fn bind_fetch_session(&self, request: &mut crate::driver::PartitionFetchRequest) {
        if self.session_capacity == 0 {
            request.bind_session(FetchSessionRequest::LEGACY);
            return;
        }
        let position = request.fence().position();
        let metadata = self
            .sessions
            .iter()
            .find(|session| session.position == position)
            .map_or(FetchSessionRequest::INITIAL, |session| session.metadata);
        request.bind_session(metadata);
    }

    pub(super) fn commit_fetch_session(&mut self, fence: FetchFence, update: FetchSessionUpdate) {
        if self.session_capacity == 0 {
            return;
        }
        let position = fence.position();
        if let Some(index) = self
            .sessions
            .iter()
            .position(|session| session.position.partition() == position.partition())
        {
            match update {
                FetchSessionUpdate::Reset => {
                    self.sessions.swap_remove(index);
                }
                FetchSessionUpdate::Continue(metadata) => {
                    self.sessions[index] = DirectFetchSession { position, metadata };
                }
            }
            return;
        }
        if let FetchSessionUpdate::Continue(metadata) = update
            && self.sessions.len() < self.session_capacity
        {
            self.sessions
                .push(DirectFetchSession { position, metadata });
        }
    }
}
