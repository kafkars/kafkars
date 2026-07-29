//! Generated API-key 80 adaptation for one committed metadata-quorum voter addition.

mod model;
mod request;
mod response;
mod retention;

pub(crate) use super::request_timeout_error::{
    AdminRequestDeadlineError as AddRaftVoterDeadlineError, remaining_timeout_ms,
};
pub(crate) use model::NormalizedAddRaftVoterResponse;
pub(crate) use request::{AddRaftVoterRequestFailure, add_raft_voter_request};
pub(crate) use response::{AddRaftVoterResponseFailure, normalize_add_raft_voter_response};
#[cfg(test)]
pub(crate) use retention::ADD_RAFT_VOTER_MAX_RETAINED_BYTES;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
