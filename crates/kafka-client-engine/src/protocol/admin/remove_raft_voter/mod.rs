//! Generated API-key 81 adaptation for one metadata-quorum voter removal.

mod model;
mod request;
mod response;
mod retention;

pub(crate) use model::NormalizedRemoveRaftVoterResponse;
pub(crate) use request::remove_raft_voter_request;
pub(crate) use response::{RemoveRaftVoterResponseFailure, normalize_remove_raft_voter_response};
#[cfg(test)]
pub(crate) use retention::REMOVE_RAFT_VOTER_MAX_RETAINED_BYTES;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
