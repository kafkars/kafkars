//! Bounded coordinator-rejection replacement policy for classic group commits.

mod candidate;
mod state;

pub(crate) use candidate::GroupOffsetCommitReplacementPoll;
pub(super) use candidate::{
    GroupOffsetCommitRetryCandidate, classify_group_offset_commit_settlement,
};
pub(crate) use state::{
    GroupOffsetCommitBeginError, GroupOffsetCommitConfirmationError, GroupOffsetCommitPoll,
    GroupOffsetCommitRefreshPoll, GroupOffsetCommitRestoreError, GroupOffsetCommitRestoreFailure,
};
pub(super) use state::{RouteTokenDestination, route_token_destination};

#[cfg(test)]
mod candidate_test;
#[cfg(test)]
mod state_test;

#[cfg(test)]
pub(super) use state_test::broker_rejection;
