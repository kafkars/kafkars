//! Private bounded composition of classic-group offset commit ownership.

mod admission;
mod error;
mod host;
mod notifier_lifecycle;
mod preparation;
mod preparation_failure;
mod publication;
mod recovery;
mod recovery_replay;
mod rollback;
mod settlement;
mod snapshot;
mod turn;

pub(in crate::consumer::group) use admission::{
    GroupOffsetCommitAdmissionFailure, GroupOffsetCommitAdmissionFailureKind,
};
pub(in crate::consumer::group) use error::GroupOffsetCommitHostError;
pub(in crate::consumer::group) use host::{
    AcceptedGroupOffsetCommit, GroupOffsetCommitHost, GroupOffsetCommitTurn,
};

#[cfg(test)]
mod admission_test;
#[cfg(test)]
mod error_test;
#[cfg(test)]
mod host_test;
#[cfg(test)]
mod notifier_lifecycle_test;
#[cfg(test)]
mod preparation_failure_test;
#[cfg(test)]
mod preparation_test;
#[cfg(test)]
mod publication_test;
#[cfg(test)]
mod recovery_replay_test;
#[cfg(test)]
mod recovery_test;
#[cfg(test)]
mod rollback_test;
#[cfg(test)]
mod settlement_test;
#[cfg(test)]
mod snapshot_test;
#[cfg(test)]
pub(in crate::consumer::group) mod test_support;
#[cfg(test)]
mod turn_test;
