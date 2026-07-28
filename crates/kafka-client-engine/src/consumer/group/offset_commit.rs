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
pub(in crate::consumer) use host::AcceptedGroupOffsetCommit;
pub(in crate::consumer::group) use host::{GroupOffsetCommitHost, GroupOffsetCommitTurn};

impl AcceptedGroupOffsetCommit {
    pub(in crate::consumer) const fn host_faulted(&self) -> bool {
        self.fault.is_some()
    }

    pub(in crate::consumer) fn into_observer(
        self,
    ) -> crate::completion::CompletionObserver<kafka_client_core::GroupOffsetCommitTerminal> {
        self.observer
    }
}

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
