//! Declarative boundary for one-partition Fetch RPC ownership.

mod admission;
#[cfg(test)]
mod admission_test;
mod calls;
#[cfg(test)]
mod calls_test;
mod fence;
#[cfg(test)]
mod fence_test;
#[cfg(test)]
mod routed_response_broker_test;
#[cfg(test)]
mod routed_response_test;
mod settlement;
mod settlement_owner;
#[cfg(test)]
mod settlement_owner_test;
#[cfg(test)]
mod settlement_test;
mod stale;
#[cfg(test)]
mod stale_test;
mod submission;
#[cfg(test)]
mod submission_test;
mod terminal;
#[cfg(test)]
mod terminal_test;

pub(crate) use admission::{
    FetchAdmissionFailure, FetchAdmissionFailureSource, FetchCallAdmission,
    FetchRequestPreparationError, PartitionFetchRequest,
};
pub(crate) use calls::TrackedFetchCalls;
pub(crate) use settlement::{
    FetchBeginSettlementError, FetchConfirmationError, FetchPoll, FetchRestoreError,
    FetchRestoreFailure, StaleFetchConfirmationError,
};
pub(crate) use stale::{FetchControlPending, FetchRecovery, StaleFetchDrains};
pub(crate) use terminal::{FetchCompletionFailure, FetchCompletionObservation, FetchTerminal};
