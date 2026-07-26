//! Declarative boundary for one-partition Fetch RPC ownership.

mod admission;
#[cfg(test)]
mod admission_test;
mod calls;
#[cfg(test)]
mod calls_test;
mod failure;
#[cfg(test)]
mod failure_admission_test;
#[cfg(test)]
mod failure_driver_test;
mod failure_wire;
#[cfg(test)]
mod failure_wire_decode_test;
#[cfg(test)]
mod failure_wire_encode_test;
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

pub(crate) use admission::{FetchCallAdmission, PartitionFetchRequest};
pub(crate) use calls::TrackedFetchCalls;
pub(crate) use failure::{classify_fetch_admission, classify_fetch_request_error};
pub(crate) use settlement::{
    FetchBeginSettlementError, FetchConfirmationError, FetchPoll, StaleFetchConfirmationError,
};
pub(crate) use stale::{FetchControlPending, FetchRecovery};
pub(crate) use terminal::{FetchCompletionObservation, FetchTerminal};
