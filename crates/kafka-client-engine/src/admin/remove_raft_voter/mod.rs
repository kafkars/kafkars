//! Declarative facade for the concrete Admin `RemoveRaftVoter` engine owner.

pub(crate) mod api;
mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod result;
mod shard;

pub use error::{RemoveRaftVoterAdmissionError, RemoveRaftVoterAdmissionErrorKind};
pub use handle::{
    RemoveRaftVoterAccepted, RemoveRaftVoterAcceptedFaultKind, RemoveRaftVoterCapture,
};
pub use model::RemoveRaftVoterRequest;
pub use observer::RemoveRaftVoterObserver;
pub use outcome::{
    RemoveRaftVoterBrokerError, RemoveRaftVoterDeliveryStatus, RemoveRaftVoterFailure,
    RemoveRaftVoterFailureKind, RemoveRaftVoterObserverError, RemoveRaftVoterOutcome,
};
pub use result::RemoveRaftVoterResult;

pub(crate) use error::RemoveRaftVoterHostError;
pub(crate) use host::{REMOVE_RAFT_VOTER_CAPACITY, RemoveRaftVoterHost, RemoveRaftVoterTurn};
pub(crate) use shard::{
    RemoveRaftVoterAdmissionPort, RemoveRaftVoterShardLockError, RemoveRaftVoterShardOwner,
    RemoveRaftVoterShardWake, RemoveRaftVoterShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
