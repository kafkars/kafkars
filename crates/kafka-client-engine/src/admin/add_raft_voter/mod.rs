//! Declarative facade for the concrete Admin `AddRaftVoter` engine owner.

pub(crate) mod api;
mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod result;
mod shard;

pub use error::{AddRaftVoterAdmissionError, AddRaftVoterAdmissionErrorKind};
pub use handle::{AddRaftVoterAccepted, AddRaftVoterAcceptedFaultKind, AddRaftVoterCapture};
pub use model::{AddRaftVoterEndpoint, AddRaftVoterRequest};
pub use observer::AddRaftVoterObserver;
pub use outcome::{
    AddRaftVoterBrokerError, AddRaftVoterDeliveryStatus, AddRaftVoterFailure,
    AddRaftVoterFailureKind, AddRaftVoterObserverError, AddRaftVoterOutcome,
};
pub use result::AddRaftVoterResult;

pub(crate) use error::AddRaftVoterHostError;
pub(crate) use host::{ADD_RAFT_VOTER_CAPACITY, AddRaftVoterHost, AddRaftVoterTurn};
pub(crate) use shard::{
    AddRaftVoterAdmissionPort, AddRaftVoterShardLockError, AddRaftVoterShardOwner,
    AddRaftVoterShardWake, AddRaftVoterShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
