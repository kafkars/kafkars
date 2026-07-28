//! Declarative facade for leader-election alteration ownership.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub(crate) use error::ElectLeadersHostError;
pub use error::{ElectLeadersAdmissionError, ElectLeadersAdmissionErrorKind};
pub use handle::{ElectLeadersAccepted, ElectLeadersAcceptedFaultKind};
pub(crate) use host::{ELECT_LEADERS_CAPACITY, ElectLeadersHost, ElectLeadersTurn};
pub use model::{ElectLeadersRequest, LeaderElectionTarget, LeaderElectionType};
pub use observer::ElectLeadersObserver;
pub use outcome::{
    ElectLeadersBatch, ElectLeadersDeliveryStatus, ElectLeadersFailure, ElectLeadersFailureKind,
    ElectLeadersObserverError, ElectLeadersOutcome, LeaderElectionBrokerError,
    LeaderElectionResult,
};
pub(crate) use shard::{
    ElectLeadersAdmissionPort, ElectLeadersShardLockError, ElectLeadersShardOwner,
    ElectLeadersShardWake, ElectLeadersShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
