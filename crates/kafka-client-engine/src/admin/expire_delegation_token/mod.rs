//! Concrete bounded engine ownership for Kafka API key 40.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod result;
mod shard;

pub use error::{ExpireDelegationTokenAdmissionError, ExpireDelegationTokenAdmissionErrorKind};
pub use handle::{
    ExpireDelegationTokenAccepted, ExpireDelegationTokenAcceptedFaultKind,
    ExpireDelegationTokenCapture,
};
pub use model::{ExpireDelegationTokenHmac, ExpireDelegationTokenRequest};
pub use observer::ExpireDelegationTokenObserver;
pub use outcome::{
    ExpireDelegationTokenBrokerError, ExpireDelegationTokenDeliveryStatus,
    ExpireDelegationTokenFailure, ExpireDelegationTokenFailureKind,
    ExpireDelegationTokenObserverError, ExpireDelegationTokenOutcome,
};
pub use result::ExpireDelegationTokenResult;

pub(crate) use error::ExpireDelegationTokenHostError;
pub(crate) use host::{
    EXPIRE_DELEGATION_TOKEN_CAPACITY, ExpireDelegationTokenHost, ExpireDelegationTokenTurn,
};
pub(crate) use shard::{
    ExpireDelegationTokenAdmissionPort, ExpireDelegationTokenShardLockError,
    ExpireDelegationTokenShardOwner, ExpireDelegationTokenShardWake,
    ExpireDelegationTokenShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod result_test;
