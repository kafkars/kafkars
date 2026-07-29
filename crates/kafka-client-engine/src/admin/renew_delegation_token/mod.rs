//! Concrete bounded engine ownership for Kafka API key 39.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod result;
mod shard;

pub use error::{RenewDelegationTokenAdmissionError, RenewDelegationTokenAdmissionErrorKind};
pub use handle::{
    RenewDelegationTokenAccepted, RenewDelegationTokenAcceptedFaultKind,
    RenewDelegationTokenCapture,
};
pub use model::{RenewDelegationTokenHmac, RenewDelegationTokenRequest};
pub use observer::RenewDelegationTokenObserver;
pub use outcome::{
    RenewDelegationTokenBrokerError, RenewDelegationTokenDeliveryStatus,
    RenewDelegationTokenFailure, RenewDelegationTokenFailureKind,
    RenewDelegationTokenObserverError, RenewDelegationTokenOutcome,
};
pub use result::RenewDelegationTokenResult;

pub(crate) use error::RenewDelegationTokenHostError;
pub(crate) use host::{
    RENEW_DELEGATION_TOKEN_CAPACITY, RenewDelegationTokenHost, RenewDelegationTokenTurn,
};
pub(crate) use shard::{
    RenewDelegationTokenAdmissionPort, RenewDelegationTokenShardLockError,
    RenewDelegationTokenShardOwner, RenewDelegationTokenShardWake,
    RenewDelegationTokenShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod result_test;
