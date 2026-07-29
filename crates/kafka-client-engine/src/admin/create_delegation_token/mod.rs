//! Declarative facade for the concrete Admin `CreateDelegationToken` owner.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod result;
mod shard;

pub use error::{CreateDelegationTokenAdmissionError, CreateDelegationTokenAdmissionErrorKind};
pub use handle::{
    CreateDelegationTokenAccepted, CreateDelegationTokenAcceptedFaultKind,
    CreateDelegationTokenCapture,
};
pub use model::{CreateDelegationTokenPrincipal, CreateDelegationTokenRequest};
pub use observer::CreateDelegationTokenObserver;
pub use outcome::{
    CreateDelegationTokenBrokerError, CreateDelegationTokenDeliveryStatus,
    CreateDelegationTokenFailure, CreateDelegationTokenFailureKind,
    CreateDelegationTokenObserverError, CreateDelegationTokenOutcome,
};
pub use result::{CreateDelegationTokenHmac, CreateDelegationTokenResult, CreatedDelegationToken};

pub(crate) use error::CreateDelegationTokenHostError;
pub(crate) use host::{
    CREATE_DELEGATION_TOKEN_CAPACITY, CreateDelegationTokenHost, CreateDelegationTokenTurn,
};
pub(crate) use shard::{
    CreateDelegationTokenAdmissionPort, CreateDelegationTokenShardLockError,
    CreateDelegationTokenShardOwner, CreateDelegationTokenShardWake,
    CreateDelegationTokenShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
