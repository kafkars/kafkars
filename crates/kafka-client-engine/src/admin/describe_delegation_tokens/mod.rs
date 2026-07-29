//! Declarative facade for the concrete Admin `DescribeDelegationTokens` owner.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod result;
mod shard;

pub use error::{
    DescribeDelegationTokensAdmissionError, DescribeDelegationTokensAdmissionErrorKind,
};
pub use handle::{
    DescribeDelegationTokensAccepted, DescribeDelegationTokensAcceptedFaultKind,
    DescribeDelegationTokensCapture,
};
pub use model::{DescribeDelegationTokenPrincipal, DescribeDelegationTokensRequest};
pub use observer::DescribeDelegationTokensObserver;
pub use outcome::{
    DescribeDelegationTokensBrokerError, DescribeDelegationTokensDeliveryStatus,
    DescribeDelegationTokensFailure, DescribeDelegationTokensFailureKind,
    DescribeDelegationTokensObserverError, DescribeDelegationTokensOutcome,
};
pub use result::{
    DescribeDelegationTokenHmac, DescribeDelegationTokensResult, DescribedDelegationToken,
};

pub(crate) use error::DescribeDelegationTokensHostError;
pub(crate) use host::{
    DESCRIBE_DELEGATION_TOKENS_CAPACITY, DescribeDelegationTokensHost, DescribeDelegationTokensTurn,
};
pub(crate) use shard::{
    DescribeDelegationTokensAdmissionPort, DescribeDelegationTokensShardLockError,
    DescribeDelegationTokensShardOwner, DescribeDelegationTokensShardWake,
    DescribeDelegationTokensShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
