//! Declarative facade for the concrete Admin `DescribeUserScramCredentials` engine owner.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub use error::{
    DescribeUserScramCredentialsAdmissionError, DescribeUserScramCredentialsAdmissionErrorKind,
};
pub use handle::{
    DescribeUserScramCredentialsAccepted, DescribeUserScramCredentialsAcceptedFaultKind,
};
pub use model::DescribeUserScramCredentialsRequest;
pub use observer::DescribeUserScramCredentialsObserver;
pub use outcome::{
    DescribeUserScramCredentialInfo, DescribeUserScramCredentialOutcome,
    DescribeUserScramCredentialsBatch, DescribeUserScramCredentialsBrokerError,
    DescribeUserScramCredentialsDeliveryStatus, DescribeUserScramCredentialsFailure,
    DescribeUserScramCredentialsFailureKind, DescribeUserScramCredentialsObserverError,
    DescribeUserScramCredentialsOutcome, DescribeUserScramCredentialsUserResult,
};

pub(crate) use error::DescribeUserScramCredentialsHostError;
pub(crate) use host::{
    DESCRIBE_USER_SCRAM_CREDENTIALS_CAPACITY, DescribeUserScramCredentialsHost,
    DescribeUserScramCredentialsTurn,
};
pub(crate) use shard::{
    DescribeUserScramCredentialsAdmissionPort, DescribeUserScramCredentialsShardLockError,
    DescribeUserScramCredentialsShardOwner, DescribeUserScramCredentialsShardWake,
    DescribeUserScramCredentialsShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
