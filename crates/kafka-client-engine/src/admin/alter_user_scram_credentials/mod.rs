//! Declarative facade for the concrete Admin `AlterUserScramCredentials` owner.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub use error::{
    AlterUserScramCredentialsAdmissionError, AlterUserScramCredentialsAdmissionErrorKind,
};
pub use handle::{
    AlterUserScramCredentialsAccepted, AlterUserScramCredentialsAcceptedFaultKind,
    AlterUserScramCredentialsCapture,
};
pub use model::{AlterUserScramCredential, AlterUserScramCredentialsRequest};
pub use observer::AlterUserScramCredentialsObserver;
pub use outcome::{
    AlterUserScramCredentialBrokerError, AlterUserScramCredentialOutcome,
    AlterUserScramCredentialsBatch, AlterUserScramCredentialsDeliveryStatus,
    AlterUserScramCredentialsFailure, AlterUserScramCredentialsFailureKind,
    AlterUserScramCredentialsObserverError, AlterUserScramCredentialsOutcome,
};

pub(crate) use error::AlterUserScramCredentialsHostError;
pub(crate) use host::{
    ALTER_USER_SCRAM_CREDENTIALS_CAPACITY, AlterUserScramCredentialsHost,
    AlterUserScramCredentialsTurn,
};
pub(crate) use shard::{
    AlterUserScramCredentialsAdmissionPort, AlterUserScramCredentialsShardLockError,
    AlterUserScramCredentialsShardOwner, AlterUserScramCredentialsShardWake,
    AlterUserScramCredentialsShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
