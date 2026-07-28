//! Declarative facade for the concrete Admin `CreateAcls` engine owner.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub use error::{CreateAclsAdmissionError, CreateAclsAdmissionErrorKind};
pub use handle::{CreateAclsAccepted, CreateAclsAcceptedFaultKind};
pub use model::{CreateAclBinding, CreateAclsRequest};
pub use observer::CreateAclsObserver;
pub use outcome::{
    CreateAclBrokerError, CreateAclOutcome, CreateAclResult, CreateAclsBatch,
    CreateAclsDeliveryStatus, CreateAclsFailure, CreateAclsFailureKind, CreateAclsObserverError,
    CreateAclsOutcome,
};

pub(crate) use host::{CREATE_ACLS_CAPACITY, CreateAclsHost, CreateAclsHostError, CreateAclsTurn};
pub(crate) use shard::{
    CreateAclsAdmissionPort, CreateAclsShardLockError, CreateAclsShardOwner, CreateAclsShardWake,
    CreateAclsShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
