//! Declarative value facade for Admin `DeleteAcls`.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub use error::{DeleteAclsAdmissionError, DeleteAclsAdmissionErrorKind};
pub use handle::{DeleteAclsAccepted, DeleteAclsAcceptedFaultKind};
pub use model::{DeleteAclsFilter, DeleteAclsRequest};
pub use observer::DeleteAclsObserver;
pub use outcome::{
    DeleteAclBrokerError, DeleteAclFilterOutcome, DeleteAclFilterResult, DeleteAclMatchResult,
    DeleteAclMatchingBinding, DeleteAclsBatch, DeleteAclsDeliveryStatus, DeleteAclsFailure,
    DeleteAclsFailureKind, DeleteAclsObserverError, DeleteAclsOutcome,
};

pub(crate) use host::{DELETE_ACLS_CAPACITY, DeleteAclsHost, DeleteAclsHostError, DeleteAclsTurn};
pub(crate) use outcome::{
    DeleteAclsPreparedOutcomes, DeleteAclsTranslationError, translate_terminal_into,
};
pub(crate) use shard::{
    DeleteAclsAdmissionPort, DeleteAclsShardLockError, DeleteAclsShardOwner, DeleteAclsShardWake,
    DeleteAclsShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_failure_test;
#[cfg(test)]
mod outcome_test;
