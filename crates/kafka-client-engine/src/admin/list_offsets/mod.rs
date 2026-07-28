//! Declarative facade for the concrete Admin `ListOffsets` engine owner.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub use error::{AdminListOffsetsAdmissionError, AdminListOffsetsAdmissionErrorKind};
pub use handle::{AdminListOffsetsAccepted, AdminListOffsetsAcceptedFaultKind};
pub use model::{
    AdminListOffsetsRequest, AdminListOffsetsRequestSpec, AdminListOffsetsRequestTarget,
};
pub use observer::AdminListOffsetsObserver;
pub use outcome::{
    AdminListOffsetDescription, AdminListOffsetEngineBrokerError, AdminListOffsetEngineResult,
    AdminListOffsetsDeliveryStatus, AdminListOffsetsEngineBatch, AdminListOffsetsFailure,
    AdminListOffsetsFailureKind, AdminListOffsetsObserverError, AdminListOffsetsOutcome,
};

pub(crate) use error::AdminListOffsetsHostError;
pub(crate) use host::{ADMIN_LIST_OFFSETS_CAPACITY, AdminListOffsetsHost, AdminListOffsetsTurn};
pub(crate) use shard::{
    AdminListOffsetsAdmissionPort, AdminListOffsetsShardLockError, AdminListOffsetsShardOwner,
    AdminListOffsetsShardWake, AdminListOffsetsShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
