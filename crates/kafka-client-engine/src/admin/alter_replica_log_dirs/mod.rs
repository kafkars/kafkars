//! Declarative facade for the concrete Admin `AlterReplicaLogDirs` engine owner.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub use error::{AlterReplicaLogDirsAdmissionError, AlterReplicaLogDirsAdmissionErrorKind};
pub use handle::{AlterReplicaLogDirsAccepted, AlterReplicaLogDirsAcceptedFaultKind};
pub use model::{AlterReplicaLogDirAssignment, AlterReplicaLogDirsRequest};
pub use observer::AlterReplicaLogDirsObserver;
pub use outcome::{
    AlterReplicaLogDirEngineBrokerError, AlterReplicaLogDirEngineOutcome,
    AlterReplicaLogDirEngineResult, AlterReplicaLogDirsDeliveryStatus,
    AlterReplicaLogDirsEngineBatch, AlterReplicaLogDirsFailure, AlterReplicaLogDirsFailureKind,
    AlterReplicaLogDirsObserverError, AlterReplicaLogDirsOutcome,
};

pub(crate) use error::AlterReplicaLogDirsHostError;
pub(crate) use host::{
    ALTER_REPLICA_LOG_DIRS_CAPACITY, AlterReplicaLogDirsHost, AlterReplicaLogDirsTurn,
};
pub(crate) use shard::{
    AlterReplicaLogDirsAdmissionPort, AlterReplicaLogDirsShardLockError,
    AlterReplicaLogDirsShardOwner, AlterReplicaLogDirsShardWake, AlterReplicaLogDirsShardWakeError,
};

#[cfg(test)]
mod error_test;
#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
