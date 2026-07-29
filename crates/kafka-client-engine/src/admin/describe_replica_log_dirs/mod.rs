//! Declarative facade for the concrete Admin `DescribeReplicaLogDirs` engine owner.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub use error::{DescribeReplicaLogDirsAdmissionError, DescribeReplicaLogDirsAdmissionErrorKind};
pub use handle::{
    DescribeReplicaLogDirsAccepted, DescribeReplicaLogDirsAcceptedFaultKind,
    DescribeReplicaLogDirsCapture,
};
pub use model::{DescribeReplicaLogDirsRequest, DescribeReplicaLogDirsTarget};
pub use observer::DescribeReplicaLogDirsObserver;
pub use outcome::{
    DescribeReplicaLogDirsBrokerError, DescribeReplicaLogDirsDeliveryStatus,
    DescribeReplicaLogDirsEngineBatch, DescribeReplicaLogDirsEngineReplicaOutcome,
    DescribeReplicaLogDirsEngineReplicaResult, DescribeReplicaLogDirsFailure,
    DescribeReplicaLogDirsFailureKind, DescribeReplicaLogDirsObserverError,
    DescribeReplicaLogDirsOutcome, ReplicaLogDirInfo, ReplicaLogDirLocation,
};

pub(crate) use error::DescribeReplicaLogDirsHostError;
pub(crate) use host::{
    DESCRIBE_REPLICA_LOG_DIRS_CAPACITY, DescribeReplicaLogDirsHost, DescribeReplicaLogDirsTurn,
};
pub(crate) use shard::{
    DescribeReplicaLogDirsAdmissionPort, DescribeReplicaLogDirsShardLockError,
    DescribeReplicaLogDirsShardOwner, DescribeReplicaLogDirsShardWake,
    DescribeReplicaLogDirsShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
