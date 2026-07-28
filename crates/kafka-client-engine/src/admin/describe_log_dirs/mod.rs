//! Declarative facade for the concrete Admin `DescribeLogDirs` engine owner.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub use error::{DescribeLogDirsAdmissionError, DescribeLogDirsAdmissionErrorKind};
pub use handle::{DescribeLogDirsAccepted, DescribeLogDirsAcceptedFaultKind};
pub use model::DescribeLogDirsRequest;
pub use observer::DescribeLogDirsObserver;
pub use outcome::{
    DescribeLogDirDescription, DescribeLogDirEngineBrokerError, DescribeLogDirEngineOutcome,
    DescribeLogDirsBrokerFailure, DescribeLogDirsBrokerFailureKind, DescribeLogDirsDeliveryStatus,
    DescribeLogDirsEngineBatch, DescribeLogDirsEngineBrokerOutcome,
    DescribeLogDirsEngineBrokerResult, DescribeLogDirsFailure, DescribeLogDirsObserverError,
    DescribeLogDirsOutcome, DescribeLogDirsReplicaInfo,
};

pub(crate) use error::DescribeLogDirsHostError;
pub(crate) use host::{DESCRIBE_LOG_DIRS_CAPACITY, DescribeLogDirsHost, DescribeLogDirsTurn};
pub(crate) use shard::{
    DescribeLogDirsAdmissionPort, DescribeLogDirsShardLockError, DescribeLogDirsShardOwner,
    DescribeLogDirsShardWake, DescribeLogDirsShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
