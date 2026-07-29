//! Declarative facade for the concrete Admin `DescribeMetadataQuorum` engine owner.

mod error;
mod handle;
mod host;
mod observer;
mod outcome;
mod shard;
mod value;

pub use error::{DescribeMetadataQuorumAdmissionError, DescribeMetadataQuorumAdmissionErrorKind};
pub use handle::{DescribeMetadataQuorumAccepted, DescribeMetadataQuorumAcceptedFaultKind};
pub use observer::DescribeMetadataQuorumObserver;
pub use outcome::{
    DescribeMetadataQuorumBrokerError, DescribeMetadataQuorumDeliveryStatus,
    DescribeMetadataQuorumFailure, DescribeMetadataQuorumFailureKind,
    DescribeMetadataQuorumObserverError, DescribeMetadataQuorumOutcome,
    DescribeMetadataQuorumPartitionError,
};
pub use value::{
    DescribeMetadataQuorumDescription, DescribeMetadataQuorumListener, DescribeMetadataQuorumNode,
    DescribeMetadataQuorumReplica,
};

pub(crate) use error::DescribeMetadataQuorumHostError;
pub(crate) use host::{
    DESCRIBE_METADATA_QUORUM_CAPACITY, DescribeMetadataQuorumHost, DescribeMetadataQuorumTurn,
};
pub(crate) use shard::{
    DescribeMetadataQuorumAdmissionPort, DescribeMetadataQuorumShardLockError,
    DescribeMetadataQuorumShardOwner, DescribeMetadataQuorumShardWake,
    DescribeMetadataQuorumShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod outcome_test;
