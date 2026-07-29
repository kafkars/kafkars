//! Declarative facade for the concrete Admin `DescribeTopicPartitions` engine owner.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub use error::{
    AdminDescribeTopicPartitionsAdmissionError, AdminDescribeTopicPartitionsAdmissionErrorKind,
};
pub use handle::{
    AdminDescribeTopicPartitionsAccepted, AdminDescribeTopicPartitionsAcceptedFaultKind,
};
pub use model::{AdminDescribeTopicPartitionsCursor, AdminDescribeTopicPartitionsRequest};
pub use observer::AdminDescribeTopicPartitionsObserver;
pub use outcome::{
    AdminDescribeTopicPartition, AdminDescribeTopicPartitionsDeliveryStatus,
    AdminDescribeTopicPartitionsFailure, AdminDescribeTopicPartitionsFailureKind,
    AdminDescribeTopicPartitionsObserverError, AdminDescribeTopicPartitionsOutcome,
    AdminDescribeTopicPartitionsPage, AdminDescribeTopicPartitionsTopic,
};

pub(crate) use error::AdminDescribeTopicPartitionsHostError;
pub(crate) use host::{
    ADMIN_DESCRIBE_TOPIC_PARTITIONS_CAPACITY, AdminDescribeTopicPartitionsHost,
    AdminDescribeTopicPartitionsTurn,
};
pub(crate) use shard::{
    AdminDescribeTopicPartitionsAdmissionPort, AdminDescribeTopicPartitionsShardLockError,
    AdminDescribeTopicPartitionsShardOwner, AdminDescribeTopicPartitionsShardWake,
    AdminDescribeTopicPartitionsShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
