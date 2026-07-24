//! Concrete name-based `DescribeTopics` ownership domain.
mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;
pub use error::{DescribeTopicsAdmissionError, DescribeTopicsAdmissionErrorKind};
pub use handle::{DescribeTopicsAccepted, DescribeTopicsAcceptedFaultKind};
pub(crate) use host::{
    DESCRIBE_TOPICS_CAPACITY, DescribeTopicsHost, DescribeTopicsHostError, DescribeTopicsTurn,
};
pub use model::DescribeTopicsRequest;
pub use observer::DescribeTopicsObserver;
pub use outcome::{
    DescribeTopicError, DescribeTopicResult, DescribeTopicsDeliveryStatus, DescribeTopicsFailure,
    DescribeTopicsFailureKind, DescribeTopicsObserverError, DescribeTopicsOutcome,
    TopicDescription, TopicPartitionDescription,
};
pub(crate) use shard::{
    DescribeTopicsAdmissionPort, DescribeTopicsShardLockError, DescribeTopicsShardOwner,
    DescribeTopicsShardWake, DescribeTopicsShardWakeError,
};
#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod shard_test;
