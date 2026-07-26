//! Concrete explicit-selection `DescribeTopics` ownership domain.
mod error;
mod handle;
mod host;
mod limits;
mod model;
mod observer;
mod outcome;
mod shard;
pub use error::{DescribeTopicsAdmissionError, DescribeTopicsAdmissionErrorKind};
pub use handle::{DescribeTopicsAccepted, DescribeTopicsAcceptedFaultKind};
pub(crate) use host::{DescribeTopicsHost, DescribeTopicsHostError, DescribeTopicsTurn};
pub(crate) use limits::DESCRIBE_TOPICS_CAPACITY;
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
mod limits_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod shard_test;
