//! Deterministic ownership of one explicit API-key 75 topic-partition page.

mod machine;
mod model;
mod outcome;
mod page;
mod partition;
mod topic;
mod transition;
mod value_error;

pub use machine::{
    DescribeTopicPartitionsEffect, DescribeTopicPartitionsInput, DescribeTopicPartitionsMachine,
    DescribeTopicPartitionsMachineError, DescribeTopicPartitionsState,
    DescribeTopicPartitionsTransition,
};
pub use model::{
    DESCRIBE_TOPIC_PARTITIONS_MAX_REQUEST_TOPIC_BYTES,
    DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS, DESCRIBE_TOPIC_PARTITIONS_MAX_TOPICS,
    DescribeTopicPartitionsCursor, DescribeTopicPartitionsPlan, DescribeTopicPartitionsPlanError,
};
pub use outcome::{
    DescribeTopicPartitionsDeliveryStatus, DescribeTopicPartitionsFailure,
    DescribeTopicPartitionsFailureKind, DescribeTopicPartitionsTerminal,
};
pub use page::{
    DESCRIBE_TOPIC_PARTITIONS_MAX_BROKER_REFERENCES,
    DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_TOPIC_BYTES,
    DESCRIBE_TOPIC_PARTITIONS_MAX_RETAINED_BYTES, DescribeTopicPartitionsPage,
};
pub use partition::DescribeTopicPartition;
pub use topic::DescribeTopicPartitionsTopic;
pub use value_error::DescribeTopicPartitionsValueError;

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod page_test;
#[cfg(test)]
mod value_test;
