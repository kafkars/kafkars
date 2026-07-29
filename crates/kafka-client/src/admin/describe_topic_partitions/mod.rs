//! Declarative facade for one-page public Admin `DescribeTopicPartitions`.

mod builder;
mod cursor;
mod operation;
mod page;
mod partition;
mod topic;

pub use builder::DescribeTopicPartitionsBuilder;
pub use cursor::DescribeTopicPartitionsCursor;
pub use operation::DescribeTopicPartitions;
pub use page::DescribeTopicPartitionsPage;
pub use partition::DescribeTopicPartition;
pub use topic::DescribeTopicPartitionsTopic;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod cursor_test;
#[cfg(test)]
mod page_test;
