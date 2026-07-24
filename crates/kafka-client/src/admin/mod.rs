//! Declarative facade for concrete batched Kafka administration.

mod batch_result;
mod builder;
mod configs;
mod create_partitions;
mod create_topics;
mod delete_builder;
mod delete_topics;
mod describe_builder;
mod describe_cluster;
mod describe_topics;
mod description;
mod handle;
mod new_partitions;
mod new_topic;
mod partitions_builder;
mod topic_description;
mod topics_builder;

pub use batch_result::BatchResult;
pub use builder::CreateTopicsBuilder;
pub use configs::{
    ConfigEntry, ConfigSynonym, DescribeConfigs, DescribeConfigsBuilder, DescribeConfigsResult,
    TopicConfigQuery,
};
pub use create_partitions::CreatePartitions;
pub use create_topics::CreateTopics;
pub use delete_builder::DeleteTopicsBuilder;
pub use delete_topics::DeleteTopics;
pub use describe_builder::DescribeClusterBuilder;
pub use describe_cluster::DescribeCluster;
pub use describe_topics::DescribeTopics;
pub use description::{ClusterBroker, ClusterDescription};
pub use handle::Admin;
pub use new_partitions::NewPartitions;
pub use new_topic::NewTopic;
pub use partitions_builder::CreatePartitionsBuilder;
pub use topic_description::{TopicDescription, TopicPartitionDescription};
pub use topics_builder::DescribeTopicsBuilder;

#[cfg(test)]
mod batch_result_test;
#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod create_partitions_test;
#[cfg(test)]
mod create_topics_test;
#[cfg(test)]
mod delete_builder_test;
#[cfg(test)]
mod delete_topics_test;
#[cfg(test)]
mod describe_builder_test;
#[cfg(test)]
mod describe_cluster_test;
#[cfg(test)]
mod describe_topics_test;
#[cfg(test)]
mod description_test;
#[cfg(test)]
mod handle_test;
#[cfg(test)]
mod new_partitions_test;
#[cfg(test)]
mod new_topic_test;
#[cfg(test)]
mod partitions_builder_test;
#[cfg(test)]
mod topic_description_test;
#[cfg(test)]
mod topics_builder_test;
