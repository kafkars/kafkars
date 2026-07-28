//! Declarative facade for concrete batched Kafka administration.
mod alter_configs;
mod alter_replica_log_dirs;
mod batch_result;
mod builder;
mod configs;
mod create_partitions;
mod create_topics;
mod delete_builder;
mod delete_consumer_group_offsets;
mod delete_consumer_group_offsets_builder;
mod delete_consumer_group_offsets_result;
mod delete_consumer_groups;
mod delete_records;
mod delete_topics;
mod describe_acls;
mod describe_builder;
mod describe_cluster;
mod describe_consumer_groups;
mod describe_log_dirs;
mod describe_topics;
mod description;
mod elect_leaders;
mod group_offsets;
mod handle;
mod list_consumer_group_offsets;
mod list_consumer_group_offsets_builder;
mod list_consumer_group_offsets_result;
mod list_consumer_groups;
mod list_offsets;
mod list_topics;
mod list_topics_builder;
mod new_partitions;
mod new_topic;
mod partition_reassignments;
mod partitions_builder;
mod public_api;
mod remove_consumer_group_members;
mod topic_description;
mod topics_builder;
pub use public_api::*;
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
mod delete_consumer_group_offsets_builder_test;
#[cfg(test)]
mod delete_consumer_group_offsets_result_test;
#[cfg(test)]
mod delete_consumer_group_offsets_test;
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
mod list_consumer_group_offsets_builder_test;
#[cfg(test)]
mod list_consumer_group_offsets_result_test;
#[cfg(test)]
mod list_consumer_group_offsets_test;
#[cfg(test)]
mod list_topics_builder_test;
#[cfg(test)]
mod list_topics_test;
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
