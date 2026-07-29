//! Declarative facade for concrete batched Kafka administration.
mod abort_partition_transaction;
mod acls;
mod add_raft_voter;
mod alter_client_quotas;
mod alter_configs;
mod alter_replica_log_dirs;
mod alter_share_group_offsets;
mod alter_streams_group_offsets;
mod alter_user_scram_credentials;
mod batch_result;
mod builder;
mod configs;
mod create_acls;
mod create_delegation_token;
mod create_partitions;
mod create_topics;
mod delete_acls;
mod delete_builder;
mod delete_consumer_group_offsets;
mod delete_consumer_group_offsets_builder;
mod delete_consumer_group_offsets_result;
mod delete_consumer_groups;
mod delete_records;
mod delete_share_group_offsets;
mod delete_share_groups;
mod delete_streams_group_offsets;
mod delete_streams_groups;
mod delete_topics;
mod delete_topics_by_id;
mod delete_topics_by_id_builder;
mod describe_acls;
mod describe_builder;
mod describe_classic_groups;
mod describe_client_quotas;
mod describe_cluster;
mod describe_consumer_groups;
mod describe_delegation_tokens;
mod describe_features;
mod describe_log_dirs;
mod describe_metadata_quorum;
mod describe_producers;
mod describe_replica_log_dirs;
mod describe_share_group;
mod describe_share_groups;
mod describe_streams_group;
mod describe_streams_groups;
mod describe_topic_partitions;
mod describe_topics;
mod describe_topics_by_id;
mod describe_transactions;
mod describe_user_scram_credentials;
mod description;
mod elect_leaders;
mod expire_delegation_token;
mod fence_producers;
mod force_terminate_transaction;
mod group_offsets;
mod handle;
mod legacy_replace_topic_configs;
mod list_client_metrics_resources;
mod list_config_resources;
mod list_consumer_group_offsets;
mod list_consumer_group_offsets_builder;
mod list_consumer_group_offsets_result;
mod list_consumer_groups;
mod list_consumer_groups_offsets;
mod list_consumer_groups_offsets_builder;
mod list_consumer_groups_offsets_result;
mod list_groups;
mod list_offsets;
mod list_share_group_offsets;
mod list_share_groups_offsets;
mod list_streams_group_offsets;
mod list_streams_groups_offsets;
mod list_topics;
mod list_topics_builder;
mod list_transactions;
mod new_partitions;
mod new_topic;
mod partition_reassignments;
mod partitions_builder;
mod public_api;
mod raft_voter;
mod remove_consumer_group_members;
mod remove_raft_voter;
mod renew_delegation_token;
mod topic_description;
mod topics_builder;
mod topics_by_id_builder;
mod unregister_broker;
mod update_features;
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
mod delete_topics_by_id_builder_test;
#[cfg(test)]
mod delete_topics_by_id_test;
#[cfg(test)]
mod delete_topics_test;
#[cfg(test)]
mod describe_builder_test;
#[cfg(test)]
mod describe_cluster_test;
#[cfg(test)]
mod describe_topics_by_id_test;
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
mod list_consumer_groups_offsets_builder_test;
#[cfg(test)]
mod list_consumer_groups_offsets_test;
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
#[cfg(test)]
mod topics_by_id_builder_test;
