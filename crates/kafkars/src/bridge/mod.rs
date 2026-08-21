//! Declarative private boundary between the Rust facade and shared engine.
pub(crate) mod abort_partition_transaction;
pub(crate) mod add_raft_voter;
pub(crate) mod admin;
pub(crate) mod admin_alter_config_resources_operation;
pub(crate) mod admin_alter_configs_operation;
pub(crate) mod admin_alter_configs_request;
pub(crate) mod admin_alter_configs_result;
pub(crate) mod admin_alter_replica_log_dirs;
pub(crate) mod admin_config_resources_operation;
pub(crate) mod admin_configs_operation;
pub(crate) mod admin_configs_request;
pub(crate) mod admin_configs_result;
pub(crate) mod admin_create_acls;
pub(crate) mod admin_delete_acls;
pub(crate) mod admin_delete_by_id_operation;
pub(crate) mod admin_delete_by_id_result;
pub(crate) mod admin_delete_consumer_groups;
pub(crate) mod admin_delete_operation;
pub(crate) mod admin_delete_records;
pub(crate) mod admin_delete_result;
pub(crate) mod admin_describe_acls;
pub(crate) mod admin_describe_classic_groups;
pub(crate) mod admin_describe_consumer_groups;
pub(crate) mod admin_describe_log_dirs;
pub(crate) mod admin_describe_operation;
pub(crate) mod admin_describe_replica_log_dirs;
pub(crate) mod admin_describe_result;
pub(crate) mod admin_elect_leaders;
pub(crate) mod admin_group_offset_delete_operation;
pub(crate) mod admin_group_offset_delete_request;
pub(crate) mod admin_group_offset_delete_result;
pub(crate) mod admin_group_offsets;
pub(crate) mod admin_list_consumer_groups;
pub(crate) mod admin_list_groups;
pub(crate) mod admin_list_offsets;
pub(crate) mod admin_list_partition_reassignments;
pub(crate) mod admin_list_transactions;
pub(crate) mod admin_operation;
pub(crate) mod admin_partition_reassignments;
pub(crate) mod admin_partitions_operation;
pub(crate) mod admin_partitions_result;
pub(crate) mod admin_remove_consumer_group_members;
pub(crate) mod admin_result;
pub(crate) mod admin_topics_by_id_operation;
pub(crate) mod admin_topics_by_id_result;
pub(crate) mod admin_topics_operation;
pub(crate) mod admin_topics_request;
pub(crate) mod admin_topics_result;
pub(crate) mod alter_client_quotas;
pub(crate) mod alter_share_group_offsets;
pub(crate) mod alter_user_scram_credentials;
pub(crate) mod client;
pub(crate) mod client_shutdown;
pub(crate) mod consumer;
pub(crate) mod consumer_configuration;
pub(crate) mod consumer_facade;
pub(crate) mod create_delegation_token;
pub(crate) mod delete_share_group_offsets;
pub(crate) mod describe_client_quotas;
pub(crate) mod describe_delegation_tokens;
pub(crate) mod describe_features;
pub(crate) mod describe_metadata_quorum;
pub(crate) mod describe_producers;
pub(crate) mod describe_share_group;
pub(crate) mod describe_share_groups;
pub(crate) mod describe_streams_group;
pub(crate) mod describe_streams_groups;
pub(crate) mod describe_topic_partitions;
pub(crate) mod describe_transactions;
pub(crate) mod describe_user_scram_credentials;
pub(crate) mod expire_delegation_token;
pub(crate) mod fence_producers;
pub(crate) mod legacy_replace_topic_configs;
pub(crate) mod list_client_metrics_resources;
pub(crate) mod list_config_resources;
pub(crate) mod list_share_group_offsets;
pub(crate) mod producer;
pub(crate) mod producer_result;
pub(crate) mod remove_raft_voter;
pub(crate) mod renew_delegation_token;
pub(crate) mod transaction;
pub(crate) mod unregister_broker;
pub(crate) mod update_features;
pub(crate) use client::ClientEngine;
#[cfg(test)]
mod admin_alter_config_resources_operation_test;
#[cfg(test)]
mod admin_alter_configs_operation_test;
#[cfg(test)]
mod admin_alter_configs_request_test;
#[cfg(test)]
mod admin_alter_configs_result_test;
#[cfg(test)]
mod admin_config_resources_operation_test;
#[cfg(test)]
mod admin_configs_operation_test;
#[cfg(test)]
mod admin_configs_request_test;
#[cfg(test)]
mod admin_configs_result_test;
#[cfg(test)]
mod admin_delete_operation_test;
#[cfg(test)]
mod admin_delete_result_test;
#[cfg(test)]
mod admin_describe_operation_test;
#[cfg(test)]
mod admin_describe_result_test;
#[cfg(test)]
mod admin_group_offset_delete_operation_test;
#[cfg(test)]
mod admin_group_offset_delete_request_test;
#[cfg(test)]
mod admin_group_offset_delete_result_test;
#[cfg(test)]
mod admin_operation_test;
#[cfg(test)]
mod admin_partitions_operation_test;
#[cfg(test)]
mod admin_partitions_result_test;
#[cfg(test)]
mod admin_result_test;
#[cfg(test)]
mod admin_test;
#[cfg(test)]
mod admin_topics_operation_test;
#[cfg(test)]
mod admin_topics_request_test;
#[cfg(test)]
mod admin_topics_result_test;
#[cfg(test)]
mod client_shutdown_test;
#[cfg(test)]
mod client_test;
#[cfg(test)]
mod consumer_configuration_test;
