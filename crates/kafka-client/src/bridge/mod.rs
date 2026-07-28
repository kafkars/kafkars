//! Declarative private boundary between the Rust facade and shared engine.
pub(crate) mod admin;
pub(crate) mod admin_alter_configs_operation;
pub(crate) mod admin_alter_configs_request;
pub(crate) mod admin_alter_configs_result;
pub(crate) mod admin_alter_replica_log_dirs;
pub(crate) mod admin_configs_operation;
pub(crate) mod admin_configs_request;
pub(crate) mod admin_configs_result;
pub(crate) mod admin_create_acls;
pub(crate) mod admin_delete_acls;
pub(crate) mod admin_delete_consumer_groups;
pub(crate) mod admin_delete_operation;
pub(crate) mod admin_delete_records;
pub(crate) mod admin_delete_result;
pub(crate) mod admin_describe_acls;
pub(crate) mod admin_describe_consumer_groups;
pub(crate) mod admin_describe_log_dirs;
pub(crate) mod admin_describe_operation;
pub(crate) mod admin_describe_result;
pub(crate) mod admin_elect_leaders;
pub(crate) mod admin_group_offset_delete_operation;
pub(crate) mod admin_group_offset_delete_request;
pub(crate) mod admin_group_offset_delete_result;
pub(crate) mod admin_group_offsets;
pub(crate) mod admin_list_consumer_groups;
pub(crate) mod admin_list_offsets;
pub(crate) mod admin_list_partition_reassignments;
pub(crate) mod admin_operation;
pub(crate) mod admin_partition_reassignments;
pub(crate) mod admin_partitions_operation;
pub(crate) mod admin_partitions_result;
pub(crate) mod admin_remove_consumer_group_members;
pub(crate) mod admin_result;
pub(crate) mod admin_topics_operation;
pub(crate) mod admin_topics_request;
pub(crate) mod admin_topics_result;
pub(crate) mod alter_client_quotas;
mod client;
pub(crate) mod client_shutdown;
pub(crate) mod consumer;
pub(crate) mod consumer_facade;
pub(crate) mod describe_client_quotas;
pub(crate) mod describe_user_scram_credentials;
pub(crate) mod producer;
pub(crate) mod producer_result;
pub(crate) mod transaction;
pub(crate) use client::ClientEngine;
#[cfg(test)]
mod admin_alter_configs_operation_test;
#[cfg(test)]
mod admin_alter_configs_request_test;
#[cfg(test)]
mod admin_alter_configs_result_test;
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
