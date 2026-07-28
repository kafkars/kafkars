//! Generated-message adaptation for concrete Kafka admin operations.

pub(crate) mod alter_client_quotas;
pub(crate) mod alter_partition_reassignments;
pub(crate) mod alter_replica_log_dirs;
pub(crate) mod create_acls;
pub(crate) mod create_partitions;
mod create_partitions_budget;
pub(crate) mod create_topics;
pub(crate) mod delete_acls;
pub(crate) mod delete_groups;
pub(crate) mod delete_records;
pub(crate) mod delete_topics;
mod delete_topics_budget;
pub(crate) mod describe_acls;
pub(crate) mod describe_client_quotas;
pub(crate) mod describe_cluster;
pub(crate) mod describe_configs;
mod describe_configs_budget;
mod describe_configs_model;
pub(crate) mod describe_configs_response;
mod describe_configs_values;
pub(crate) mod describe_consumer_groups;
pub(crate) mod describe_log_dirs;
mod describe_topic_value;
pub(crate) mod describe_topics;
mod describe_topics_budget;
pub(crate) mod describe_topics_response;
pub(crate) mod describe_user_scram_credentials;
pub(crate) mod elect_leaders;
#[cfg_attr(not(test), expect(dead_code, reason = "offset alter host follows"))]
pub(crate) mod group_offset_alter;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "group-offset deletion host integration follows this protocol slice"
    )
)]
pub(crate) mod group_offset_delete;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "group-offset host integration follows this protocol slice"
    )
)]
pub(crate) mod group_offsets;
pub(crate) mod incremental_alter_configs;
pub(crate) mod list_consumer_groups;
pub(crate) mod list_offsets;
pub(crate) mod list_partition_reassignments;
mod list_topics_response;
pub(crate) mod remove_consumer_group_members;
mod request_timeout_error;
#[cfg(test)]
mod request_timeout_error_test;
mod result_budget;

#[cfg(test)]
mod create_partitions_budget_test;
#[cfg(test)]
mod create_partitions_test;
#[cfg(test)]
mod create_topics_test;
#[cfg(test)]
mod delete_topics_budget_test;
#[cfg(test)]
mod delete_topics_test;
#[cfg(test)]
mod describe_cluster_test;
#[cfg(test)]
mod describe_configs_budget_test;
#[cfg(test)]
mod describe_configs_model_test;
#[cfg(test)]
mod describe_configs_response_test;
#[cfg(test)]
mod describe_configs_test;
#[cfg(test)]
mod describe_configs_values_test;
#[cfg(test)]
mod describe_topic_value_test;
#[cfg(test)]
mod describe_topics_budget_test;
#[cfg(test)]
mod describe_topics_response_test;
#[cfg(test)]
mod describe_topics_test;
#[cfg(test)]
mod list_topics_response_test;
#[cfg(test)]
mod result_budget_test;
