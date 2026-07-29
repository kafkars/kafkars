//! Deterministic policy for concrete Kafka admin operations.

mod abort_partition_transaction;
mod add_raft_voter;
mod alter_client_quotas;
mod alter_configs;
mod alter_partition_reassignments;
mod alter_replica_log_dirs;
mod alter_share_group_offsets;
mod alter_user_scram_credentials;
mod create_acls;
mod create_delegation_token;
mod delete_acls;
mod delete_consumer_groups;
mod delete_machine;
mod delete_model;
mod delete_outcome;
mod delete_records;
mod delete_share_group_offsets;
mod delete_transition;
mod describe_acls;
mod describe_client_quotas;
mod describe_configs_machine;
mod describe_configs_model;
mod describe_configs_outcome;
mod describe_configs_transition;
mod describe_configs_value;
mod describe_consumer_groups;
mod describe_delegation_tokens;
mod describe_features;
mod describe_log_dirs;
mod describe_machine;
mod describe_metadata_quorum;
mod describe_outcome;
mod describe_producers;
mod describe_replica_log_dirs;
mod describe_share_group;
mod describe_streams_group;
mod describe_topic_partitions;
mod describe_transactions;
mod describe_transition;
mod describe_user_scram_credentials;
mod elect_leaders;
mod expire_delegation_token;
mod exports;
mod fence_producers;
mod group_offset_alter;
mod group_offset_delete;
mod group_offsets;
mod legacy_alter_configs;
mod list_client_metrics_resources;
mod list_config_resources;
mod list_consumer_groups;
mod list_offsets;
mod list_partition_reassignments;
mod list_share_group_offsets;
mod list_transactions;
mod machine;
mod model;
mod outcome;
mod partitions_machine;
mod partitions_model;
mod partitions_outcome;
mod partitions_transition;
mod remove_consumer_group_members;
mod remove_raft_voter;
mod renew_delegation_token;
mod topic_description;
mod topics_machine;
mod topics_model;
mod topics_outcome;
mod topics_transition;
mod transition;
mod unregister_broker;
mod update_features;

pub use exports::*;

#[cfg(test)]
mod delete_model_test;
#[cfg(test)]
mod delete_transition_test;
#[cfg(test)]
mod describe_configs_model_test;
#[cfg(test)]
mod describe_configs_outcome_test;
#[cfg(test)]
mod describe_configs_transition_test;
#[cfg(test)]
mod describe_configs_value_test;
#[cfg(test)]
mod describe_transition_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod partitions_model_test;
#[cfg(test)]
mod partitions_transition_test;
#[cfg(test)]
mod topic_description_test;
#[cfg(test)]
mod topics_list_transition_test;
#[cfg(test)]
mod topics_model_test;
#[cfg(test)]
mod topics_outcome_test;
#[cfg(test)]
mod topics_transition_test;
#[cfg(test)]
mod transition_test;
