//! Ordered admission closure for every engine-host ownership domain.

use super::EngineHostResources;

#[allow(
    clippy::too_many_lines,
    reason = "shutdown names every admission capability explicitly so omissions remain reviewable"
)]
pub(super) fn close_all(resources: &mut EngineHostResources) {
    let _close_result = resources.producer.close_admission();
    let _close_result = resources
        .abort_partition_transaction
        .admission_port()
        .close_admission();
    let _close_result = resources.create_topics.admission_port().close_admission();
    let _close_result = resources.delete_topics.admission_port().close_admission();
    let _close_result = resources
        .delete_consumer_groups
        .admission_port()
        .close_admission();
    let _close_result = resources
        .describe_cluster
        .admission_port()
        .close_admission();
    let _close_result = resources
        .describe_consumer_groups
        .admission_port()
        .close_admission();
    let _close_result = resources
        .describe_features
        .admission_port()
        .close_admission();
    let _close_result = resources
        .unregister_broker
        .admission_port()
        .close_admission();
    let _close_result = resources.add_raft_voter.admission_port().close_admission();
    let _close_result = resources
        .remove_raft_voter
        .admission_port()
        .close_admission();
    let _close_result = resources
        .describe_log_dirs
        .admission_port()
        .close_admission();
    let _close_result = resources
        .describe_replica_log_dirs
        .admission_port()
        .close_admission();
    let _close_result = resources
        .alter_replica_log_dirs
        .admission_port()
        .close_admission();
    let _close_result = resources.describe_acls.admission_port().close_admission();
    let _close_result = resources
        .describe_client_quotas
        .admission_port()
        .close_admission();
    let _close_result = resources
        .alter_client_quotas
        .admission_port()
        .close_admission();
    let _close_result = resources
        .alter_user_scram_credentials
        .admission_port()
        .close_admission();
    let _close_result = resources.update_features.admission_port().close_admission();
    let _close_result = resources
        .describe_user_scram_credentials
        .admission_port()
        .close_admission();
    let _close_result = resources
        .describe_metadata_quorum
        .admission_port()
        .close_admission();
    let _close_result = resources
        .describe_producers
        .admission_port()
        .close_admission();
    let _close_result = resources
        .describe_topic_partitions
        .admission_port()
        .close_admission();
    let _close_result = resources
        .describe_transactions
        .admission_port()
        .close_admission();
    let _close_result = resources.fence_producers.admission_port().close_admission();
    let _close_result = resources
        .list_transactions
        .admission_port()
        .close_admission();
    let _close_result = resources
        .list_client_metrics_resources
        .admission_port()
        .close_admission();
    let _close_result = resources
        .list_config_resources
        .admission_port()
        .close_admission();
    let _close_result = resources.create_acls.admission_port().close_admission();
    let _close_result = resources
        .create_delegation_token
        .admission_port()
        .close_admission();
    let _close_result = resources
        .describe_delegation_tokens
        .admission_port()
        .close_admission();
    let _close_result = resources
        .renew_delegation_token
        .admission_port()
        .close_admission();
    let _close_result = resources
        .expire_delegation_token
        .admission_port()
        .close_admission();
    let _close_result = resources.delete_acls.admission_port().close_admission();
    let _close_result = resources
        .create_partitions
        .admission_port()
        .close_admission();
    let _close_result = resources.describe_topics.admission_port().close_admission();
    let _close_result = resources
        .describe_configs
        .admission_port()
        .close_admission();
    let _close_result = resources
        .incremental_alter_configs
        .admission_port()
        .close_admission();
    let _close_result = resources
        .legacy_alter_configs
        .admission_port()
        .close_admission();
    let _close_result = resources
        .list_consumer_group_offsets
        .admission_port()
        .close_admission();
    let _close_result = resources
        .list_consumer_groups
        .admission_port()
        .close_admission();
    let _close_result = resources
        .delete_consumer_group_offsets
        .admission_port()
        .close_admission();
    let _close_result = resources
        .delete_share_group_offsets
        .admission_port()
        .close_admission();
    let _close_result = resources
        .list_share_group_offsets
        .admission_port()
        .close_admission();
    let _close_result = resources
        .alter_share_group_offsets
        .admission_port()
        .close_admission();
    let _close_result = resources
        .describe_share_group
        .admission_port()
        .close_admission();
    let _close_result = resources
        .describe_streams_group
        .admission_port()
        .close_admission();
    let _close_result = resources
        .alter_consumer_group_offsets
        .admission_port()
        .close_admission();
    let _close_result = resources.list_offsets.admission_port().close_admission();
    let _close_result = resources
        .list_partition_reassignments
        .admission_port()
        .close_admission();
    let _close_result = resources
        .alter_partition_reassignments
        .admission_port()
        .close_admission();
    let _close_result = resources.elect_leaders.admission_port().close_admission();
    let _close_result = resources
        .remove_consumer_group_members
        .admission_port()
        .close_admission();
    let _close_result = resources.delete_records.admission_port().close_admission();
    let _close_result = resources.assigned_consumer.close_assigned_admission();
    resources.group_consumers.close_admission();
    resources
        .transaction_initialization
        .admission_port()
        .close_admission();
}
