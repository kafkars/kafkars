//! Ordered admission closure for every engine-host ownership domain.

use super::EngineHostResources;

pub(super) fn close_all(resources: &mut EngineHostResources) {
    let _close_result = resources.producer.close_admission();
    let _close_result = resources.create_topics.admission_port().close_admission();
    let _close_result = resources.delete_topics.admission_port().close_admission();
    let _close_result = resources
        .delete_consumer_groups
        .describe_cluster
        .admission_port()
        .close_admission();
    let _close_result = resources
    let _close_result = resources.describe_acls.admission_port().close_admission();
    let _close_result = resources.create_acls.admission_port().close_admission();
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
    let _close_result = resources
        .describe_consumer_groups
        .admission_port()
        .close_admission();
    let _close_result = resources
        .describe_log_dirs
        .admission_port()
        .close_admission();
    let _close_result = resources
        .alter_replica_log_dirs
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
