//! Closed shared-admin capacity equations for destructive group-offset owners.

use super::{
    ADMIN_LIST_OFFSETS_CAPACITY, ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY,
    ALTER_PARTITION_REASSIGNMENTS_CAPACITY, ALTER_REPLICA_LOG_DIRS_CAPACITY,
    CREATE_PARTITIONS_CAPACITY, CREATE_TOPICS_CAPACITY, DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY,
    DELETE_TOPICS_CAPACITY, DESCRIBE_CLUSTER_CAPACITY, DESCRIBE_CONFIGS_CAPACITY,
    DESCRIBE_LOG_DIRS_CAPACITY, DESCRIBE_TOPICS_CAPACITY, INCREMENTAL_ALTER_CONFIGS_CAPACITY,
    LIST_CONSUMER_GROUP_OFFSETS_CAPACITY, LIST_PARTITION_REASSIGNMENTS_CAPACITY,
    completion::AdminCompletionNotifier,
};

#[test]
fn group_offset_delete_is_included_in_the_closed_shared_capacity_equation() {
    assert_eq!(
        AdminCompletionNotifier::capacity_for_test().checked_sub(
            CREATE_TOPICS_CAPACITY
                + DELETE_TOPICS_CAPACITY
                + DESCRIBE_CLUSTER_CAPACITY
                + CREATE_PARTITIONS_CAPACITY
                + DESCRIBE_TOPICS_CAPACITY
                + DESCRIBE_CONFIGS_CAPACITY
                + INCREMENTAL_ALTER_CONFIGS_CAPACITY
                + LIST_CONSUMER_GROUP_OFFSETS_CAPACITY
                + ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY
                + ADMIN_LIST_OFFSETS_CAPACITY
                + LIST_PARTITION_REASSIGNMENTS_CAPACITY
                + ALTER_PARTITION_REASSIGNMENTS_CAPACITY
                + DESCRIBE_LOG_DIRS_CAPACITY
                + ALTER_REPLICA_LOG_DIRS_CAPACITY
        ),
        Some(DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY)
    );
}

#[test]
fn group_offset_alter_is_included_in_the_closed_shared_capacity_equation() {
    assert_eq!(
        AdminCompletionNotifier::capacity_for_test().checked_sub(
            CREATE_TOPICS_CAPACITY
                + DELETE_TOPICS_CAPACITY
                + DESCRIBE_CLUSTER_CAPACITY
                + CREATE_PARTITIONS_CAPACITY
                + DESCRIBE_TOPICS_CAPACITY
                + DESCRIBE_CONFIGS_CAPACITY
                + INCREMENTAL_ALTER_CONFIGS_CAPACITY
                + LIST_CONSUMER_GROUP_OFFSETS_CAPACITY
                + DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY
                + ADMIN_LIST_OFFSETS_CAPACITY
                + LIST_PARTITION_REASSIGNMENTS_CAPACITY
                + ALTER_PARTITION_REASSIGNMENTS_CAPACITY
                + DESCRIBE_LOG_DIRS_CAPACITY
                + ALTER_REPLICA_LOG_DIRS_CAPACITY
        ),
        Some(ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY)
    );
}
