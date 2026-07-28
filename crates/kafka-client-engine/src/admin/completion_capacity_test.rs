//! Closed shared-admin capacity equations for concrete completion owners.

use super::{
    ADMIN_LIST_OFFSETS_CAPACITY, ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY,
    ALTER_PARTITION_REASSIGNMENTS_CAPACITY, ALTER_REPLICA_LOG_DIRS_CAPACITY,
    CREATE_PARTITIONS_CAPACITY, CREATE_TOPICS_CAPACITY, DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY,
    DELETE_CONSUMER_GROUPS_CAPACITY, DELETE_RECORDS_CAPACITY, DELETE_TOPICS_CAPACITY,
    DESCRIBE_ACLS_CAPACITY, DESCRIBE_CLUSTER_CAPACITY, DESCRIBE_CONFIGS_CAPACITY,
    DESCRIBE_CONSUMER_GROUPS_CAPACITY, DESCRIBE_LOG_DIRS_CAPACITY, DESCRIBE_TOPICS_CAPACITY,
    ELECT_LEADERS_CAPACITY, INCREMENTAL_ALTER_CONFIGS_CAPACITY,
    LIST_CONSUMER_GROUP_OFFSETS_CAPACITY, LIST_CONSUMER_GROUPS_CAPACITY,
    LIST_PARTITION_REASSIGNMENTS_CAPACITY, REMOVE_CONSUMER_GROUP_MEMBERS_CAPACITY,
    completion::AdminCompletionNotifier,
};

const CLOSED_CAPACITY: usize = CREATE_TOPICS_CAPACITY
    + DELETE_TOPICS_CAPACITY
    + DESCRIBE_CLUSTER_CAPACITY
    + CREATE_PARTITIONS_CAPACITY
    + DESCRIBE_TOPICS_CAPACITY
    + DESCRIBE_CONFIGS_CAPACITY
    + INCREMENTAL_ALTER_CONFIGS_CAPACITY
    + LIST_CONSUMER_GROUP_OFFSETS_CAPACITY
    + DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY
    + DELETE_CONSUMER_GROUPS_CAPACITY
    + ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY
    + ADMIN_LIST_OFFSETS_CAPACITY
    + LIST_PARTITION_REASSIGNMENTS_CAPACITY
    + ALTER_PARTITION_REASSIGNMENTS_CAPACITY
    + ELECT_LEADERS_CAPACITY
    + DELETE_RECORDS_CAPACITY
    + DESCRIBE_CONSUMER_GROUPS_CAPACITY
    + LIST_CONSUMER_GROUPS_CAPACITY
    + REMOVE_CONSUMER_GROUP_MEMBERS_CAPACITY
    + DESCRIBE_LOG_DIRS_CAPACITY
    + ALTER_REPLICA_LOG_DIRS_CAPACITY
    + DESCRIBE_ACLS_CAPACITY;

#[test]
fn shared_capacity_is_the_sum_of_the_closed_admin_ticket_set() {
    assert_eq!(
        AdminCompletionNotifier::capacity_for_test(),
        CLOSED_CAPACITY
    );
}

fn capacity_without(excluded: usize) -> Option<usize> {
    AdminCompletionNotifier::capacity_for_test().checked_sub(CLOSED_CAPACITY - excluded)
}

#[test]
fn describe_topics_is_included_in_the_closed_shared_capacity_equation() {
    assert_eq!(
        capacity_without(DESCRIBE_TOPICS_CAPACITY),
        Some(DESCRIBE_TOPICS_CAPACITY)
    );
}

#[test]
fn create_partitions_is_included_in_the_closed_shared_capacity_equation() {
    assert_eq!(
        capacity_without(CREATE_PARTITIONS_CAPACITY),
        Some(CREATE_PARTITIONS_CAPACITY)
    );
}

#[test]
fn describe_configs_is_included_in_the_closed_shared_capacity_equation() {
    assert_eq!(
        capacity_without(DESCRIBE_CONFIGS_CAPACITY),
        Some(DESCRIBE_CONFIGS_CAPACITY)
    );
}

#[test]
fn incremental_alter_configs_is_included_in_the_closed_shared_capacity_equation() {
    assert_eq!(
        capacity_without(INCREMENTAL_ALTER_CONFIGS_CAPACITY),
        Some(INCREMENTAL_ALTER_CONFIGS_CAPACITY)
    );
}

#[test]
fn group_offsets_is_included_in_the_closed_shared_capacity_equation() {
    assert_eq!(
        capacity_without(LIST_CONSUMER_GROUP_OFFSETS_CAPACITY),
        Some(LIST_CONSUMER_GROUP_OFFSETS_CAPACITY)
    );
}

#[test]
fn partition_reassignment_listings_are_included_in_the_closed_shared_capacity_equation() {
    assert_eq!(
        capacity_without(LIST_PARTITION_REASSIGNMENTS_CAPACITY),
        Some(LIST_PARTITION_REASSIGNMENTS_CAPACITY)
    );
}

#[test]
fn partition_reassignment_alterations_are_included_in_the_closed_shared_capacity_equation() {
    assert_eq!(
        capacity_without(ALTER_PARTITION_REASSIGNMENTS_CAPACITY),
        Some(ALTER_PARTITION_REASSIGNMENTS_CAPACITY)
    );
}

#[test]
fn replica_log_dir_alterations_are_included_in_the_closed_shared_capacity_equation() {
    assert_eq!(
        capacity_without(ALTER_REPLICA_LOG_DIRS_CAPACITY),
        Some(ALTER_REPLICA_LOG_DIRS_CAPACITY)
    );
}
