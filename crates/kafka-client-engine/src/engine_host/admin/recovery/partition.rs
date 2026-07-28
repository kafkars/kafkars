//! Ordered post-driver recovery for partition-scoped admin owners.

use super::super::super::{EngineHostError, EngineHostResources};

pub(super) fn recover_partition_operations(
    resources: &EngineHostResources,
    mut failure: EngineHostError,
) -> EngineHostError {
    let mut list_partition_reassignments = resources.list_partition_reassignments.terminal_host();
    if let Some(cleanup) = list_partition_reassignments
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::ListPartitionReassignments)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(list_partition_reassignments);
    let mut alter_partition_reassignments = resources.alter_partition_reassignments.terminal_host();
    if let Some(cleanup) = alter_partition_reassignments
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::AlterPartitionReassignments)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(alter_partition_reassignments);
    let mut elect_leaders = resources.elect_leaders.terminal_host();
    if let Some(cleanup) = elect_leaders
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::ElectLeaders)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(elect_leaders);
    let mut remove_consumer_group_members = resources.remove_consumer_group_members.terminal_host();
    if let Some(cleanup) = remove_consumer_group_members
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::RemoveConsumerGroupMembers)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(remove_consumer_group_members);
    let mut delete_records = resources.delete_records.terminal_host();
    if let Some(cleanup) = delete_records
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DeleteRecords)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(delete_records);
    failure
}
