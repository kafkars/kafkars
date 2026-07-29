//! Ordered post-driver recovery for group and transaction coordinator operations.

use super::super::super::{EngineHostError, EngineHostResources};

pub(super) fn recover(
    resources: &EngineHostResources,
    mut failure: EngineHostError,
) -> EngineHostError {
    let mut list_consumer_group_offsets = resources.list_consumer_group_offsets.terminal_host();
    if let Some(cleanup) = list_consumer_group_offsets
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::ListConsumerGroupOffsets)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(list_consumer_group_offsets);
    let mut list_consumer_groups = resources.list_consumer_groups.terminal_host();
    if let Some(cleanup) = list_consumer_groups
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::ListConsumerGroups)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(list_consumer_groups);
    let mut delete_consumer_group_offsets = resources.delete_consumer_group_offsets.terminal_host();
    if let Some(cleanup) = delete_consumer_group_offsets
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DeleteConsumerGroupOffsets)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(delete_consumer_group_offsets);
    let mut delete_share_group_offsets = resources.delete_share_group_offsets.terminal_host();
    if let Some(cleanup) = delete_share_group_offsets
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DeleteShareGroupOffsets)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(delete_share_group_offsets);
    let mut list_share_group_offsets = resources.list_share_group_offsets.terminal_host();
    if let Some(cleanup) = list_share_group_offsets
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::ListShareGroupOffsets)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(list_share_group_offsets);
    let mut alter_share_group_offsets = resources.alter_share_group_offsets.terminal_host();
    if let Some(cleanup) = alter_share_group_offsets
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::AlterShareGroupOffsets)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(alter_share_group_offsets);
    let mut describe_share_group = resources.describe_share_group.terminal_host();
    if let Some(cleanup) = describe_share_group
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DescribeShareGroup)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(describe_share_group);
    let mut describe_streams_group = resources.describe_streams_group.terminal_host();
    if let Some(cleanup) = describe_streams_group
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DescribeStreamsGroup)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(describe_streams_group);
    let mut delete_consumer_groups = resources.delete_consumer_groups.terminal_host();
    if let Some(cleanup) = delete_consumer_groups
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DeleteConsumerGroups)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(delete_consumer_groups);
    let mut describe_transactions = resources.describe_transactions.terminal_host();
    if let Some(cleanup) = describe_transactions
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::AdminDescribeTransactions)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(describe_transactions);
    let mut fence_producers = resources.fence_producers.terminal_host();
    if let Some(cleanup) = fence_producers
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::AdminFenceProducers)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(fence_producers);
    let mut list_transactions = resources.list_transactions.terminal_host();
    if let Some(cleanup) = list_transactions
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::AdminListTransactions)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(list_transactions);
    let mut alter_consumer_group_offsets = resources.alter_consumer_group_offsets.terminal_host();
    if let Some(cleanup) = alter_consumer_group_offsets
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::AlterConsumerGroupOffsets)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(alter_consumer_group_offsets);
    let mut list_offsets = resources.list_offsets.terminal_host();
    if let Some(cleanup) = list_offsets
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::AdminListOffsets)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(list_offsets);
    failure
}
