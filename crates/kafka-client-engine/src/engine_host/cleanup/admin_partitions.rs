//! Terminal verification for partition- and transaction-scoped Admin owners.

use super::super::{EngineHostError, EngineHostResources};

#[allow(
    clippy::too_many_lines,
    reason = "terminal verification names every partition-scoped owner and its exact error"
)]
pub(super) fn verify(resources: &EngineHostResources) -> Result<(), EngineHostError> {
    let list_offsets = resources.list_offsets.terminal_host().unsettled();
    if list_offsets != 0 {
        return Err(EngineHostError::AdminListOffsets(
            crate::admin::AdminListOffsetsHostError::Unsettled(list_offsets),
        ));
    }
    super::list_partition_reassignments::verify(resources)?;
    super::alter_partition_reassignments::verify(resources)?;
    let elect_leaders = resources.elect_leaders.terminal_host().unsettled();
    if elect_leaders != 0 {
        return Err(EngineHostError::ElectLeaders(
            crate::admin::ElectLeadersHostError::Unsettled(elect_leaders),
        ));
    }
    let remove_consumer_group_members = resources
        .remove_consumer_group_members
        .terminal_host()
        .unsettled();
    if remove_consumer_group_members != 0 {
        return Err(EngineHostError::RemoveConsumerGroupMembers(
            crate::admin::RemoveConsumerGroupMembersHostError::Unsettled(
                remove_consumer_group_members,
            ),
        ));
    }
    let describe_producers = resources.describe_producers.terminal_host().unsettled();
    if describe_producers != 0 {
        return Err(EngineHostError::AdminDescribeProducers(
            crate::admin::AdminDescribeProducersHostError::Unsettled(describe_producers),
        ));
    }
    let describe_topic_partitions = resources
        .describe_topic_partitions
        .terminal_host()
        .unsettled();
    if describe_topic_partitions != 0 {
        return Err(EngineHostError::AdminDescribeTopicPartitions(
            crate::admin::AdminDescribeTopicPartitionsHostError::Unsettled(
                describe_topic_partitions,
            ),
        ));
    }
    let list_client_metrics_resources = resources
        .list_client_metrics_resources
        .terminal_host()
        .unsettled();
    if list_client_metrics_resources != 0 {
        return Err(EngineHostError::ListClientMetricsResources(
            crate::admin::list_client_metrics_resources::internal_api::
                ListClientMetricsResourcesHostError::Unsettled(
                list_client_metrics_resources,
            ),
        ));
    }
    let list_config_resources = resources.list_config_resources.terminal_host().unsettled();
    if list_config_resources != 0 {
        return Err(EngineHostError::ListConfigResources(
            crate::admin::list_config_resources::ListConfigResourcesHostError::Unsettled(
                list_config_resources,
            ),
        ));
    }
    let describe_transactions = resources.describe_transactions.terminal_host().unsettled();
    if describe_transactions != 0 {
        return Err(EngineHostError::AdminDescribeTransactions(
            crate::admin::AdminDescribeTransactionsHostError::Unsettled(describe_transactions),
        ));
    }
    let fence_producers = resources.fence_producers.terminal_host().unsettled();
    if fence_producers != 0 {
        return Err(EngineHostError::AdminFenceProducers(
            crate::admin::AdminFenceProducersHostError::Unsettled(fence_producers),
        ));
    }
    let list_transactions = resources.list_transactions.terminal_host().unsettled();
    if list_transactions != 0 {
        return Err(EngineHostError::AdminListTransactions(
            crate::admin::AdminListTransactionsHostError::Unsettled(list_transactions),
        ));
    }
    let delete_records = resources.delete_records.terminal_host().unsettled();
    if delete_records != 0 {
        return Err(EngineHostError::DeleteRecords(
            crate::admin::DeleteRecordsHostError::Unsettled(delete_records),
        ));
    }
    let partition_transaction_aborts = resources
        .abort_partition_transaction
        .terminal_host()
        .unsettled();
    if partition_transaction_aborts != 0 {
        return Err(EngineHostError::AbortPartitionTransaction(
            crate::admin::AbortPartitionTransactionHostError::Unsettled(
                partition_transaction_aborts,
            ),
        ));
    }
    let delete_consumer_groups = resources.delete_consumer_groups.terminal_host().unsettled();
    if delete_consumer_groups != 0 {
        return Err(EngineHostError::DeleteConsumerGroups(
            crate::admin::DeleteConsumerGroupsHostError::Unsettled(delete_consumer_groups),
        ));
    }
    Ok(())
}
