//! Closed ownership aggregate for the native engine host.

use std::sync::Arc;

use crate::{
    admin::{
        AdminListOffsetsShardOwner, AlterConsumerGroupOffsetsShardOwner,
        AlterPartitionReassignmentsShardOwner, AlterReplicaLogDirsShardOwner,
        CreatePartitionsShardOwner, CreateTopicsShardOwner, DeleteConsumerGroupOffsetsShardOwner,
        DeleteRecordsShardOwner, DeleteTopicsShardOwner, DescribeClusterShardOwner,
        DescribeConfigsShardOwner, DescribeConsumerGroupsShardOwner, DescribeLogDirsShardOwner,
        DescribeTopicsShardOwner, ElectLeadersShardOwner, IncrementalAlterConfigsShardOwner,
        ListConsumerGroupOffsetsShardOwner, ListConsumerGroupsShardOwner,
        ListPartitionReassignmentsShardOwner,
    },
    clock::MonotonicClock,
    driver::{
        DescribeClusterCalls, DescribeConfigsCalls, DescribeTopicsCalls, DriverOwner,
        IncrementalAlterConfigsCalls, TrackedCreatePartitionsCalls, TrackedCreateTopicsCalls,
        TrackedDeleteTopicsCalls, TrackedProduceCalls, TrackedProducerIdentityCalls,
    },
    producer::{host_turn::ProducerTurnBudget, ingress::ProducerShardOwner},
    transaction::TransactionInitializationShardOwner,
};

use super::super::EngineHostControl;

pub(crate) struct EngineHostResources {
    pub(in super::super) driver: Option<DriverOwner>,
    pub(in super::super) producer: ProducerShardOwner,
    pub(in super::super) admin_notifier: crate::admin::AdminCompletionNotifier,
    pub(in super::super) assigned_consumer_notifier:
        crate::consumer::AssignedConsumerCompletionNotifier,
    pub(in super::super) create_topics: CreateTopicsShardOwner,
    pub(in super::super) delete_topics: DeleteTopicsShardOwner,
    pub(in super::super) delete_records: DeleteRecordsShardOwner,
    pub(in super::super) describe_cluster: DescribeClusterShardOwner,
    pub(in super::super) create_partitions: CreatePartitionsShardOwner,
    pub(in super::super) describe_topics: DescribeTopicsShardOwner,
    pub(in super::super) describe_configs: DescribeConfigsShardOwner,
    pub(in super::super) incremental_alter_configs: IncrementalAlterConfigsShardOwner,
    pub(in super::super) list_consumer_group_offsets: ListConsumerGroupOffsetsShardOwner,
    pub(in super::super) list_consumer_groups: ListConsumerGroupsShardOwner,
    pub(in super::super) delete_consumer_group_offsets: DeleteConsumerGroupOffsetsShardOwner,
    pub(in super::super) alter_consumer_group_offsets: AlterConsumerGroupOffsetsShardOwner,
    pub(in super::super) list_offsets: AdminListOffsetsShardOwner,
    pub(in super::super) list_partition_reassignments: ListPartitionReassignmentsShardOwner,
    pub(in super::super) alter_partition_reassignments: AlterPartitionReassignmentsShardOwner,
    pub(in super::super) describe_consumer_groups: DescribeConsumerGroupsShardOwner,
    pub(in super::super) describe_log_dirs: DescribeLogDirsShardOwner,
    pub(in super::super) alter_replica_log_dirs: AlterReplicaLogDirsShardOwner,
    pub(in super::super) elect_leaders: ElectLeadersShardOwner,
    pub(in super::super) assigned_consumer: crate::consumer::AssignedConsumerShardOwner,
    pub(in super::super) group_consumers: crate::consumer::GroupConsumerShardOwner,
    pub(in super::super) transaction_initialization: TransactionInitializationShardOwner,
    pub(in super::super) clock: Arc<MonotonicClock>,
    pub(in super::super) control: Arc<EngineHostControl>,
    pub(in super::super) budget: ProducerTurnBudget,
    pub(in super::super) produce_calls: TrackedProduceCalls,
    pub(in super::super) producer_identity_calls: TrackedProducerIdentityCalls,
    pub(in super::super) producer_partitioning_call:
        Option<super::super::produce::ProducerPartitioningCall>,
    pub(in super::super) create_topics_calls: TrackedCreateTopicsCalls,
    pub(in super::super) delete_topics_calls: TrackedDeleteTopicsCalls,
    pub(in super::super) describe_cluster_calls: DescribeClusterCalls,
    pub(in super::super) create_partitions_calls: TrackedCreatePartitionsCalls,
    pub(in super::super) describe_topics_calls: DescribeTopicsCalls,
    pub(in super::super) describe_configs_calls: DescribeConfigsCalls,
    pub(in super::super) incremental_alter_configs_calls: IncrementalAlterConfigsCalls,
}

impl Drop for EngineHostResources {
    fn drop(&mut self) {
        super::super::admission_close::close_all(self);
    }
}
