//! One bounded notifier worker shared only by concrete admin terminal owners.

use std::thread::ThreadId;

use kafka_client_core::{
    AdminListOffsetsTerminal, AlterConsumerGroupOffsetsTerminal,
    AlterPartitionReassignmentsTerminal, CreatePartitionsTerminal, CreateTopicsTerminal,
    DeleteConsumerGroupOffsetsTerminal, DeleteTopicsTerminal, DescribeClusterTerminal,
    DescribeConfigsTerminal, DescribeTopicsTerminal, IncrementalAlterConfigsTerminal,
    ListConsumerGroupOffsetsTerminal, ListPartitionReassignmentsTerminal,
};

use crate::completion::{
    CompletionRegistryError, NotificationTicket, NotifierJoin, PublishTicket, SharedNotifier,
    SharedPublishPort,
};

use super::{
    ADMIN_LIST_OFFSETS_CAPACITY, ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY,
    ALTER_PARTITION_REASSIGNMENTS_CAPACITY, CREATE_PARTITIONS_CAPACITY, CREATE_TOPICS_CAPACITY,
    DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY, DELETE_TOPICS_CAPACITY, DESCRIBE_CLUSTER_CAPACITY,
    DESCRIBE_CONFIGS_CAPACITY, DESCRIBE_TOPICS_CAPACITY, INCREMENTAL_ALTER_CONFIGS_CAPACITY,
    LIST_CONSUMER_GROUP_OFFSETS_CAPACITY, LIST_PARTITION_REASSIGNMENTS_CAPACITY,
};

const ADMIN_NOTIFIER_THREAD: &str = "kafka-client-admin-completion-notifier";
const ADMIN_NOTIFICATION_CAPACITY: usize = CREATE_TOPICS_CAPACITY
    + DELETE_TOPICS_CAPACITY
    + DESCRIBE_CLUSTER_CAPACITY
    + CREATE_PARTITIONS_CAPACITY
    + DESCRIBE_TOPICS_CAPACITY
    + DESCRIBE_CONFIGS_CAPACITY
    + INCREMENTAL_ALTER_CONFIGS_CAPACITY
    + LIST_CONSUMER_GROUP_OFFSETS_CAPACITY
    + DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY
    + ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY
    + ADMIN_LIST_OFFSETS_CAPACITY
    + LIST_PARTITION_REASSIGNMENTS_CAPACITY
    + ALTER_PARTITION_REASSIGNMENTS_CAPACITY;

/// Closed allocation-free set of terminal tickets accepted by the admin worker.
pub(crate) enum AdminPublishTicket {
    CreateTopics(PublishTicket<CreateTopicsTerminal>),
    DeleteTopics(PublishTicket<DeleteTopicsTerminal>),
    DescribeCluster(PublishTicket<DescribeClusterTerminal>),
    CreatePartitions(PublishTicket<CreatePartitionsTerminal>),
    DescribeTopics(PublishTicket<DescribeTopicsTerminal>),
    DescribeConfigs(PublishTicket<DescribeConfigsTerminal>),
    IncrementalAlterConfigs(PublishTicket<IncrementalAlterConfigsTerminal>),
    ListConsumerGroupOffsets(PublishTicket<ListConsumerGroupOffsetsTerminal>),
    DeleteConsumerGroupOffsets(PublishTicket<DeleteConsumerGroupOffsetsTerminal>),
    AlterConsumerGroupOffsets(PublishTicket<AlterConsumerGroupOffsetsTerminal>),
    AdminListOffsets(PublishTicket<AdminListOffsetsTerminal>),
    ListPartitionReassignments(PublishTicket<ListPartitionReassignmentsTerminal>),
    AlterPartitionReassignments(PublishTicket<AlterPartitionReassignmentsTerminal>),
}

impl NotificationTicket for AdminPublishTicket {
    fn publish(self) {
        match self {
            Self::CreateTopics(ticket) => ticket.publish(),
            Self::DeleteTopics(ticket) => ticket.publish(),
            Self::DescribeCluster(ticket) => ticket.publish(),
            Self::CreatePartitions(ticket) => ticket.publish(),
            Self::DescribeTopics(ticket) => ticket.publish(),
            Self::DescribeConfigs(ticket) => ticket.publish(),
            Self::IncrementalAlterConfigs(ticket) => ticket.publish(),
            Self::ListConsumerGroupOffsets(ticket) => ticket.publish(),
            Self::DeleteConsumerGroupOffsets(ticket) => ticket.publish(),
            Self::AlterConsumerGroupOffsets(ticket) => ticket.publish(),
            Self::AdminListOffsets(ticket) => ticket.publish(),
            Self::ListPartitionReassignments(ticket) => ticket.publish(),
            Self::AlterPartitionReassignments(ticket) => ticket.publish(),
        }
    }
}

pub(crate) type CreateTopicsPublisher = SharedPublishPort<CreateTopicsTerminal, AdminPublishTicket>;
pub(crate) type DeleteTopicsPublisher = SharedPublishPort<DeleteTopicsTerminal, AdminPublishTicket>;
pub(crate) type DescribeClusterPublisher =
    SharedPublishPort<DescribeClusterTerminal, AdminPublishTicket>;
pub(crate) type CreatePartitionsPublisher =
    SharedPublishPort<CreatePartitionsTerminal, AdminPublishTicket>;
pub(crate) type DescribeTopicsPublisher =
    SharedPublishPort<DescribeTopicsTerminal, AdminPublishTicket>;
pub(crate) type DescribeConfigsPublisher =
    SharedPublishPort<DescribeConfigsTerminal, AdminPublishTicket>;
pub(crate) type IncrementalAlterConfigsPublisher =
    SharedPublishPort<IncrementalAlterConfigsTerminal, AdminPublishTicket>;
pub(crate) type ListConsumerGroupOffsetsPublisher =
    SharedPublishPort<ListConsumerGroupOffsetsTerminal, AdminPublishTicket>;
pub(crate) type DeleteConsumerGroupOffsetsPublisher =
    SharedPublishPort<DeleteConsumerGroupOffsetsTerminal, AdminPublishTicket>;
pub(crate) type AlterConsumerGroupOffsetsPublisher =
    SharedPublishPort<AlterConsumerGroupOffsetsTerminal, AdminPublishTicket>;
pub(crate) type AdminListOffsetsPublisher =
    SharedPublishPort<AdminListOffsetsTerminal, AdminPublishTicket>;
pub(crate) type ListPartitionReassignmentsPublisher =
    SharedPublishPort<ListPartitionReassignmentsTerminal, AdminPublishTicket>;
pub(crate) type AlterPartitionReassignmentsPublisher =
    SharedPublishPort<AlterPartitionReassignmentsTerminal, AdminPublishTicket>;

/// Exact typed ports issued once with the shared worker.
pub(crate) struct AdminCompletionPorts {
    pub(crate) create_topics: CreateTopicsPublisher,
    pub(crate) delete_topics: DeleteTopicsPublisher,
    pub(crate) describe_cluster: DescribeClusterPublisher,
    pub(crate) create_partitions: CreatePartitionsPublisher,
    pub(crate) describe_topics: DescribeTopicsPublisher,
    pub(crate) describe_configs: DescribeConfigsPublisher,
    pub(crate) incremental_alter_configs: IncrementalAlterConfigsPublisher,
    pub(crate) list_consumer_group_offsets: ListConsumerGroupOffsetsPublisher,
    pub(crate) delete_consumer_group_offsets: DeleteConsumerGroupOffsetsPublisher,
    pub(crate) alter_consumer_group_offsets: AlterConsumerGroupOffsetsPublisher,
    pub(crate) admin_list_offsets: AdminListOffsetsPublisher,
    pub(crate) list_partition_reassignments: ListPartitionReassignmentsPublisher,
    pub(crate) alter_partition_reassignments: AlterPartitionReassignmentsPublisher,
}

/// Unique lifecycle owner for the one shared admin notifier.
pub(crate) struct AdminCompletionNotifier {
    worker: Option<SharedNotifier<AdminPublishTicket>>,
}

impl AdminCompletionNotifier {
    pub(crate) fn start() -> std::io::Result<(Self, AdminCompletionPorts)> {
        let worker = SharedNotifier::start(ADMIN_NOTIFICATION_CAPACITY, ADMIN_NOTIFIER_THREAD)?;
        let ports = AdminCompletionPorts {
            create_topics: worker.publish_port(AdminPublishTicket::CreateTopics),
            delete_topics: worker.publish_port(AdminPublishTicket::DeleteTopics),
            describe_cluster: worker.publish_port(AdminPublishTicket::DescribeCluster),
            create_partitions: worker.publish_port(AdminPublishTicket::CreatePartitions),
            describe_topics: worker.publish_port(AdminPublishTicket::DescribeTopics),
            describe_configs: worker.publish_port(AdminPublishTicket::DescribeConfigs),
            incremental_alter_configs: worker
                .publish_port(AdminPublishTicket::IncrementalAlterConfigs),
            list_consumer_group_offsets: worker
                .publish_port(AdminPublishTicket::ListConsumerGroupOffsets),
            delete_consumer_group_offsets: worker
                .publish_port(AdminPublishTicket::DeleteConsumerGroupOffsets),
            alter_consumer_group_offsets: worker
                .publish_port(AdminPublishTicket::AlterConsumerGroupOffsets),
            admin_list_offsets: worker.publish_port(AdminPublishTicket::AdminListOffsets),
            list_partition_reassignments: worker
                .publish_port(AdminPublishTicket::ListPartitionReassignments),
            alter_partition_reassignments: worker
                .publish_port(AdminPublishTicket::AlterPartitionReassignments),
        };
        Ok((
            Self {
                worker: Some(worker),
            },
            ports,
        ))
    }

    pub(crate) fn stop(&mut self) -> Result<NotifierJoin, CompletionRegistryError> {
        self.take_join()
            .ok_or(CompletionRegistryError::NotifierStopped)
    }

    pub(crate) fn take_join(&mut self) -> Option<NotifierJoin> {
        self.worker.take().map(SharedNotifier::stop)
    }

    pub(crate) fn thread_id(&self) -> Option<ThreadId> {
        self.worker.as_ref().and_then(SharedNotifier::thread_id)
    }

    #[cfg(test)]
    pub(super) const fn capacity_for_test() -> usize {
        ADMIN_NOTIFICATION_CAPACITY
    }
}
