//! One bounded notifier worker shared only by concrete admin terminal owners.

use std::thread::ThreadId;

use kafka_client_core::{
    AdminDescribeConsumerGroupsTerminal, AdminDescribeLogDirsTerminal,
    AdminListConsumerGroupsTerminal, AdminListOffsetsTerminal, AlterClientQuotasTerminal,
    AlterConsumerGroupOffsetsTerminal, AlterPartitionReassignmentsTerminal,
    AlterReplicaLogDirsTerminal, CreatePartitionsTerminal, CreateTopicsTerminal,
    DeleteConsumerGroupOffsetsTerminal, DeleteConsumerGroupsTerminal, DeleteRecordsTerminal,
    DeleteTopicsTerminal, DescribeAclsTerminal, DescribeClientQuotasTerminal,
    DescribeClusterTerminal, DescribeConfigsTerminal, DescribeTopicsTerminal, ElectLeadersTerminal,
    IncrementalAlterConfigsTerminal, ListConsumerGroupOffsetsTerminal,
    ListPartitionReassignmentsTerminal, RemoveConsumerGroupMembersTerminal,
};

use super::{CreateAclsOutcome, DeleteAclsOutcome};

use crate::completion::{
    CompletionRegistryError, NotificationTicket, NotifierJoin, PublishTicket, SharedNotifier,
    SharedPublishPort,
};

use super::{
    ADMIN_LIST_OFFSETS_CAPACITY, ALTER_CLIENT_QUOTAS_CAPACITY,
    ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY, ALTER_PARTITION_REASSIGNMENTS_CAPACITY,
    ALTER_REPLICA_LOG_DIRS_CAPACITY, CREATE_ACLS_CAPACITY, CREATE_PARTITIONS_CAPACITY,
    CREATE_TOPICS_CAPACITY, DELETE_ACLS_CAPACITY, DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY,
    DELETE_CONSUMER_GROUPS_CAPACITY, DELETE_RECORDS_CAPACITY, DELETE_TOPICS_CAPACITY,
    DESCRIBE_ACLS_CAPACITY, DESCRIBE_CLIENT_QUOTAS_CAPACITY, DESCRIBE_CLUSTER_CAPACITY,
    DESCRIBE_CONFIGS_CAPACITY, DESCRIBE_CONSUMER_GROUPS_CAPACITY, DESCRIBE_LOG_DIRS_CAPACITY,
    DESCRIBE_TOPICS_CAPACITY, ELECT_LEADERS_CAPACITY, INCREMENTAL_ALTER_CONFIGS_CAPACITY,
    LIST_CONSUMER_GROUP_OFFSETS_CAPACITY, LIST_CONSUMER_GROUPS_CAPACITY,
    LIST_PARTITION_REASSIGNMENTS_CAPACITY, REMOVE_CONSUMER_GROUP_MEMBERS_CAPACITY,
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
    + DESCRIBE_ACLS_CAPACITY
    + DESCRIBE_CLIENT_QUOTAS_CAPACITY
    + ALTER_CLIENT_QUOTAS_CAPACITY
    + CREATE_ACLS_CAPACITY
    + DELETE_ACLS_CAPACITY;

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
    DeleteConsumerGroups(PublishTicket<DeleteConsumerGroupsTerminal>),
    AlterConsumerGroupOffsets(PublishTicket<AlterConsumerGroupOffsetsTerminal>),
    AdminListOffsets(PublishTicket<AdminListOffsetsTerminal>),
    ListPartitionReassignments(PublishTicket<ListPartitionReassignmentsTerminal>),
    AlterPartitionReassignments(PublishTicket<AlterPartitionReassignmentsTerminal>),
    ElectLeaders(PublishTicket<ElectLeadersTerminal>),
    DeleteRecords(PublishTicket<DeleteRecordsTerminal>),
    DescribeConsumerGroups(PublishTicket<AdminDescribeConsumerGroupsTerminal>),
    ListConsumerGroups(PublishTicket<AdminListConsumerGroupsTerminal>),
    RemoveConsumerGroupMembers(PublishTicket<RemoveConsumerGroupMembersTerminal>),
    DescribeLogDirs(PublishTicket<AdminDescribeLogDirsTerminal>),
    AlterReplicaLogDirs(PublishTicket<AlterReplicaLogDirsTerminal>),
    DescribeAcls(PublishTicket<DescribeAclsTerminal>),
    DescribeClientQuotas(PublishTicket<DescribeClientQuotasTerminal>),
    AlterClientQuotas(PublishTicket<AlterClientQuotasTerminal>),
    CreateAcls(PublishTicket<CreateAclsOutcome>),
    DeleteAcls(PublishTicket<DeleteAclsOutcome>),
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
            Self::DeleteConsumerGroups(ticket) => ticket.publish(),
            Self::AlterConsumerGroupOffsets(ticket) => ticket.publish(),
            Self::AdminListOffsets(ticket) => ticket.publish(),
            Self::ListPartitionReassignments(ticket) => ticket.publish(),
            Self::AlterPartitionReassignments(ticket) => ticket.publish(),
            Self::ElectLeaders(ticket) => ticket.publish(),
            Self::DeleteRecords(ticket) => ticket.publish(),
            Self::DescribeConsumerGroups(ticket) => ticket.publish(),
            Self::ListConsumerGroups(ticket) => ticket.publish(),
            Self::RemoveConsumerGroupMembers(ticket) => ticket.publish(),
            Self::DescribeLogDirs(ticket) => ticket.publish(),
            Self::AlterReplicaLogDirs(ticket) => ticket.publish(),
            Self::DescribeAcls(ticket) => ticket.publish(),
            Self::DescribeClientQuotas(ticket) => ticket.publish(),
            Self::AlterClientQuotas(ticket) => ticket.publish(),
            Self::CreateAcls(ticket) => ticket.publish(),
            Self::DeleteAcls(ticket) => ticket.publish(),
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
pub(crate) type DeleteConsumerGroupsPublisher =
    SharedPublishPort<DeleteConsumerGroupsTerminal, AdminPublishTicket>;
pub(crate) type AlterConsumerGroupOffsetsPublisher =
    SharedPublishPort<AlterConsumerGroupOffsetsTerminal, AdminPublishTicket>;
pub(crate) type AdminListOffsetsPublisher =
    SharedPublishPort<AdminListOffsetsTerminal, AdminPublishTicket>;
pub(crate) type ListPartitionReassignmentsPublisher =
    SharedPublishPort<ListPartitionReassignmentsTerminal, AdminPublishTicket>;
pub(crate) type AlterPartitionReassignmentsPublisher =
    SharedPublishPort<AlterPartitionReassignmentsTerminal, AdminPublishTicket>;
pub(crate) type ElectLeadersPublisher = SharedPublishPort<ElectLeadersTerminal, AdminPublishTicket>;
pub(crate) type DeleteRecordsPublisher =
    SharedPublishPort<DeleteRecordsTerminal, AdminPublishTicket>;
pub(crate) type AdminDescribeConsumerGroupsPublisher =
    SharedPublishPort<AdminDescribeConsumerGroupsTerminal, AdminPublishTicket>;
pub(crate) type AdminListConsumerGroupsPublisher =
    SharedPublishPort<AdminListConsumerGroupsTerminal, AdminPublishTicket>;
pub(crate) type RemoveConsumerGroupMembersPublisher =
    SharedPublishPort<RemoveConsumerGroupMembersTerminal, AdminPublishTicket>;
pub(crate) type AdminDescribeLogDirsPublisher =
    SharedPublishPort<AdminDescribeLogDirsTerminal, AdminPublishTicket>;
pub(crate) type AdminAlterReplicaLogDirsPublisher =
    SharedPublishPort<AlterReplicaLogDirsTerminal, AdminPublishTicket>;
pub(crate) type AdminDescribeAclsPublisher =
    SharedPublishPort<DescribeAclsTerminal, AdminPublishTicket>;
pub(crate) type AdminDescribeClientQuotasPublisher =
    SharedPublishPort<DescribeClientQuotasTerminal, AdminPublishTicket>;
pub(crate) type AdminAlterClientQuotasPublisher =
    SharedPublishPort<AlterClientQuotasTerminal, AdminPublishTicket>;
pub(crate) type AdminCreateAclsPublisher = SharedPublishPort<CreateAclsOutcome, AdminPublishTicket>;
pub(crate) type AdminDeleteAclsPublisher = SharedPublishPort<DeleteAclsOutcome, AdminPublishTicket>;

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
    pub(crate) delete_consumer_groups: DeleteConsumerGroupsPublisher,
    pub(crate) alter_consumer_group_offsets: AlterConsumerGroupOffsetsPublisher,
    pub(crate) admin_list_offsets: AdminListOffsetsPublisher,
    pub(crate) list_partition_reassignments: ListPartitionReassignmentsPublisher,
    pub(crate) alter_partition_reassignments: AlterPartitionReassignmentsPublisher,
    pub(crate) elect_leaders: ElectLeadersPublisher,
    pub(crate) delete_records: DeleteRecordsPublisher,
    pub(crate) describe_consumer_groups: AdminDescribeConsumerGroupsPublisher,
    pub(crate) list_consumer_groups: AdminListConsumerGroupsPublisher,
    pub(crate) remove_consumer_group_members: RemoveConsumerGroupMembersPublisher,
    pub(crate) describe_log_dirs: AdminDescribeLogDirsPublisher,
    pub(crate) alter_replica_log_dirs: AdminAlterReplicaLogDirsPublisher,
    pub(crate) describe_acls: AdminDescribeAclsPublisher,
    pub(crate) describe_client_quotas: AdminDescribeClientQuotasPublisher,
    pub(crate) alter_client_quotas: AdminAlterClientQuotasPublisher,
    pub(crate) create_acls: AdminCreateAclsPublisher,
    pub(crate) delete_acls: AdminDeleteAclsPublisher,
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
            delete_consumer_groups: worker.publish_port(AdminPublishTicket::DeleteConsumerGroups),
            alter_consumer_group_offsets: worker
                .publish_port(AdminPublishTicket::AlterConsumerGroupOffsets),
            admin_list_offsets: worker.publish_port(AdminPublishTicket::AdminListOffsets),
            list_partition_reassignments: worker
                .publish_port(AdminPublishTicket::ListPartitionReassignments),
            alter_partition_reassignments: worker
                .publish_port(AdminPublishTicket::AlterPartitionReassignments),
            elect_leaders: worker.publish_port(AdminPublishTicket::ElectLeaders),
            delete_records: worker.publish_port(AdminPublishTicket::DeleteRecords),
            describe_consumer_groups: worker
                .publish_port(AdminPublishTicket::DescribeConsumerGroups),
            list_consumer_groups: worker.publish_port(AdminPublishTicket::ListConsumerGroups),
            remove_consumer_group_members: worker
                .publish_port(AdminPublishTicket::RemoveConsumerGroupMembers),
            describe_log_dirs: worker.publish_port(AdminPublishTicket::DescribeLogDirs),
            alter_replica_log_dirs: worker.publish_port(AdminPublishTicket::AlterReplicaLogDirs),
            describe_acls: worker.publish_port(AdminPublishTicket::DescribeAcls),
            describe_client_quotas: worker.publish_port(AdminPublishTicket::DescribeClientQuotas),
            alter_client_quotas: worker.publish_port(AdminPublishTicket::AlterClientQuotas),
            create_acls: worker.publish_port(AdminPublishTicket::CreateAcls),
            delete_acls: worker.publish_port(AdminPublishTicket::DeleteAcls),
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
