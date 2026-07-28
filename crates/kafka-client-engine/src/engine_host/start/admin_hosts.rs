//! Closed construction of concrete admin hosts from notifier publishers.

use crate::admin::{
    AdminCompletionPorts, AdminListOffsetsPublisher, AlterConsumerGroupOffsetsPublisher,
    AlterPartitionReassignmentsPublisher, AlterReplicaLogDirsHost, CreatePartitionsHost,
    CreateTopicsHost, DeleteConsumerGroupOffsetsHost, DeleteRecordsHost, DeleteTopicsHost,
    DescribeClusterHost, DescribeConfigsPublisher, DescribeLogDirsHost, DescribeTopicsHost,
    ElectLeadersHost, IncrementalAlterConfigsHost, ListConsumerGroupOffsetsHost,
    ListConsumerGroupsHost, ListPartitionReassignmentsPublisher,
};

pub(super) struct StartedAdminHosts {
    pub(super) create_topics: CreateTopicsHost,
    pub(super) delete_topics: DeleteTopicsHost,
    pub(super) delete_records: DeleteRecordsHost,
    pub(super) describe_cluster: DescribeClusterHost,
    pub(super) create_partitions: CreatePartitionsHost,
    pub(super) describe_topics: DescribeTopicsHost,
    pub(super) describe_configs: DescribeConfigsPublisher,
    pub(super) incremental_alter_configs: IncrementalAlterConfigsHost,
    pub(super) list_consumer_group_offsets: ListConsumerGroupOffsetsHost,
    pub(super) list_consumer_groups: ListConsumerGroupsHost,
    pub(super) delete_consumer_group_offsets: DeleteConsumerGroupOffsetsHost,
    pub(super) alter_consumer_group_offsets: AlterConsumerGroupOffsetsPublisher,
    pub(super) admin_list_offsets: AdminListOffsetsPublisher,
    pub(super) list_partition_reassignments: ListPartitionReassignmentsPublisher,
    pub(super) alter_partition_reassignments: AlterPartitionReassignmentsPublisher,
    pub(super) describe_log_dirs: DescribeLogDirsHost,
    pub(super) alter_replica_log_dirs: AlterReplicaLogDirsHost,
    pub(super) elect_leaders: ElectLeadersHost,
}

pub(super) fn start(ports: AdminCompletionPorts) -> StartedAdminHosts {
    let AdminCompletionPorts {
        create_topics,
        delete_topics,
        delete_records,
        describe_cluster,
        create_partitions,
        describe_topics,
        describe_configs,
        incremental_alter_configs,
        list_consumer_group_offsets,
        list_consumer_groups,
        delete_consumer_group_offsets,
        alter_consumer_group_offsets,
        admin_list_offsets,
        list_partition_reassignments,
        alter_partition_reassignments,
        describe_log_dirs,
        alter_replica_log_dirs,
        elect_leaders,
    } = ports;
    StartedAdminHosts {
        create_topics: CreateTopicsHost::new(create_topics),
        delete_topics: DeleteTopicsHost::new(delete_topics),
        delete_records: DeleteRecordsHost::new(delete_records),
        describe_cluster: DescribeClusterHost::new(describe_cluster),
        create_partitions: CreatePartitionsHost::new(create_partitions),
        describe_topics: DescribeTopicsHost::new(describe_topics),
        describe_configs,
        incremental_alter_configs: IncrementalAlterConfigsHost::new(incremental_alter_configs),
        list_consumer_group_offsets: ListConsumerGroupOffsetsHost::new(list_consumer_group_offsets),
        list_consumer_groups: ListConsumerGroupsHost::new(list_consumer_groups),
        delete_consumer_group_offsets: DeleteConsumerGroupOffsetsHost::new(
            delete_consumer_group_offsets,
        ),
        alter_consumer_group_offsets,
        admin_list_offsets,
        list_partition_reassignments,
        alter_partition_reassignments,
        describe_log_dirs: DescribeLogDirsHost::new(describe_log_dirs),
        alter_replica_log_dirs: AlterReplicaLogDirsHost::new(alter_replica_log_dirs),
        elect_leaders: ElectLeadersHost::new(elect_leaders),
    }
}
