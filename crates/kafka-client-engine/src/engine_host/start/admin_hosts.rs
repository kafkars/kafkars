//! Closed construction of concrete admin hosts from notifier publishers.

use crate::admin::{
    AdminCompletionPorts, AdminListOffsetsPublisher, AlterConsumerGroupOffsetsPublisher,
    CreatePartitionsHost, CreateTopicsHost, DeleteConsumerGroupOffsetsHost, DeleteTopicsHost,
    DescribeClusterHost, DescribeConfigsPublisher, DescribeTopicsHost, IncrementalAlterConfigsHost,
    ListConsumerGroupOffsetsHost, ListPartitionReassignmentsPublisher,
};

pub(super) struct StartedAdminHosts {
    pub(super) create_topics: CreateTopicsHost,
    pub(super) delete_topics: DeleteTopicsHost,
    pub(super) describe_cluster: DescribeClusterHost,
    pub(super) create_partitions: CreatePartitionsHost,
    pub(super) describe_topics: DescribeTopicsHost,
    pub(super) describe_configs: DescribeConfigsPublisher,
    pub(super) incremental_alter_configs: IncrementalAlterConfigsHost,
    pub(super) list_consumer_group_offsets: ListConsumerGroupOffsetsHost,
    pub(super) delete_consumer_group_offsets: DeleteConsumerGroupOffsetsHost,
    pub(super) alter_consumer_group_offsets: AlterConsumerGroupOffsetsPublisher,
    pub(super) admin_list_offsets: AdminListOffsetsPublisher,
    pub(super) list_partition_reassignments: ListPartitionReassignmentsPublisher,
}

pub(super) fn start(ports: AdminCompletionPorts) -> StartedAdminHosts {
    let AdminCompletionPorts {
        create_topics,
        delete_topics,
        describe_cluster,
        create_partitions,
        describe_topics,
        describe_configs,
        incremental_alter_configs,
        list_consumer_group_offsets,
        delete_consumer_group_offsets,
        alter_consumer_group_offsets,
        admin_list_offsets,
        list_partition_reassignments,
    } = ports;
    StartedAdminHosts {
        create_topics: CreateTopicsHost::new(create_topics),
        delete_topics: DeleteTopicsHost::new(delete_topics),
        describe_cluster: DescribeClusterHost::new(describe_cluster),
        create_partitions: CreatePartitionsHost::new(create_partitions),
        describe_topics: DescribeTopicsHost::new(describe_topics),
        describe_configs,
        incremental_alter_configs: IncrementalAlterConfigsHost::new(incremental_alter_configs),
        list_consumer_group_offsets: ListConsumerGroupOffsetsHost::new(list_consumer_group_offsets),
        delete_consumer_group_offsets: DeleteConsumerGroupOffsetsHost::new(
            delete_consumer_group_offsets,
        ),
        alter_consumer_group_offsets,
        admin_list_offsets,
        list_partition_reassignments,
    }
}
