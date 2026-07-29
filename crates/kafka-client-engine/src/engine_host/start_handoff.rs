//! Startup capabilities and rollback joining for the native engine host.

use std::{
    sync::{Arc, mpsc::SyncSender},
    thread::JoinHandle,
};

use crate::{
    admin::{
        AdminListOffsetsAdmissionPort, AlterClientQuotasAdmissionPort,
        AlterConsumerGroupOffsetsAdmissionPort, AlterPartitionReassignmentsAdmissionPort,
        AlterReplicaLogDirsAdmissionPort, AlterUserScramCredentialsAdmissionPort,
        CreateAclsAdmissionPort, CreatePartitionsAdmissionPort, CreateTopicsAdmissionPort,
        DeleteAclsAdmissionPort, DeleteConsumerGroupOffsetsAdmissionPort,
        DeleteConsumerGroupsAdmissionPort, DeleteRecordsAdmissionPort, DeleteTopicsAdmissionPort,
        DescribeAclsAdmissionPort, DescribeClientQuotasAdmissionPort, DescribeClusterAdmissionPort,
        DescribeConfigsAdmissionPort, DescribeConsumerGroupsAdmissionPort,
        DescribeLogDirsAdmissionPort, DescribeTopicsAdmissionPort,
        DescribeUserScramCredentialsAdmissionPort, ElectLeadersAdmissionPort,
        IncrementalAlterConfigsAdmissionPort, ListConsumerGroupOffsetsAdmissionPort,
        ListPartitionReassignmentsAdmissionPort, RemoveConsumerGroupMembersAdmissionPort,
    },
    clock::MonotonicClock,
    consumer::{AssignedConsumerPort, GroupConsumerPort},
    producer::ingress::ProducerAdmissionPort,
    transaction::TransactionInitializationAdmissionPort,
};

use super::{EngineHostControl, EngineHostResources, EngineLifecycle, EngineStartError};

pub(crate) struct StartedEngineHost {
    pub(crate) admission: ProducerAdmissionPort,
    pub(crate) create_topics_admission: CreateTopicsAdmissionPort,
    pub(crate) create_acls_admission: CreateAclsAdmissionPort,
    pub(crate) delete_acls_admission: DeleteAclsAdmissionPort,
    pub(crate) delete_topics_admission: DeleteTopicsAdmissionPort,
    pub(crate) delete_consumer_groups_admission: DeleteConsumerGroupsAdmissionPort,
    pub(crate) delete_records_admission: DeleteRecordsAdmissionPort,
    pub(crate) describe_acls_admission: DescribeAclsAdmissionPort,
    pub(crate) describe_client_quotas_admission: DescribeClientQuotasAdmissionPort,
    pub(crate) alter_client_quotas_admission: AlterClientQuotasAdmissionPort,
    pub(crate) alter_user_scram_credentials_admission: AlterUserScramCredentialsAdmissionPort,
    pub(crate) describe_user_scram_credentials_admission: DescribeUserScramCredentialsAdmissionPort,
    pub(crate) describe_cluster_admission: DescribeClusterAdmissionPort,
    pub(crate) describe_consumer_groups_admission: DescribeConsumerGroupsAdmissionPort,
    pub(crate) describe_log_dirs_admission: DescribeLogDirsAdmissionPort,
    pub(crate) create_partitions_admission: CreatePartitionsAdmissionPort,
    pub(crate) describe_topics_admission: DescribeTopicsAdmissionPort,
    pub(crate) describe_configs_admission: DescribeConfigsAdmissionPort,
    pub(crate) incremental_alter_configs_admission: IncrementalAlterConfigsAdmissionPort,
    pub(crate) list_consumer_group_offsets_admission: ListConsumerGroupOffsetsAdmissionPort,
    pub(crate) list_consumer_groups_admission: crate::admin::ListConsumerGroupsAdmissionPort,
    pub(crate) delete_consumer_group_offsets_admission: DeleteConsumerGroupOffsetsAdmissionPort,
    pub(crate) alter_consumer_group_offsets_admission: AlterConsumerGroupOffsetsAdmissionPort,
    pub(crate) list_offsets_admission: AdminListOffsetsAdmissionPort,
    pub(crate) list_partition_reassignments_admission: ListPartitionReassignmentsAdmissionPort,
    pub(crate) alter_partition_reassignments_admission: AlterPartitionReassignmentsAdmissionPort,
    pub(crate) alter_replica_log_dirs_admission: AlterReplicaLogDirsAdmissionPort,
    pub(crate) elect_leaders_admission: ElectLeadersAdmissionPort,
    pub(crate) remove_consumer_group_members_admission: RemoveConsumerGroupMembersAdmissionPort,
    pub(crate) assigned_consumer: AssignedConsumerPort,
    pub(crate) group_consumer: GroupConsumerPort,
    pub(crate) transaction_initialization: TransactionInitializationAdmissionPort,
    pub(crate) clock: Arc<MonotonicClock>,
    pub(crate) control: Arc<EngineHostControl>,
    pub(crate) lifecycle: Arc<EngineLifecycle>,
}

pub(super) fn cancel_start(
    sender: SyncSender<EngineHostResources>,
    handle: JoinHandle<()>,
    error: EngineStartError,
) -> Result<StartedEngineHost, EngineStartError> {
    drop(sender);
    join_cancelled(handle);
    Err(error)
}

pub(super) fn join_cancelled(handle: JoinHandle<()>) {
    let _join_result = handle.join();
}
