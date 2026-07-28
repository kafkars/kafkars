//! Internal terminal diagnostics for concrete engine-host owners.

use crate::{
    admin::{
        AdminListOffsetsHostError, AlterConsumerGroupOffsetsHostError,
        AlterPartitionReassignmentsHostError, CreatePartitionsHostError, CreateTopicsHostError,
        DeleteConsumerGroupOffsetsHostError, DeleteTopicsHostError, DescribeClusterHostError,
        DescribeConfigsHostError, DescribeTopicsHostError, IncrementalAlterConfigsHostError,
        ListConsumerGroupOffsetsHostError, ListPartitionReassignmentsHostError,
    },
    clock::ClockError,
    completion::{CompletionRegistryError, NotifierJoinError},
    consumer::{
        AssignedConsumerFaultKind, AssignedConsumerOwnerError, AssignedConsumerRecoveryReport,
        GroupConsumerHostError,
    },
    driver::{
        CreatePartitionsCompletionFailure, CreateTopicsCompletionFailure,
        DeleteTopicsCompletionFailure, DescribeClusterCompletionFailure,
        DescribeConfigsCompletionFailure, DescribeTopicsCompletionFailure, DriverOwnerError,
        IncrementalAlterConfigsCompletionFailure, ProduceCompletionFailure,
        ProducerIdentityCompletionFailure,
    },
    producer::{
        ProducerHostInvariantError, ProducerIdentityHandoffError,
        execution::PreparedProduceHandoffError, execution_stop::ProducerExecutionStopError,
        ingress::ProducerShardTerminalError,
    },
    transaction::TransactionInitializationHostError,
};

#[derive(Debug)]
pub(crate) enum EngineHostError {
    Clock(ClockError),
    Producer(ProducerHostInvariantError),
    ProducerHandoff(PreparedProduceHandoffError),
    ProducerIdentityHandoff(ProducerIdentityHandoffError),
    ProduceCompletion(ProduceCompletionFailure),
    ProducerIdentityCompletion(ProducerIdentityCompletionFailure),
    ProducerStop(ProducerExecutionStopError),
    ProducerCleanup(ProducerShardTerminalError),
    ProducerLockPoisoned,
    AssignedConsumer(AssignedConsumerOwnerError),
    AssignedConsumerFault(AssignedConsumerFaultKind),
    AssignedConsumerLockPoisoned,
    AssignedConsumerOwnerMissing,
    AssignedConsumerCloseIncomplete,
    AssignedConsumerUnsettled(usize),
    AssignedConsumerRecovery(Box<AssignedConsumerRecoveryReport>),
    AssignedConsumerCompletion(CompletionRegistryError),
    GroupConsumer(GroupConsumerHostError),
    GroupConsumerLockPoisoned,
    TransactionInitialization(TransactionInitializationHostError),
    TransactionInitializationLockPoisoned,
    CreateTopics(CreateTopicsHostError),
    CreateTopicsCompletion(CreateTopicsCompletionFailure),
    CreateTopicsLockPoisoned,
    DeleteTopics(DeleteTopicsHostError),
    DeleteTopicsCompletion(DeleteTopicsCompletionFailure),
    DeleteTopicsLockPoisoned,
    DescribeCluster(DescribeClusterHostError),
    DescribeClusterCompletion(DescribeClusterCompletionFailure),
    DescribeClusterLockPoisoned,
    CreatePartitions(CreatePartitionsHostError),
    CreatePartitionsCompletion(CreatePartitionsCompletionFailure),
    CreatePartitionsLockPoisoned,
    DescribeTopics(DescribeTopicsHostError),
    DescribeTopicsCompletion(DescribeTopicsCompletionFailure),
    DescribeTopicsLockPoisoned,
    DescribeConfigs(DescribeConfigsHostError),
    DescribeConfigsCompletion(DescribeConfigsCompletionFailure),
    DescribeConfigsLockPoisoned,
    IncrementalAlterConfigs(IncrementalAlterConfigsHostError),
    IncrementalAlterConfigsCompletion(IncrementalAlterConfigsCompletionFailure),
    IncrementalAlterConfigsLockPoisoned,
    ListConsumerGroupOffsets(ListConsumerGroupOffsetsHostError),
    ListConsumerGroupOffsetsLockPoisoned,
    DeleteConsumerGroupOffsets(DeleteConsumerGroupOffsetsHostError),
    DeleteConsumerGroupOffsetsLockPoisoned,
    AlterConsumerGroupOffsets(AlterConsumerGroupOffsetsHostError),
    AlterConsumerGroupOffsetsLockPoisoned,
    AdminListOffsets(AdminListOffsetsHostError),
    AdminListOffsetsLockPoisoned,
    ListPartitionReassignments(ListPartitionReassignmentsHostError),
    ListPartitionReassignmentsLockPoisoned,
    AlterPartitionReassignments(AlterPartitionReassignmentsHostError),
    AlterPartitionReassignmentsLockPoisoned,
    AdminCompletion(CompletionRegistryError),
    Driver(DriverOwnerError),
    DriverOwnerMissing,
    DriverStopped,
    TrackedProduceCallsRemain(usize),
    TrackedProducerIdentityCallsRemain(usize),
    TrackedCreateTopicsCallsRemain(usize),
    TrackedDeleteTopicsCallsRemain(usize),
    DescribeClusterCallsRemain(usize),
    TrackedCreatePartitionsCallsRemain(usize),
    DescribeTopicsCallsRemain(usize),
    DescribeConfigsCallsRemain(usize),
    IncrementalAlterConfigsCallsRemain(usize),
    HostPanicked,
    Notifier(NotifierJoinError),
    Recovery {
        primary: Box<EngineHostError>,
        cleanup: Box<EngineHostError>,
    },
    #[cfg(test)]
    ForcedTestFailure,
}

impl EngineHostError {
    pub(in crate::engine_host) fn with_cleanup(self, cleanup: Self) -> Self {
        Self::Recovery {
            primary: Box::new(self),
            cleanup: Box::new(cleanup),
        }
    }
}

impl std::error::Error for EngineHostError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Recovery { primary, .. } => Some(primary),
            _ => None,
        }
    }
}
