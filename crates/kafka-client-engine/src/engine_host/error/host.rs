//! Internal terminal diagnostics for concrete engine-host owners.

use crate::{
    admin::{
        CreatePartitionsHostError, CreateTopicsHostError, DeleteTopicsHostError,
        DescribeClusterHostError, DescribeConfigsHostError, DescribeTopicsHostError,
    },
    clock::ClockError,
    completion::{CompletionRegistryError, NotifierJoinError},
    consumer::{
        AssignedConsumerFaultKind, AssignedConsumerOwnerError, AssignedConsumerRecoveryReport,
    },
    driver::{
        CreatePartitionsCompletionFailure, CreateTopicsCompletionFailure,
        DeleteTopicsCompletionFailure, DescribeClusterCompletionFailure,
        DescribeConfigsCompletionFailure, DescribeTopicsCompletionFailure, DriverOwnerError,
        ProduceCompletionFailure, ProducerIdentityCompletionFailure,
    },
    producer::{
        ProducerHostInvariantError, ProducerIdentityHandoffError,
        execution::PreparedProduceHandoffError, execution_stop::ProducerExecutionStopError,
        ingress::ProducerShardTerminalError,
    },
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
