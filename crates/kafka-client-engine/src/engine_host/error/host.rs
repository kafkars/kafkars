//! Internal terminal diagnostics for concrete engine-host owners.

use std::fmt;

use crate::{
    admin::{
        CreatePartitionsHostError, CreateTopicsHostError, DeleteTopicsHostError,
        DescribeClusterHostError, DescribeTopicsHostError,
    },
    clock::ClockError,
    completion::{CompletionRegistryError, NotifierJoinError},
    driver::{
        CreatePartitionsCompletionFailure, CreateTopicsCompletionFailure,
        DeleteTopicsCompletionFailure, DescribeClusterCompletionFailure,
        DescribeTopicsCompletionFailure, DriverOwnerError, ProduceCompletionFailure,
        ProducerIdentityCompletionFailure,
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
    HostPanicked,
    Notifier(NotifierJoinError),
    Recovery {
        primary: Box<EngineHostError>,
        cleanup: Box<EngineHostError>,
    },
    #[cfg(test)]
    ForcedTestFailure,
}

impl fmt::Display for EngineHostError {
    // Keeping every concrete owner visible here prevents a generic error layer
    // from erasing which lifecycle failed.
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Clock(error) => write!(formatter, "engine clock failed: {error}"),
            Self::Producer(error) => write!(formatter, "producer host failed: {error}"),
            Self::ProducerHandoff(error) => {
                write!(formatter, "prepared Produce handoff failed: {error}")
            }
            Self::ProducerIdentityHandoff(error) => {
                write!(formatter, "producer identity handoff failed: {error}")
            }
            Self::ProduceCompletion(error) => write!(formatter, "{error}"),
            Self::ProducerIdentityCompletion(error) => write!(formatter, "{error}"),
            Self::ProducerStop(error) => write!(formatter, "producer recovery failed: {error}"),
            Self::ProducerCleanup(error) => {
                write!(formatter, "producer terminal cleanup failed: {error}")
            }
            Self::ProducerLockPoisoned => {
                formatter.write_str("producer host ownership lock is poisoned")
            }
            Self::CreateTopics(error) => write!(formatter, "CreateTopics host failed: {error}"),
            Self::CreateTopicsCompletion(error) => write!(formatter, "{error}"),
            Self::CreateTopicsLockPoisoned => {
                formatter.write_str("CreateTopics host ownership lock is poisoned")
            }
            Self::DeleteTopics(error) => write!(formatter, "DeleteTopics host failed: {error}"),
            Self::DeleteTopicsCompletion(error) => write!(formatter, "{error}"),
            Self::DeleteTopicsLockPoisoned => {
                formatter.write_str("DeleteTopics host ownership lock is poisoned")
            }
            Self::DescribeCluster(error) => {
                write!(formatter, "DescribeCluster host failed: {error}")
            }
            Self::DescribeClusterCompletion(error) => write!(formatter, "{error}"),
            Self::DescribeClusterLockPoisoned => {
                formatter.write_str("DescribeCluster host ownership lock is poisoned")
            }
            Self::CreatePartitions(error) => {
                write!(formatter, "CreatePartitions host failed: {error}")
            }
            Self::CreatePartitionsCompletion(error) => write!(formatter, "{error}"),
            Self::CreatePartitionsLockPoisoned => {
                formatter.write_str("CreatePartitions host ownership lock is poisoned")
            }
            Self::DescribeTopics(error) => write!(formatter, "DescribeTopics host failed: {error}"),
            Self::DescribeTopicsCompletion(error) => write!(formatter, "{error}"),
            Self::DescribeTopicsLockPoisoned => {
                formatter.write_str("DescribeTopics host ownership lock is poisoned")
            }
            Self::AdminCompletion(error) => {
                write!(
                    formatter,
                    "shared admin completion notifier failed: {error}"
                )
            }
            Self::Driver(error) => write!(formatter, "embedded driver failed: {error}"),
            Self::DriverOwnerMissing => formatter.write_str("embedded driver owner is unavailable"),
            Self::DriverStopped => formatter.write_str("embedded driver stopped unexpectedly"),
            Self::TrackedProduceCallsRemain(count) => {
                write!(
                    formatter,
                    "{count} tracked Produce calls remain at terminal cleanup"
                )
            }
            Self::TrackedProducerIdentityCallsRemain(count) => write!(
                formatter,
                "{count} tracked producer identity calls remain at terminal cleanup"
            ),
            Self::TrackedCreateTopicsCallsRemain(count) => {
                write!(
                    formatter,
                    "{count} tracked CreateTopics calls remain at terminal cleanup"
                )
            }
            Self::TrackedDeleteTopicsCallsRemain(count) => {
                write!(
                    formatter,
                    "{count} tracked DeleteTopics calls remain at terminal cleanup"
                )
            }
            Self::DescribeClusterCallsRemain(count) => {
                write!(
                    formatter,
                    "{count} DescribeCluster calls remain at terminal cleanup"
                )
            }
            Self::TrackedCreatePartitionsCallsRemain(count) => {
                write!(
                    formatter,
                    "{count} tracked CreatePartitions calls remain at terminal cleanup"
                )
            }
            Self::DescribeTopicsCallsRemain(count) => {
                write!(
                    formatter,
                    "{count} DescribeTopics calls remain at terminal cleanup"
                )
            }
            Self::HostPanicked => formatter.write_str("engine host thread panicked"),
            Self::Notifier(error) => write!(formatter, "completion notifier failed: {error}"),
            Self::Recovery { primary, cleanup } => {
                write!(
                    formatter,
                    "{primary}; terminal cleanup also failed: {cleanup}"
                )
            }
            #[cfg(test)]
            Self::ForcedTestFailure => formatter.write_str("forced engine host test failure"),
        }
    }
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
