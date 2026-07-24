//! Internal terminal diagnostics for concrete engine-host owners.

use std::fmt;

use crate::{
    admin::{CreateTopicsHostError, DeleteTopicsHostError},
    clock::ClockError,
    completion::NotifierJoinError,
    driver::{
        CreateTopicsCompletionFailure, DeleteTopicsCompletionFailure, DriverOwnerError,
        ProduceCompletionFailure,
    },
    producer::{
        ProducerHostInvariantError, execution::PreparedProduceHandoffError,
        execution_stop::ProducerExecutionStopError, ingress::ProducerShardTerminalError,
    },
};

#[derive(Debug)]
pub(crate) enum EngineHostError {
    Clock(ClockError),
    Producer(ProducerHostInvariantError),
    ProducerHandoff(PreparedProduceHandoffError),
    ProduceCompletion(ProduceCompletionFailure),
    ProducerStop(ProducerExecutionStopError),
    ProducerCleanup(ProducerShardTerminalError),
    ProducerLockPoisoned,
    CreateTopics(CreateTopicsHostError),
    CreateTopicsCompletion(CreateTopicsCompletionFailure),
    CreateTopicsLockPoisoned,
    DeleteTopics(DeleteTopicsHostError),
    DeleteTopicsCompletion(DeleteTopicsCompletionFailure),
    DeleteTopicsLockPoisoned,
    Driver(DriverOwnerError),
    DriverOwnerMissing,
    DriverStopped,
    TrackedProduceCallsRemain(usize),
    TrackedCreateTopicsCallsRemain(usize),
    TrackedDeleteTopicsCallsRemain(usize),
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
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Clock(error) => write!(formatter, "engine clock failed: {error}"),
            Self::Producer(error) => write!(formatter, "producer host failed: {error}"),
            Self::ProducerHandoff(error) => {
                write!(formatter, "prepared Produce handoff failed: {error}")
            }
            Self::ProduceCompletion(error) => write!(formatter, "{error}"),
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
            Self::Driver(error) => write!(formatter, "embedded driver failed: {error}"),
            Self::DriverOwnerMissing => formatter.write_str("embedded driver owner is unavailable"),
            Self::DriverStopped => formatter.write_str("embedded driver stopped unexpectedly"),
            Self::TrackedProduceCallsRemain(count) => {
                write!(
                    formatter,
                    "{count} tracked Produce calls remain at terminal cleanup"
                )
            }
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
