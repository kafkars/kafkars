//! Exact driver-era ownership retained by failed position reconciliation.

use kafka_client_core::{GroupPositionBootstrapEffect, GroupPositionPartitionFact};

use crate::{
    driver::{
        GroupPositionOffsetFetchAccepted, GroupPositionOffsetFetchCompletionObservation,
        GroupPositionOffsetFetchKey, GroupPositionOffsetFetchTerminal,
    },
    protocol::consumer::GroupOffsetFetchCorrelation,
};

use super::{ClassicGroupPositionExecutionError, ClassicGroupPositionTerminalApplicationFailure};

enum ClassicGroupPositionRecoveryOwnership {
    Key(GroupPositionOffsetFetchKey),
    Fence(kafka_client_core::GroupPositionFence),
    RawTerminal(GroupPositionOffsetFetchTerminal),
    Terminal {
        terminal: GroupPositionOffsetFetchTerminal,
        application: ClassicGroupPositionTerminalApplicationFailure,
    },
    PostCore {
        key: GroupPositionOffsetFetchKey,
        machine: kafka_client_core::GroupPositionBootstrapMachine,
        correlation: GroupOffsetFetchCorrelation,
        accepted: GroupPositionOffsetFetchAccepted,
        result_buffer: Vec<GroupPositionPartitionFact>,
        effect: Option<GroupPositionBootstrapEffect>,
    },
}

/// Exact unresolved position owner retained after failed shutdown reconciliation.
#[must_use = "position recovery failure retains its exact driver-era owner"]
pub(in crate::consumer::group) struct ClassicGroupPositionRecoveryFault {
    error: ClassicGroupPositionExecutionError,
    ownership: ClassicGroupPositionRecoveryOwnership,
    completion: Option<GroupPositionOffsetFetchCompletionObservation>,
}

impl ClassicGroupPositionRecoveryFault {
    pub(super) fn missing_key(
        error: ClassicGroupPositionExecutionError,
        key: GroupPositionOffsetFetchKey,
    ) -> Self {
        Self::key(error, key)
    }

    pub(super) fn missing_completion(
        error: ClassicGroupPositionExecutionError,
        key: GroupPositionOffsetFetchKey,
        observation: GroupPositionOffsetFetchCompletionObservation,
    ) -> Self {
        Self::key(error, key).with_completion(observation)
    }

    pub(super) const fn missing_fence(
        error: ClassicGroupPositionExecutionError,
        fence: kafka_client_core::GroupPositionFence,
    ) -> Self {
        Self::fence(error, fence)
    }

    pub(super) fn missing_terminal(
        error: ClassicGroupPositionExecutionError,
        terminal: GroupPositionOffsetFetchTerminal,
    ) -> Self {
        Self {
            error,
            ownership: ClassicGroupPositionRecoveryOwnership::RawTerminal(terminal),
            completion: None,
        }
    }

    pub(super) fn with_completion(
        self,
        observation: GroupPositionOffsetFetchCompletionObservation,
    ) -> Self {
        Self {
            completion: Some(observation),
            ..self
        }
    }

    pub(super) fn key(
        error: ClassicGroupPositionExecutionError,
        key: GroupPositionOffsetFetchKey,
    ) -> Self {
        Self {
            error,
            ownership: ClassicGroupPositionRecoveryOwnership::Key(key),
            completion: None,
        }
    }

    pub(super) fn terminal(
        terminal: GroupPositionOffsetFetchTerminal,
        application: ClassicGroupPositionTerminalApplicationFailure,
    ) -> Self {
        Self {
            error: application.error(),
            ownership: ClassicGroupPositionRecoveryOwnership::Terminal {
                terminal,
                application,
            },
            completion: None,
        }
    }

    pub(super) const fn fence(
        error: ClassicGroupPositionExecutionError,
        fence: kafka_client_core::GroupPositionFence,
    ) -> Self {
        Self {
            error,
            ownership: ClassicGroupPositionRecoveryOwnership::Fence(fence),
            completion: None,
        }
    }

    pub(super) fn post_core(
        key: GroupPositionOffsetFetchKey,
        machine: kafka_client_core::GroupPositionBootstrapMachine,
        correlation: GroupOffsetFetchCorrelation,
        accepted: GroupPositionOffsetFetchAccepted,
        result_buffer: Vec<GroupPositionPartitionFact>,
        effect: Option<GroupPositionBootstrapEffect>,
    ) -> Self {
        Self {
            error: ClassicGroupPositionExecutionError::TerminalEffect,
            ownership: ClassicGroupPositionRecoveryOwnership::PostCore {
                key,
                machine,
                correlation,
                accepted,
                result_buffer,
                effect,
            },
            completion: None,
        }
    }

    pub(in crate::consumer::group) const fn error(&self) -> ClassicGroupPositionExecutionError {
        self.error
    }

    #[cfg(test)]
    pub(in crate::consumer::group) const fn completion_observation(
        &self,
    ) -> Option<GroupPositionOffsetFetchCompletionObservation> {
        self.completion
    }

    pub(in crate::consumer::group) fn retained_owner_count(&self) -> usize {
        match &self.ownership {
            ClassicGroupPositionRecoveryOwnership::Key(key) => {
                let _ = key;
            }
            ClassicGroupPositionRecoveryOwnership::Fence(fence) => {
                let _ = fence;
            }
            ClassicGroupPositionRecoveryOwnership::RawTerminal(terminal) => {
                let _ = terminal;
            }
            ClassicGroupPositionRecoveryOwnership::Terminal {
                terminal,
                application,
            } => {
                let _ = (terminal, application);
            }
            ClassicGroupPositionRecoveryOwnership::PostCore {
                key,
                machine,
                correlation,
                accepted,
                result_buffer,
                effect,
            } => {
                let _ = (key, machine, correlation, accepted, result_buffer, effect);
            }
        }
        let _ = self
            .completion
            .map(GroupPositionOffsetFetchCompletionObservation::kind);
        1
    }
}
