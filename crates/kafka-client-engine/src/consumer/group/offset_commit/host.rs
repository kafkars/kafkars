//! Sole retained-state owner for private classic-group offset commits.

use core::mem::size_of;

use kafka_client_core::{
    GroupAssignmentPartition, GroupOffsetCommitEffect, GroupOffsetCommitMachine,
    GroupOffsetCommitTerminal, OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::{CompletionId, CompletionObserver, CompletionRegistry, NotifierJoin},
    driver::{GroupOffsetCommitShutdownRecovery, TrackedGroupOffsetCommitCalls},
    protocol::consumer::{
        GroupOffsetCommitPreparationError, PreparedGroupOffsetCommit,
        PreparedGroupOffsetCommitRequest,
    },
};

pub(super) use super::error::GroupOffsetCommitHostError;

pub(super) const GROUP_OFFSET_COMMIT_CAPACITY: usize = 8;
pub(super) const GROUP_OFFSET_COMMIT_OPERATION_BYTES: usize = 256 * 1024;
pub(super) const GROUP_OFFSET_COMMIT_RETAINED_BYTES: usize =
    GROUP_OFFSET_COMMIT_CAPACITY * GROUP_OFFSET_COMMIT_OPERATION_BYTES;

/// Accepted observation lane plus an invariant fault discovered after admission.
pub(super) struct AcceptedGroupOffsetCommit {
    pub(super) observer: CompletionObserver<GroupOffsetCommitTerminal>,
    pub(super) fault: Option<GroupOffsetCommitHostError>,
}

pub(super) struct GroupOffsetCommitOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: GroupOffsetCommitMachine,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) attempt: Option<GroupOffsetCommitAttempt>,
    pub(super) terminal: Option<GroupOffsetCommitTerminal>,
    pub(super) byte_charge: usize,
}

impl GroupOffsetCommitOperation {
    pub(super) fn replace_attempt(
        &mut self,
        attempt: Option<GroupOffsetCommitAttempt>,
    ) -> Option<GroupOffsetCommitAttempt> {
        core::mem::replace(&mut self.attempt, attempt)
    }

    pub(super) fn replace_terminal(
        &mut self,
        terminal: Option<GroupOffsetCommitTerminal>,
    ) -> Option<GroupOffsetCommitTerminal> {
        core::mem::replace(&mut self.terminal, terminal)
    }
}

#[must_use = "a queued commit submission owns correlation and generated request together"]
pub(super) struct GroupOffsetCommitSubmission {
    pub(super) prepared: PreparedGroupOffsetCommit,
    pub(super) request: PreparedGroupOffsetCommitRequest,
}

#[must_use = "an operation attempt must remain in exactly one execution stage"]
pub(super) enum GroupOffsetCommitAttempt {
    Queued(GroupOffsetCommitSubmission),
    HandedOff,
    Recovery(PreparedGroupOffsetCommit),
}

#[must_use = "an accepted invariant fault retains every preparation owner"]
pub(super) enum GroupOffsetCommitPreparationFault {
    Ready(GroupOffsetCommitSubmission),
    Protocol {
        error: GroupOffsetCommitPreparationError,
        request: PreparedGroupOffsetCommitRequest,
    },
    #[cfg(test)]
    RetainedByteOverflowForTest,
}

impl GroupOffsetCommitPreparationFault {
    pub(super) fn retained_bytes(&self) -> Option<usize> {
        match self {
            Self::Ready(submission) => submission
                .prepared
                .retained_bytes()?
                .checked_add(submission.request.retained_bytes()),
            Self::Protocol { error, request } => {
                let _kind = error.kind();
                Some(request.retained_bytes())
            }
            #[cfg(test)]
            Self::RetainedByteOverflowForTest => None,
        }
    }
}

#[must_use = "a settlement fault retains the exact normalized core input"]
pub(super) struct GroupOffsetCommitSettlementFault {
    pub(super) operation_id: OperationId,
    pub(super) input: kafka_client_core::GroupOffsetCommitInput,
    pub(super) provenance: GroupOffsetCommitSettlementProvenance,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupOffsetCommitSettlementProvenance {
    DefinitelyUnsent,
    TransportOwned,
}

/// Result of one bounded nonblocking host turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupOffsetCommitTurn {
    Idle,
    Progress,
}

/// Concrete private owner joining catalog, core, driver, bytes, and completion.
pub(super) struct GroupOffsetCommitHost {
    pub(super) operations: Vec<GroupOffsetCommitOperation>,
    pub(super) calls: TrackedGroupOffsetCommitCalls,
    pub(super) completions: CompletionRegistry<GroupOffsetCommitTerminal>,
    pub(super) next_operation_id: Option<OperationId>,
    pub(super) reclaim_pending: Option<CompletionId>,
    pub(super) published_bytes: Vec<(CompletionId, usize)>,
    pub(super) retained_bytes: usize,
    pub(super) accepting: bool,
    pub(super) fault: Option<GroupOffsetCommitHostError>,
    pub(super) preparation_fault: Option<GroupOffsetCommitPreparationFault>,
    pub(super) settlement_fault: Option<GroupOffsetCommitSettlementFault>,
    pub(super) shutdown_recovery: Option<GroupOffsetCommitShutdownRecovery>,
    pub(super) effect_fault: Option<GroupOffsetCommitEffect>,
    pub(super) recovery_faults: Vec<PreparedGroupOffsetCommit>,
}

impl GroupOffsetCommitHost {
    pub(super) fn start() -> std::io::Result<Self> {
        let mut operations = Vec::new();
        operations
            .try_reserve_exact(GROUP_OFFSET_COMMIT_CAPACITY)
            .map_err(|_error| std::io::Error::other("group commit operation reservation failed"))?;
        let mut published_bytes = Vec::new();
        published_bytes
            .try_reserve_exact(GROUP_OFFSET_COMMIT_CAPACITY)
            .map_err(|_error| {
                std::io::Error::other("group commit publication reservation failed")
            })?;
        let mut recovery_faults = Vec::new();
        recovery_faults
            .try_reserve_exact(GROUP_OFFSET_COMMIT_CAPACITY)
            .map_err(|_error| std::io::Error::other("group commit recovery reservation failed"))?;
        Ok(Self {
            operations,
            calls: TrackedGroupOffsetCommitCalls::try_new(GROUP_OFFSET_COMMIT_CAPACITY)
                .map_err(|_error| std::io::Error::other("group commit call reservation failed"))?,
            completions: CompletionRegistry::start(GROUP_OFFSET_COMMIT_CAPACITY)?,
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            published_bytes,
            retained_bytes: 0,
            accepting: true,
            fault: None,
            preparation_fault: None,
            settlement_fault: None,
            shutdown_recovery: None,
            effect_fault: None,
            recovery_faults,
        })
    }

    pub(super) fn close_admission(&mut self) {
        self.accepting = false;
    }

    pub(super) fn is_available_for_admission(&self) -> bool {
        self.fault.is_none()
            && self.preparation_fault.is_none()
            && self.settlement_fault.is_none()
            && self.shutdown_recovery.is_none()
            && self.effect_fault.is_none()
            && self.recovery_faults.is_empty()
    }

    pub(super) fn has_effect_fault(&self) -> bool {
        self.effect_fault.is_some()
    }

    pub(super) fn finish_shutdown(&mut self) -> Result<NotifierJoin, GroupOffsetCommitHostError> {
        if self.accepting
            || !self.operations.is_empty()
            || self.calls.retained_group_commit_count() != 0
            || self.fault.is_some()
            || self.preparation_fault.is_some()
            || self.settlement_fault.is_some()
            || self.shutdown_recovery.is_some()
            || self.effect_fault.is_some()
            || !self.recovery_faults.is_empty()
        {
            return Err(GroupOffsetCommitHostError::Unsettled);
        }
        self.completions
            .stop_notifier()
            .map_err(GroupOffsetCommitHostError::Completion)
    }

    pub(super) fn actual_operation_bytes(
        machine: &GroupOffsetCommitMachine,
        prepared: &PreparedGroupOffsetCommit,
        request: &PreparedGroupOffsetCommitRequest,
    ) -> Option<usize> {
        machine
            .expected_capacity()
            .checked_mul(size_of::<GroupAssignmentPartition>())?
            .checked_add(prepared.retained_bytes()?)
            .and_then(|bytes| bytes.checked_add(request.retained_bytes()))
    }

    pub(super) fn operation_index(&self, operation_id: OperationId) -> Option<usize> {
        self.operations
            .iter()
            .position(|operation| operation.operation_id == operation_id)
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }
}
