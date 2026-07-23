//! Immutable first-poison terminal evidence and ownership-returning refusal.

use kafka_client_core::{OperationId, ProducerCompletion};

use crate::completion::CompletionId;

use super::super::super::ProducerHostInvariantError;

/// A terminal rejected from the normal FIFO, retained as corruption evidence.
#[derive(Debug, Eq, PartialEq)]
pub(in crate::producer) struct RejectedTerminal {
    operation_id: OperationId,
    completion_id: Option<CompletionId>,
    completion: ProducerCompletion,
    reason: ProducerHostInvariantError,
}

impl RejectedTerminal {
    pub(in crate::producer) const fn new(
        operation_id: OperationId,
        completion_id: Option<CompletionId>,
        completion: ProducerCompletion,
        reason: ProducerHostInvariantError,
    ) -> Self {
        Self {
            operation_id,
            completion_id,
            completion,
            reason,
        }
    }

    pub(in crate::producer) const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    pub(in crate::producer) const fn completion_id(&self) -> Option<CompletionId> {
        self.completion_id
    }

    #[cfg(test)]
    pub(in crate::producer) const fn completion(&self) -> ProducerCompletion {
        self.completion
    }

    pub(in crate::producer) const fn reason(&self) -> ProducerHostInvariantError {
        self.reason
    }
}

/// Ownership-carrying refusal to overwrite the first poison evidence.
#[derive(Debug)]
pub(in crate::producer) struct PoisonRetentionFailure {
    evidence: RejectedTerminal,
}

impl PoisonRetentionFailure {
    pub(super) const fn new(evidence: RejectedTerminal) -> Self {
        Self { evidence }
    }

    pub(in crate::producer) const fn evidence(&self) -> &RejectedTerminal {
        &self.evidence
    }

    pub(in crate::producer) fn into_evidence(self) -> RejectedTerminal {
        self.evidence
    }
}

/// Single immutable first-evidence owner reached only while poisoning the host.
#[derive(Debug)]
pub(in crate::producer) struct TerminalPoisonSlot {
    retained: Option<RejectedTerminal>,
}

impl TerminalPoisonSlot {
    pub(in crate::producer) const fn empty() -> Self {
        Self { retained: None }
    }

    pub(in crate::producer) fn retain(
        &mut self,
        evidence: RejectedTerminal,
    ) -> Result<(), PoisonRetentionFailure> {
        if self.retained.is_some() {
            return Err(PoisonRetentionFailure::new(evidence));
        }
        self.retained = Some(evidence);
        Ok(())
    }

    pub(in crate::producer) const fn evidence(&self) -> Option<&RejectedTerminal> {
        self.retained.as_ref()
    }

    pub(in crate::producer) fn clear_terminal(&mut self) {
        self.retained = None;
    }

    pub(in crate::producer) fn len(&self) -> usize {
        usize::from(self.retained.is_some())
    }
}
