//! Ownership-returning quarantine refusals and their single recovery owner.

use kafka_client_core::{ProducerEffect, ProducerInput};

use super::{PoisonRetentionFailure, TailQuarantineError};

/// A refused mechanism tail whose exact tokens remain caller-owned.
#[derive(Debug)]
pub(in crate::producer) struct TailQuarantineFailure {
    error: TailQuarantineError,
    tail: Vec<ProducerEffect>,
}

impl TailQuarantineFailure {
    pub(super) const fn new(error: TailQuarantineError, tail: Vec<ProducerEffect>) -> Self {
        Self { error, tail }
    }

    #[cfg(test)]
    pub(in crate::producer) const fn error(&self) -> TailQuarantineError {
        self.error
    }

    pub(in crate::producer) fn into_tail(self) -> Vec<ProducerEffect> {
        self.tail
    }

    pub(in crate::producer) fn len(&self) -> usize {
        self.tail.len()
    }
}

/// Refused generated facts whose exact values remain caller-owned.
#[derive(Debug)]
pub(in crate::producer) struct GeneratedQuarantineFailure {
    error: TailQuarantineError,
    generated: Vec<ProducerInput>,
}

impl GeneratedQuarantineFailure {
    pub(super) const fn new(error: TailQuarantineError, generated: Vec<ProducerInput>) -> Self {
        Self { error, generated }
    }

    #[cfg(test)]
    pub(in crate::producer) const fn error(&self) -> TailQuarantineError {
        self.error
    }

    pub(in crate::producer) fn into_generated(self) -> Vec<ProducerInput> {
        self.generated
    }

    pub(in crate::producer) fn len(&self) -> usize {
        self.generated.len()
    }
}

/// Vacant fixed slot proving a tail refusal cannot overwrite prior evidence.
pub(in crate::producer) struct TailRefusalVacancy<'a> {
    slot: &'a mut Option<TailQuarantineFailure>,
}

impl TailRefusalVacancy<'_> {
    pub(in crate::producer) fn retain(self, failure: TailQuarantineFailure) {
        *self.slot = Some(failure);
    }
}

/// Vacant fixed slot proving generated facts cannot overwrite prior evidence.
pub(in crate::producer) struct GeneratedRefusalVacancy<'a> {
    slot: &'a mut Option<GeneratedQuarantineFailure>,
}

impl GeneratedRefusalVacancy<'_> {
    pub(in crate::producer) fn retain(self, failure: GeneratedQuarantineFailure) {
        *self.slot = Some(failure);
    }
}

/// Vacant fixed slot preserving an exact typed terminal refusal.
pub(in crate::producer) struct TerminalRefusalVacancy<'a> {
    slot: &'a mut Option<PoisonRetentionFailure>,
}

impl TerminalRefusalVacancy<'_> {
    pub(in crate::producer) fn retain(self, failure: PoisonRetentionFailure) {
        *self.slot = Some(failure);
    }
}

/// Three fixed refusal slots reached once behind the immutable poison fence.
#[derive(Debug)]
pub(in crate::producer) struct TerminalRefusalOwner {
    tail: Option<TailQuarantineFailure>,
    generated: Option<GeneratedQuarantineFailure>,
    terminal: Option<PoisonRetentionFailure>,
}

impl TerminalRefusalOwner {
    pub(in crate::producer) const fn empty() -> Self {
        Self {
            tail: None,
            generated: None,
            terminal: None,
        }
    }

    pub(in crate::producer) fn tail_vacancy(&mut self) -> Option<TailRefusalVacancy<'_>> {
        if self.tail.is_some() {
            return None;
        }
        Some(TailRefusalVacancy {
            slot: &mut self.tail,
        })
    }

    pub(in crate::producer) fn generated_vacancy(&mut self) -> Option<GeneratedRefusalVacancy<'_>> {
        if self.generated.is_some() {
            return None;
        }
        Some(GeneratedRefusalVacancy {
            slot: &mut self.generated,
        })
    }

    pub(in crate::producer) fn terminal_vacancy(&mut self) -> Option<TerminalRefusalVacancy<'_>> {
        if self.terminal.is_some() {
            return None;
        }
        Some(TerminalRefusalVacancy {
            slot: &mut self.terminal,
        })
    }

    pub(in crate::producer) fn retained_len(&self) -> usize {
        self.tail
            .as_ref()
            .map_or(0, TailQuarantineFailure::len)
            .saturating_add(
                self.generated
                    .as_ref()
                    .map_or(0, GeneratedQuarantineFailure::len),
            )
            .saturating_add(usize::from(self.terminal.is_some()))
    }

    #[cfg(test)]
    pub(in crate::producer) fn terminal(&self) -> Option<&super::RejectedTerminal> {
        self.terminal.as_ref().map(PoisonRetentionFailure::evidence)
    }

    pub(in crate::producer) fn clear_terminal(&mut self) {
        self.tail = None;
        self.generated = None;
        self.terminal = None;
    }
}
