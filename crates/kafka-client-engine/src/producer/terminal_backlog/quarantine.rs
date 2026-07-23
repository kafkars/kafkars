//! Poison evidence and configured-bound committed-tail quarantine ownership.

use std::collections::VecDeque;

use kafka_client_core::{ProducerEffect, ProducerInput, producer_transition_effect_capacity};

use super::super::ProducerHostLimitError;

mod poison;
#[cfg(test)]
mod poison_test;
mod refusal;
#[cfg(test)]
mod refusal_test;

pub(in crate::producer) use poison::{
    PoisonRetentionFailure, RejectedTerminal, TerminalPoisonSlot,
};
pub(in crate::producer) use refusal::{
    GeneratedQuarantineFailure, TailQuarantineFailure, TerminalRefusalOwner,
};

/// Why an exact committed tail entered the overflow evidence slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::producer) enum TailQuarantineError {
    Capacity,
    DuplicateTail,
}

/// Exact invalid terminals, one mechanism tail, and unapplied generated facts.
#[derive(Debug)]
pub(in crate::producer) struct TerminalQuarantine {
    terminal_capacity: usize,
    terminals: VecDeque<RejectedTerminal>,
    tail_capacity: usize,
    committed_tail: Option<Vec<ProducerEffect>>,
    generated_capacity: usize,
    generated: Option<Vec<ProducerInput>>,
}

impl TerminalQuarantine {
    pub(in crate::producer) fn for_capacities(
        record_capacity: usize,
        flush_capacity: usize,
    ) -> Result<Self, ProducerHostLimitError> {
        let capacity = producer_transition_effect_capacity(record_capacity, flush_capacity)
            .ok_or(ProducerHostLimitError::TerminalTailCapacityOverflow)?;
        Ok(Self::new(capacity, capacity))
    }

    pub(in crate::producer) fn new(terminal_capacity: usize, tail_capacity: usize) -> Self {
        Self {
            terminal_capacity,
            terminals: VecDeque::with_capacity(terminal_capacity),
            tail_capacity,
            committed_tail: None,
            generated_capacity: tail_capacity,
            generated: None,
        }
    }

    pub(in crate::producer) const fn transition_effect_capacity(&self) -> usize {
        self.tail_capacity
    }

    /// Transfers the poison slot's refused exact terminal into bounded evidence.
    pub(in crate::producer) fn retain_terminal(
        &mut self,
        failure: PoisonRetentionFailure,
    ) -> Result<(), PoisonRetentionFailure> {
        let evidence = failure.into_evidence();
        if self.terminals.len() >= self.terminal_capacity {
            return Err(PoisonRetentionFailure::new(evidence));
        }
        self.terminals.push_back(evidence);
        Ok(())
    }

    /// Retains the exact untransferred transition remainder without execution.
    pub(in crate::producer) fn retain_committed_tail(
        &mut self,
        tail: Vec<ProducerEffect>,
    ) -> Result<(), TailQuarantineFailure> {
        let error = if tail.len() > self.tail_capacity {
            Some(TailQuarantineError::Capacity)
        } else if self.committed_tail.is_some() {
            Some(TailQuarantineError::DuplicateTail)
        } else {
            None
        };
        if let Some(error) = error {
            return Err(TailQuarantineFailure::new(error, tail));
        }
        self.committed_tail = Some(tail);
        Ok(())
    }

    /// Retains every produced but unapplied deterministic fact after poison.
    pub(in crate::producer) fn retain_generated(
        &mut self,
        generated: Vec<ProducerInput>,
    ) -> Result<(), GeneratedQuarantineFailure> {
        let error = if generated.len() > self.generated_capacity {
            Some(TailQuarantineError::Capacity)
        } else if self.generated.is_some() {
            Some(TailQuarantineError::DuplicateTail)
        } else {
            None
        };
        if let Some(error) = error {
            return Err(GeneratedQuarantineFailure::new(error, generated));
        }
        self.generated = Some(generated);
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::producer) fn first_terminal(&self) -> Option<&RejectedTerminal> {
        self.terminals.front()
    }

    #[cfg(test)]
    pub(in crate::producer) fn terminal_len(&self) -> usize {
        self.terminals.len()
    }

    pub(in crate::producer) fn retained_tail_len(&self) -> usize {
        self.committed_tail
            .as_ref()
            .map_or(0, Vec::len)
            .saturating_add(self.generated.as_ref().map_or(0, Vec::len))
    }

    #[cfg(test)]
    pub(in crate::producer) fn committed_tail(&self) -> Option<&[ProducerEffect]> {
        self.committed_tail.as_deref()
    }

    #[cfg(test)]
    pub(in crate::producer) fn generated(&self) -> Option<&[ProducerInput]> {
        self.generated.as_deref()
    }

    pub(in crate::producer) fn retained_len(&self) -> usize {
        self.terminals
            .len()
            .saturating_add(self.retained_tail_len())
    }

    pub(in crate::producer) fn clear_terminal(&mut self) {
        self.terminals.clear();
        self.committed_tail = None;
        self.generated = None;
    }
}
