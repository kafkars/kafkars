//! Bounded FIFO and quarantine ownership for ordered producer terminals.

mod fatal_buffer;
#[cfg(test)]
mod fatal_buffer_test;
mod quarantine;
#[cfg(test)]
mod quarantine_test;

use std::collections::VecDeque;

use kafka_client_core::{OperationId, ProducerCompletion};

use crate::completion::CompletionId;

pub(super) use fatal_buffer::FatalTransitionBuffer;
pub(super) use quarantine::TerminalRefusalOwner;
pub(super) use quarantine::{RejectedTerminal, TerminalPoisonSlot, TerminalQuarantine};

/// One validated record terminal awaiting notifier ownership.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct RetainedTerminal {
    operation_id: OperationId,
    completion_id: CompletionId,
    completion: ProducerCompletion,
}

impl RetainedTerminal {
    pub(super) const fn new(
        operation_id: OperationId,
        completion_id: CompletionId,
        completion: ProducerCompletion,
    ) -> Self {
        Self {
            operation_id,
            completion_id,
            completion,
        }
    }

    pub(super) const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    pub(super) const fn completion_id(&self) -> CompletionId {
        self.completion_id
    }

    pub(super) const fn completion(&self) -> ProducerCompletion {
        self.completion
    }
}

/// Ordered terminal union reserved for record and future flush publication.
#[derive(Debug, Eq, PartialEq)]
enum OrderedTerminal {
    Record(RetainedTerminal),
}

impl OrderedTerminal {
    const fn record(&self) -> &RetainedTerminal {
        match self {
            Self::Record(terminal) => terminal,
        }
    }
}

/// Linear fixed-capacity owner preserving valid terminal publication order.
#[derive(Debug)]
pub(super) struct OrderedTerminalBacklog {
    capacity: usize,
    entries: VecDeque<OrderedTerminal>,
}

impl OrderedTerminalBacklog {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: VecDeque::with_capacity(capacity),
        }
    }

    pub(super) fn push(&mut self, terminal: RetainedTerminal) {
        debug_assert!(self.entries.len() < self.capacity);
        self.entries.push_back(OrderedTerminal::Record(terminal));
    }

    pub(super) fn pop_published(&mut self) -> Option<RetainedTerminal> {
        self.entries.pop_front().map(|entry| match entry {
            OrderedTerminal::Record(terminal) => terminal,
        })
    }

    pub(super) fn pop_rejected(&mut self) -> Option<RetainedTerminal> {
        self.pop_published()
    }

    pub(super) fn front(&self) -> Option<&RetainedTerminal> {
        self.entries.front().map(OrderedTerminal::record)
    }

    pub(super) fn back(&self) -> Option<&RetainedTerminal> {
        self.entries.back().map(OrderedTerminal::record)
    }

    pub(super) fn contains_operation(&self, operation_id: OperationId) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.record().operation_id == operation_id)
    }

    pub(super) fn contains_completion(&self, completion_id: CompletionId) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.record().completion_id == completion_id)
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
