//! Bounded FIFO ownership for ordered producer terminals.

use std::collections::VecDeque;

use kafka_client_core::{FlushId, OperationId, ProducerCompletion};

use crate::completion::CompletionId;

use super::ProducerTerminal;

/// Core owner named by one producer terminal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerTerminalOwner {
    Record(OperationId),
    Flush(FlushId),
}

/// One validated producer terminal awaiting notifier ownership.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum RetainedTerminal {
    Record {
        operation_id: OperationId,
        completion_id: CompletionId,
        completion: ProducerCompletion,
    },
    Flush {
        flush_id: FlushId,
        completion_id: CompletionId,
    },
}

impl RetainedTerminal {
    pub(super) const fn record(
        operation_id: OperationId,
        completion_id: CompletionId,
        completion: ProducerCompletion,
    ) -> Self {
        Self::Record {
            operation_id,
            completion_id,
            completion,
        }
    }

    pub(super) const fn flush(flush_id: FlushId, completion_id: CompletionId) -> Self {
        Self::Flush {
            flush_id,
            completion_id,
        }
    }

    pub(super) const fn owner(&self) -> ProducerTerminalOwner {
        match self {
            Self::Record { operation_id, .. } => ProducerTerminalOwner::Record(*operation_id),
            Self::Flush { flush_id, .. } => ProducerTerminalOwner::Flush(*flush_id),
        }
    }

    pub(super) const fn completion_id(&self) -> CompletionId {
        match self {
            Self::Record { completion_id, .. } | Self::Flush { completion_id, .. } => {
                *completion_id
            }
        }
    }

    pub(super) const fn terminal(&self) -> ProducerTerminal {
        match self {
            Self::Record { completion, .. } => ProducerTerminal::record(*completion),
            Self::Flush { .. } => ProducerTerminal::flush_completed(),
        }
    }

    #[cfg(test)]
    pub(super) const fn record_completion(&self) -> Option<ProducerCompletion> {
        match self {
            Self::Record { completion, .. } => Some(*completion),
            Self::Flush { .. } => None,
        }
    }
}

/// Linear fixed-capacity owner preserving producer terminal publication order.
#[derive(Debug)]
pub(super) struct OrderedTerminalBacklog {
    capacity: usize,
    entries: VecDeque<RetainedTerminal>,
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
        self.entries.push_back(terminal);
    }

    pub(super) fn pop_published(&mut self) -> Option<RetainedTerminal> {
        self.entries.pop_front()
    }

    pub(super) fn front(&self) -> Option<&RetainedTerminal> {
        self.entries.front()
    }

    #[cfg(test)]
    pub(super) fn back(&self) -> Option<&RetainedTerminal> {
        self.entries.back()
    }

    pub(super) fn contains_owner(&self, owner: ProducerTerminalOwner) -> bool {
        self.entries.iter().any(|entry| entry.owner() == owner)
    }

    pub(super) fn contains_completion(&self, completion_id: CompletionId) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.completion_id() == completion_id)
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
