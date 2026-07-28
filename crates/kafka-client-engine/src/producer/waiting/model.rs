//! Linear records, completion generations, and race-fenced waiting tokens.

use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{OperationId, ProducerWaiterId};

use super::super::ProducerRecord;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WaitingTokenState {
    Waiting,
    Promoting,
    Accepted(OperationId),
    Settled,
}

/// Cross-boundary race fence for abandonment and active promotion.
#[derive(Debug)]
pub(crate) struct WaitingToken {
    state: Mutex<WaitingTokenState>,
    cancellation_requested: AtomicBool,
}

impl WaitingToken {
    pub(super) const fn new() -> Self {
        Self {
            state: Mutex::new(WaitingTokenState::Waiting),
            cancellation_requested: AtomicBool::new(false),
        }
    }

    pub(in crate::producer) fn request_abandonment(&self) -> bool {
        match self.state.try_lock() {
            Ok(state) => match *state {
                WaitingTokenState::Waiting | WaitingTokenState::Promoting => {
                    self.cancellation_requested.store(true, Ordering::Release);
                    true
                }
                WaitingTokenState::Accepted(_) | WaitingTokenState::Settled => false,
            },
            Err(TryLockError::WouldBlock) => {
                self.cancellation_requested.store(true, Ordering::Release);
                true
            }
            Err(TryLockError::Poisoned(_)) => false,
        }
    }

    pub(super) fn cancellation_requested(&self) -> bool {
        self.cancellation_requested.load(Ordering::Acquire)
    }

    pub(super) fn lock(
        &self,
    ) -> std::sync::LockResult<std::sync::MutexGuard<'_, WaitingTokenState>> {
        self.state.lock()
    }
}

#[derive(Debug)]
pub(super) struct WaitingEntry {
    pub(super) id: ProducerWaiterId,
    pub(super) operation_id: OperationId,
    pub(super) record: ProducerRecord,
    pub(super) topic_id: kafka_client_core::TopicId,
    pub(super) token: Arc<WaitingToken>,
}

/// Engine counterpart to core waiting policy for byte-owning queued records.
#[derive(Debug)]
pub(in crate::producer) struct ProducerWaitingStore {
    pub(super) entries: VecDeque<WaitingEntry>,
}

impl ProducerWaitingStore {
    pub(in crate::producer) fn new(capacity: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(capacity),
        }
    }

    pub(super) fn push(&mut self, entry: WaitingEntry) {
        self.entries.push_back(entry);
    }

    pub(super) fn remove(&mut self, id: ProducerWaiterId) -> Option<WaitingEntry> {
        let index = self.entries.iter().position(|entry| entry.id == id)?;
        self.entries.remove(index)
    }

    pub(super) fn first_cancelled(&self) -> Option<ProducerWaiterId> {
        self.entries
            .iter()
            .find(|entry| entry.token.cancellation_requested())
            .map(|entry| entry.id)
    }

    pub(in crate::producer) fn clear_terminal(&mut self) {
        self.entries.clear();
    }

    pub(super) fn restore_front(&mut self, entry: WaitingEntry) {
        self.entries.push_front(entry);
    }
}

/// Waiting-partition ownership visible in host stats.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerWaitingStats {
    pub(crate) records: usize,
    pub(crate) bytes: kafka_client_core::ByteCount,
    pub(crate) terminal_bindings: usize,
}
