//! Deterministic FIFO policy for bounded producer calls waiting on active capacity.

use std::collections::VecDeque;

use crate::{ByteCount, Deadline, Moment};

/// Stable identity for one call that has not crossed active producer admission.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProducerWaiterId(u64);

impl ProducerWaiterId {
    /// Restores an engine-owned waiter identity.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw deterministic identity.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Bytes-free deterministic facts for one queued producer caller.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerWaiter {
    id: ProducerWaiterId,
    deadline: Deadline,
    retained_bytes: ByteCount,
}

impl ProducerWaiter {
    /// Returns this queue generation.
    pub const fn id(self) -> ProducerWaiterId {
        self.id
    }

    /// Returns the original public-call deadline.
    pub const fn deadline(self) -> Deadline {
        self.deadline
    }

    /// Returns bytes owned by the engine waiting partition.
    pub const fn retained_bytes(self) -> ByteCount {
        self.retained_bytes
    }
}

/// Normal bounded rejection before a waiting caller transfers ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerWaitingAdmissionError {
    /// Waiting admission has closed.
    Closed,
    /// The original public deadline already elapsed.
    DeadlineElapsed,
    /// Every waiting record slot is retained.
    RecordCapacity,
    /// Waiting bytes would exceed their independent bound.
    ByteCapacity,
    /// Retained-byte arithmetic exceeded the core domain.
    ByteCountOverflow,
    /// The bounded waiter identity domain is exhausted.
    IdentityExhausted,
}

/// Sole deterministic owner of waiting order, deadlines, count, and bytes.
#[derive(Debug)]
pub struct ProducerWaitingQueue {
    max_records: usize,
    max_bytes: ByteCount,
    used_bytes: ByteCount,
    next_id: Option<ProducerWaiterId>,
    open: bool,
    entries: VecDeque<ProducerWaiter>,
}

impl ProducerWaitingQueue {
    /// Creates an empty waiting policy with independent count and byte bounds.
    pub fn new(max_records: usize, max_bytes: ByteCount) -> Self {
        Self {
            max_records,
            max_bytes,
            used_bytes: ByteCount::new(0),
            next_id: Some(ProducerWaiterId::from_raw(1)),
            open: true,
            entries: VecDeque::with_capacity(max_records),
        }
    }

    /// Transfers one caller into deterministic FIFO ownership.
    pub fn admit(
        &mut self,
        now: Moment,
        deadline: Deadline,
        retained_bytes: ByteCount,
    ) -> Result<ProducerWaiterId, ProducerWaitingAdmissionError> {
        if !self.open {
            return Err(ProducerWaitingAdmissionError::Closed);
        }
        if deadline.is_elapsed_at(now) {
            return Err(ProducerWaitingAdmissionError::DeadlineElapsed);
        }
        if self.entries.len() >= self.max_records {
            return Err(ProducerWaitingAdmissionError::RecordCapacity);
        }
        let Some(next_bytes) = self.used_bytes.checked_add(retained_bytes) else {
            return Err(ProducerWaitingAdmissionError::ByteCountOverflow);
        };
        if next_bytes > self.max_bytes {
            return Err(ProducerWaitingAdmissionError::ByteCapacity);
        }
        let Some(id) = self.next_id else {
            return Err(ProducerWaitingAdmissionError::IdentityExhausted);
        };
        self.next_id = id.get().checked_add(1).map(ProducerWaiterId::from_raw);
        self.used_bytes = next_bytes;
        self.entries.push_back(ProducerWaiter {
            id,
            deadline,
            retained_bytes,
        });
        Ok(id)
    }

    /// Returns the FIFO head without changing ownership.
    pub fn front(&self) -> Option<ProducerWaiter> {
        self.entries.front().copied()
    }

    /// Removes one exact waiter for promotion, timeout, cancellation, or close.
    pub fn remove(&mut self, id: ProducerWaiterId) -> Option<ProducerWaiter> {
        let index = self.entries.iter().position(|entry| entry.id == id)?;
        let removed = self.entries.remove(index)?;
        self.used_bytes = self
            .used_bytes
            .checked_sub(removed.retained_bytes)
            .unwrap_or_else(|| unreachable!("waiting byte ownership underflow"));
        Some(removed)
    }

    /// Returns the first elapsed waiter in deterministic FIFO order.
    pub fn first_elapsed(&self, now: Moment) -> Option<ProducerWaiter> {
        self.entries
            .iter()
            .copied()
            .find(|entry| entry.deadline.is_elapsed_at(now))
    }

    /// Returns the earliest original public deadline.
    pub fn next_deadline(&self) -> Option<Deadline> {
        self.entries.iter().map(|entry| entry.deadline).min()
    }

    /// Permanently fences new waiting admission.
    pub const fn close(&mut self) {
        self.open = false;
    }

    /// Returns whether new callers may enter this queue.
    pub const fn admission_is_open(&self) -> bool {
        self.open
    }

    /// Returns the retained waiting caller count.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns waiting bytes independent from active producer bytes.
    pub const fn retained_bytes(&self) -> ByteCount {
        self.used_bytes
    }

    /// Returns whether no caller remains queued.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
