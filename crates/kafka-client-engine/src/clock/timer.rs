//! Ordered execution storage for core-owned producer batch timer policy.

use std::collections::{BTreeMap, BTreeSet};

use kafka_client_core::{BatchId, BatchTimerGeneration, Deadline, Moment};

use super::BatchTimerError;

/// One due timer fact ready to return to deterministic producer policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DueBatchTimer {
    deadline: Deadline,
    batch_id: BatchId,
    generation: BatchTimerGeneration,
}

impl DueBatchTimer {
    /// Returns the absolute deadline that ordered this timer.
    pub(crate) const fn deadline(self) -> Deadline {
        self.deadline
    }

    /// Returns the core-owned batch identity.
    pub(crate) const fn batch_id(self) -> BatchId {
        self.batch_id
    }

    /// Returns the generation required by `BatchTimerFired`.
    pub(crate) const fn generation(self) -> BatchTimerGeneration {
        self.generation
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ActiveTimer {
    generation: BatchTimerGeneration,
    deadline: Deadline,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ScheduledTimer {
    deadline: Deadline,
    batch_id: BatchId,
    generation: BatchTimerGeneration,
}

/// Unique engine owner of active producer batch timers.
#[derive(Debug)]
pub(crate) struct BatchTimers {
    capacity: usize,
    active: BTreeMap<BatchId, ActiveTimer>,
    schedule: BTreeSet<ScheduledTimer>,
}

impl BatchTimers {
    /// Creates an empty timer owner with a hard active-batch limit.
    pub(crate) const fn new(capacity: usize) -> Self {
        Self {
            capacity,
            active: BTreeMap::new(),
            schedule: BTreeSet::new(),
        }
    }

    /// Arms a new timer or replaces an older generation for the same batch.
    pub(crate) fn arm(
        &mut self,
        batch_id: BatchId,
        generation: BatchTimerGeneration,
        deadline: Deadline,
    ) -> Result<bool, BatchTimerError> {
        let candidate = ActiveTimer {
            generation,
            deadline,
        };
        if self
            .active
            .get(&batch_id)
            .is_some_and(|current| generation <= current.generation)
        {
            return Ok(false);
        }
        if !self.active.contains_key(&batch_id) && self.active.len() >= self.capacity {
            return Err(BatchTimerError::capacity(self.capacity));
        }
        if let Some(previous) = self.active.insert(batch_id, candidate) {
            self.schedule.remove(&scheduled(batch_id, previous));
        }
        self.schedule.insert(scheduled(batch_id, candidate));
        Ok(true)
    }

    /// Cancels only the exact active generation and ignores stale facts.
    pub(crate) fn cancel(&mut self, batch_id: BatchId, generation: BatchTimerGeneration) -> bool {
        let Some(current) = self.active.get(&batch_id).copied() else {
            return false;
        };
        if current.generation != generation {
            return false;
        }
        self.active.remove(&batch_id);
        self.schedule.remove(&scheduled(batch_id, current));
        true
    }

    /// Removes at most `limit` due timers in deterministic schedule order.
    pub(crate) fn drain_due(&mut self, now: Moment, limit: usize) -> Vec<DueBatchTimer> {
        let mut due = Vec::with_capacity(limit.min(self.schedule.len()));
        while due.len() < limit {
            let Some(next) = self.schedule.first().copied() else {
                break;
            };
            if !next.deadline.is_elapsed_at(now) {
                break;
            }
            self.schedule.remove(&next);
            if self.active.remove(&next.batch_id)
                == Some(ActiveTimer {
                    generation: next.generation,
                    deadline: next.deadline,
                })
            {
                due.push(DueBatchTimer {
                    deadline: next.deadline,
                    batch_id: next.batch_id,
                    generation: next.generation,
                });
            }
        }
        due
    }

    /// Returns the earliest active timer without reading the ambient clock.
    pub(crate) fn next_deadline(&self) -> Option<Deadline> {
        self.schedule.first().map(|timer| timer.deadline)
    }

    /// Returns active timer count for bounded host observations.
    pub(crate) fn len(&self) -> usize {
        self.active.len()
    }

    /// Returns whether no active batch timer is retained.
    pub(crate) fn is_empty(&self) -> bool {
        self.active.is_empty()
    }

    /// Drops every active timer after permanent owner shutdown.
    pub(crate) fn clear_terminal(&mut self) {
        self.active.clear();
        self.schedule.clear();
    }
}

const fn scheduled(batch_id: BatchId, timer: ActiveTimer) -> ScheduledTimer {
    ScheduledTimer {
        deadline: timer.deadline,
        batch_id,
        generation: timer.generation,
    }
}
