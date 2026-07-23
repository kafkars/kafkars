//! Bounded pre-driver deadline ownership for materialized producer batches.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
};

use kafka_client_core::{BatchId, Deadline, Moment, OperationId, ProducerInput};

/// Failure to retain a core-declared submission deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SubmissionDeadlineError {
    /// Every configured pre-driver deadline slot is occupied.
    Capacity {
        /// Configured active deadline limit.
        limit: usize,
    },
    /// The batch already owns different core-declared deadline facts.
    ConflictingBatch {
        /// Batch whose second arm disagreed with the first.
        batch_id: BatchId,
    },
}

impl fmt::Display for SubmissionDeadlineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capacity { limit } => {
                write!(formatter, "submission deadline capacity {limit} is full")
            }
            Self::ConflictingBatch { batch_id } => {
                write!(
                    formatter,
                    "batch {} already owns a different submission deadline",
                    batch_id.get()
                )
            }
        }
    }
}

impl Error for SubmissionDeadlineError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ActiveDeadline {
    operation_id: OperationId,
    deadline: Deadline,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ScheduledDeadline {
    deadline: Deadline,
    batch_id: BatchId,
}

/// One elapsed pre-driver deadline fact ready for deterministic core policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DueSubmissionDeadline {
    batch_id: BatchId,
    operation_id: OperationId,
    deadline: Deadline,
    observed_at: Moment,
}

impl DueSubmissionDeadline {
    /// Returns the batch that had not crossed driver ownership.
    pub(crate) const fn batch_id(self) -> BatchId {
        self.batch_id
    }

    /// Returns the core-selected member owning the batch deadline.
    pub(crate) const fn operation_id(self) -> OperationId {
        self.operation_id
    }

    /// Returns the unchanged absolute public deadline.
    pub(crate) const fn deadline(self) -> Deadline {
        self.deadline
    }

    /// Returns the host-supplied monotonic observation that found it due.
    pub(crate) const fn observed_at(self) -> Moment {
        self.observed_at
    }

    /// Converts stored mechanism facts into the exact core deadline input.
    pub(crate) const fn into_input(self) -> ProducerInput {
        ProducerInput::DeadlineElapsed {
            operation_id: self.operation_id,
            now: self.observed_at,
        }
    }
}

/// Unique bounded owner of batches awaiting synchronous driver acceptance.
#[derive(Debug)]
pub(crate) struct SubmissionDeadlines {
    capacity: usize,
    active: BTreeMap<BatchId, ActiveDeadline>,
    schedule: BTreeSet<ScheduledDeadline>,
}

impl SubmissionDeadlines {
    /// Creates an empty owner with a fixed active-batch limit.
    pub(crate) const fn new(capacity: usize) -> Self {
        Self {
            capacity,
            active: BTreeMap::new(),
            schedule: BTreeSet::new(),
        }
    }

    /// Arms one core-selected batch deadline without replacing policy facts.
    ///
    /// Replaying the exact same effect is idempotent. Any disagreement for an
    /// already active batch is an invariant error rather than a replacement.
    pub(crate) fn arm(
        &mut self,
        batch_id: BatchId,
        operation_id: OperationId,
        deadline: Deadline,
    ) -> Result<bool, SubmissionDeadlineError> {
        let candidate = ActiveDeadline {
            operation_id,
            deadline,
        };
        if let Some(current) = self.active.get(&batch_id) {
            return if *current == candidate {
                Ok(false)
            } else {
                Err(SubmissionDeadlineError::ConflictingBatch { batch_id })
            };
        }
        if self.active.len() >= self.capacity {
            return Err(SubmissionDeadlineError::Capacity {
                limit: self.capacity,
            });
        }
        self.active.insert(batch_id, candidate);
        self.schedule
            .insert(ScheduledDeadline { deadline, batch_id });
        Ok(true)
    }

    /// Cancels only the named batch after driver acceptance or core settlement.
    pub(crate) fn cancel(&mut self, batch_id: BatchId) -> bool {
        let Some(active) = self.active.remove(&batch_id) else {
            return false;
        };
        self.schedule.remove(&ScheduledDeadline {
            deadline: active.deadline,
            batch_id,
        });
        true
    }

    /// Removes bounded due entries in `(Deadline, BatchId)` order.
    pub(crate) fn drain_due(&mut self, now: Moment, limit: usize) -> Vec<DueSubmissionDeadline> {
        let mut due = Vec::with_capacity(limit.min(self.active.len()));
        while let Some(next) = self.schedule.first().copied() {
            if due.len() >= limit {
                break;
            }
            if !next.deadline.is_elapsed_at(now) {
                break;
            }
            self.schedule.remove(&next);
            let Some(active) = self.active.remove(&next.batch_id) else {
                continue;
            };
            if active.deadline == next.deadline {
                due.push(DueSubmissionDeadline {
                    batch_id: next.batch_id,
                    operation_id: active.operation_id,
                    deadline: active.deadline,
                    observed_at: now,
                });
            }
        }
        due
    }

    /// Returns the next core-declared deadline without consulting ambient time.
    pub(crate) fn next_deadline(&self) -> Option<Deadline> {
        self.schedule.first().map(|entry| entry.deadline)
    }

    /// Returns active pre-driver batch count.
    pub(crate) fn len(&self) -> usize {
        self.active.len()
    }

    /// Returns whether no pre-driver deadline is retained.
    pub(crate) fn is_empty(&self) -> bool {
        self.active.is_empty()
    }
}
