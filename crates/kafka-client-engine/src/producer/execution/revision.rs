//! Preflighted revocation of exact prepared bytes and submission deadlines.

use kafka_client_core::BatchExecutionId;

use super::{PreparedExecution, PreparedExecutionError, PreparedProduceError, ScheduledDeadline};

/// Prepared mechanism required by the exact pending-effect phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedRevisionExpectation {
    Absent,
    Unarmed,
    Armed,
}

/// Linear proof that exact prepared ownership can be revoked without failure.
#[derive(Debug)]
#[must_use = "a preflighted prepared revision must be committed or abandoned"]
pub(crate) struct PreparedRevisionPlan {
    execution: BatchExecutionId,
    removal: Option<PreparedRemoval>,
}

#[derive(Debug)]
struct PreparedRemoval {
    next_retained_bytes: usize,
    scheduled: Option<ScheduledDeadline>,
}

impl PreparedExecution {
    pub(crate) fn plan_revision(
        &self,
        execution: BatchExecutionId,
        expected: PreparedRevisionExpectation,
    ) -> Result<PreparedRevisionPlan, PreparedExecutionError> {
        let batch_id = execution.batch_id();
        let entry = self.entries.get(&batch_id);
        let batch_schedule_count = self
            .schedule
            .iter()
            .filter(|scheduled| scheduled.execution.batch_id() == batch_id)
            .count();
        if expected == PreparedRevisionExpectation::Absent {
            return if entry.is_none() && batch_schedule_count == 0 {
                Ok(PreparedRevisionPlan {
                    execution,
                    removal: None,
                })
            } else {
                Err(state_mismatch(execution, expected))
            };
        }
        let Some(entry) = entry.filter(|entry| entry.execution == execution) else {
            return Err(state_mismatch(execution, expected));
        };
        let scheduled = entry.submission.map(|submission| ScheduledDeadline {
            deadline: submission.deadline.core(),
            execution,
        });
        let state_matches = match expected {
            PreparedRevisionExpectation::Absent => false,
            PreparedRevisionExpectation::Unarmed => {
                entry.submission.is_none() && batch_schedule_count == 0
            }
            PreparedRevisionExpectation::Armed => {
                scheduled.is_some_and(|deadline| self.schedule.contains(&deadline))
                    && batch_schedule_count == 1
            }
        };
        if !state_matches {
            return Err(state_mismatch(execution, expected));
        }
        let next_retained_bytes = self
            .retained_bytes
            .checked_sub(entry.materialized.retained_record_bytes())
            .ok_or(PreparedExecutionError::Prepared(
                PreparedProduceError::EncodedByteOverflow,
            ))?;
        Ok(PreparedRevisionPlan {
            execution,
            removal: Some(PreparedRemoval {
                next_retained_bytes,
                scheduled,
            }),
        })
    }

    pub(crate) fn commit_revision(&mut self, plan: PreparedRevisionPlan) {
        let Some(removal) = plan.removal else {
            return;
        };
        if let Some(scheduled) = removal.scheduled {
            let removed = self.schedule.remove(&scheduled);
            debug_assert!(removed);
        }
        let removed = self.entries.remove(&plan.execution.batch_id());
        debug_assert!(removed.is_some());
        self.retained_bytes = removal.next_retained_bytes;
    }
}

const fn state_mismatch(
    execution: BatchExecutionId,
    expected: PreparedRevisionExpectation,
) -> PreparedExecutionError {
    PreparedExecutionError::RevisionStateMismatch {
        execution,
        expected,
    }
}
