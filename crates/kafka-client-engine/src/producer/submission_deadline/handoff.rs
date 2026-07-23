//! Exact removal of an original deadline after paired handoff preflight.

use kafka_client_core::{BatchExecutionId, OperationId};

use super::{ScheduledDeadline, SubmissionDeadlines};
use crate::clock::OperationDeadline;

/// Linear proof that one exact active deadline and schedule entry were observed.
#[derive(Debug)]
pub(crate) struct SubmissionDeadlineHandoffPlan {
    active: super::ActiveDeadline,
    scheduled: ScheduledDeadline,
}

impl SubmissionDeadlineHandoffPlan {
    /// Returns the exact sealed execution named by this plan.
    pub(crate) const fn execution(&self) -> BatchExecutionId {
        self.active.execution
    }

    /// Returns the core-selected operation owning the batch deadline.
    pub(crate) const fn operation_id(&self) -> OperationId {
        self.active.operation_id
    }

    /// Returns the unchanged original deadline named by this plan.
    pub(crate) const fn deadline(&self) -> OperationDeadline {
        self.active.deadline
    }
}

impl SubmissionDeadlines {
    /// Returns the oldest admitted batch ready for paired driver handoff.
    pub(crate) fn next_handoff_execution(&self) -> Option<BatchExecutionId> {
        self.active
            .first_key_value()
            .map(|(_, entry)| entry.execution)
    }

    /// Plans removal only when both exact deadline owners are present.
    pub(crate) fn plan_handoff(
        &self,
        execution: BatchExecutionId,
    ) -> Option<SubmissionDeadlineHandoffPlan> {
        let active = self
            .active
            .get(&execution.batch_id())
            .copied()
            .filter(|active| active.execution == execution)?;
        let scheduled = ScheduledDeadline {
            deadline: active.deadline.core(),
            execution,
        };
        if !self.schedule.contains(&scheduled) {
            return None;
        }
        Some(SubmissionDeadlineHandoffPlan { active, scheduled })
    }

    /// Commits one validated removal or returns its plan after exact restoration.
    pub(crate) fn commit_handoff(
        &mut self,
        plan: SubmissionDeadlineHandoffPlan,
    ) -> Result<OperationDeadline, SubmissionDeadlineHandoffPlan> {
        let batch_id = plan.active.execution.batch_id();
        if self.active.get(&batch_id) != Some(&plan.active) {
            return Err(plan);
        }
        let Some(removed) = self.active.remove(&batch_id) else {
            return Err(plan);
        };
        if !self.schedule.remove(&plan.scheduled) {
            self.active.insert(batch_id, removed);
            return Err(plan);
        }
        Ok(removed.deadline)
    }

    #[cfg(test)]
    pub(crate) fn remove_handoff_schedule_for_test(&mut self, execution: BatchExecutionId) {
        let Some(deadline) = self.deadline(execution) else {
            return;
        };
        self.schedule.remove(&ScheduledDeadline {
            deadline: deadline.core(),
            execution,
        });
    }
}
