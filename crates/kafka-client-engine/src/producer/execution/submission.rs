//! Submission arming and deadline expiry within the prepared execution owner.

use kafka_client_core::{
    AcknowledgementPolicy, BatchExecutionId, Moment, ProducerEffect, ProducerInput,
};

use super::{
    PreparedExecution, PreparedExecutionError, ScheduledDeadline, SubmissionDeadlineError,
    SubmissionFacts,
};
use crate::clock::OperationDeadline;
use crate::producer::{binding::OperationBindings, store::ProducerStore};

impl PreparedExecution {
    /// Retains the original paired deadline while encoded bytes await the driver.
    pub(crate) fn arm_submission(
        &mut self,
        store: &ProducerStore,
        bindings: &OperationBindings,
        effect: ProducerEffect,
    ) -> Result<(), PreparedExecutionError> {
        let ProducerEffect::SubmitProduce {
            execution,
            deadline_operation_id,
            deadline,
            topic_id,
            partition,
            acknowledgements,
        } = effect
        else {
            return Err(PreparedExecutionError::UnexpectedEffect);
        };
        match acknowledgements {
            AcknowledgementPolicy::All => {}
        }
        if !self.contains_execution(execution) {
            return Err(PreparedExecutionError::MissingPreparedBatch(execution));
        }
        let (stored_topic_id, stored_partition) = store
            .execution_route(execution)
            .map_err(PreparedExecutionError::Store)?;
        if stored_topic_id != topic_id || stored_partition != partition {
            return Err(PreparedExecutionError::RouteMismatch {
                execution,
                stored_topic_id,
                stored_partition,
                effect_topic_id: topic_id,
                effect_partition: partition,
            });
        }
        if !store
            .execution_contains_operation(execution, deadline_operation_id)
            .map_err(PreparedExecutionError::Store)?
        {
            return Err(PreparedExecutionError::DeadlineOperationMismatch {
                execution,
                operation_id: deadline_operation_id,
            });
        }
        let operation_deadline = bindings.deadline(deadline_operation_id).ok_or(
            PreparedExecutionError::UnknownDeadlineOperation(deadline_operation_id),
        )?;
        if operation_deadline.core() != deadline {
            return Err(PreparedExecutionError::DeadlineMismatch {
                operation_id: deadline_operation_id,
                effect: deadline,
                bound: operation_deadline.core(),
            });
        }
        self.arm_deadline(execution, deadline_operation_id, operation_deadline)
            .map(|_newly_armed| ())
            .map_err(PreparedExecutionError::Deadline)
    }

    /// Returns the unchanged operation deadline retained for driver handoff.
    pub(crate) fn submission_deadline(
        &self,
        execution: BatchExecutionId,
    ) -> Option<OperationDeadline> {
        self.entries
            .get(&execution.batch_id())
            .filter(|entry| entry.execution == execution)
            .and_then(|entry| entry.submission)
            .map(|submission| submission.deadline)
    }

    /// Converts bounded due mechanism entries into deterministic core facts.
    pub(crate) fn drain_due(&mut self, now: Moment, limit: usize) -> Vec<ProducerInput> {
        let mut due = Vec::with_capacity(limit.min(self.schedule.len()));
        while let Some(next) = self.schedule.first().copied() {
            if due.len() >= limit || !next.deadline.is_elapsed_at(now) {
                break;
            }
            self.schedule.remove(&next);
            let Some(entry) = self.entries.get_mut(&next.execution.batch_id()) else {
                continue;
            };
            if entry.execution != next.execution {
                continue;
            }
            let Some(submission) = entry.submission else {
                continue;
            };
            if submission.deadline.core() != next.deadline {
                continue;
            }
            entry.submission = None;
            due.push(ProducerInput::DeadlineElapsed {
                operation_id: submission.operation_id,
                now,
            });
        }
        due
    }

    fn contains_execution(&self, execution: BatchExecutionId) -> bool {
        self.entries
            .get(&execution.batch_id())
            .is_some_and(|entry| entry.execution == execution)
    }

    fn arm_deadline(
        &mut self,
        execution: BatchExecutionId,
        operation_id: kafka_client_core::OperationId,
        deadline: OperationDeadline,
    ) -> Result<bool, SubmissionDeadlineError> {
        let batch_id = execution.batch_id();
        let candidate = SubmissionFacts {
            operation_id,
            deadline,
        };
        let Some(entry) = self.entries.get_mut(&batch_id) else {
            return Err(SubmissionDeadlineError::ConflictingBatch { batch_id });
        };
        if entry.execution != execution {
            return Err(SubmissionDeadlineError::ConflictingBatch { batch_id });
        }
        if let Some(current) = entry.submission {
            return if current == candidate {
                Ok(false)
            } else {
                Err(SubmissionDeadlineError::ConflictingBatch { batch_id })
            };
        }
        let scheduled = ScheduledDeadline {
            deadline: deadline.core(),
            execution,
        };
        if !self.schedule.insert(scheduled) {
            return Err(SubmissionDeadlineError::ConflictingBatch { batch_id });
        }
        entry.submission = Some(candidate);
        Ok(true)
    }

    #[cfg(test)]
    pub(super) fn arm_for_test(
        &mut self,
        execution: BatchExecutionId,
        operation_id: kafka_client_core::OperationId,
        deadline: OperationDeadline,
    ) -> Result<bool, SubmissionDeadlineError> {
        self.arm_deadline(execution, operation_id, deadline)
    }

    #[cfg(test)]
    pub(super) fn remove_schedule_for_test(&mut self, execution: BatchExecutionId) {
        let Some(deadline) = self.submission_deadline(execution) else {
            return;
        };
        self.schedule.remove(&ScheduledDeadline {
            deadline: deadline.core(),
            execution,
        });
    }
}
