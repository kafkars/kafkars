//! Atomic transfer from the prepared owner into bounded handoff submissions.

mod group;
#[cfg(test)]
mod group_test;
mod identity;

use std::{error::Error, fmt};

use kafka_client_core::{BatchExecutionId, OperationId};

use super::{PreparedExecution, PreparedProduceError, ScheduledDeadline};
use crate::{clock::OperationDeadline, protocol::produce::MaterializedProduce};

/// Linear encoded owner ready for route resolution and later driver admission.
#[derive(Debug)]
pub(crate) struct PreparedProduceSubmission {
    execution: BatchExecutionId,
    operation_id: OperationId,
    deadline: OperationDeadline,
    materialized: MaterializedProduce,
}

impl PreparedProduceSubmission {
    pub(super) const fn new(
        execution: BatchExecutionId,
        operation_id: OperationId,
        deadline: OperationDeadline,
        materialized: MaterializedProduce,
    ) -> Self {
        Self {
            execution,
            operation_id,
            deadline,
            materialized,
        }
    }

    /// Returns the exact sealed-batch execution identity.
    pub(crate) const fn execution(&self) -> BatchExecutionId {
        self.execution
    }

    /// Returns the unchanged deadline pair captured at the public boundary.
    pub(crate) const fn deadline(&self) -> OperationDeadline {
        self.deadline
    }

    /// Returns records encoded into this partition batch.
    pub(crate) const fn record_count(&self) -> u32 {
        self.materialized.record_count()
    }

    /// Returns encoded record bytes transferred by this partition batch.
    pub(crate) fn encoded_record_bytes(&self) -> usize {
        self.materialized.retained_record_bytes()
    }

    /// Transfers every driver-handoff owner without rebuilding encoded bytes.
    pub(crate) fn into_parts(self) -> (BatchExecutionId, OperationDeadline, MaterializedProduce) {
        (self.execution, self.deadline, self.materialized)
    }
}

/// Exact rejection from the unified prepared-request transfer.
#[derive(Debug)]
pub(crate) enum PreparedProduceHandoffError {
    /// Bounded control storage could not retain one selected candidate window.
    GroupingCapacity {
        /// Number of exact prepared submissions selected without mutation.
        requested: usize,
    },
    /// One encoded batch exceeds the enclosing broker-request byte limit.
    RequestByteLimit {
        /// Exact execution that remains retained by prepared execution.
        execution: BatchExecutionId,
        /// Encoded record bytes observed without moving their owner.
        encoded_bytes: usize,
        /// Configured maximum encoded record bytes for one request.
        limit: usize,
    },
    /// A borrow-only route snapshot no longer matches its prepared owner.
    RouteSnapshotMismatch {
        /// Exact execution whose observed route facts became stale.
        execution: BatchExecutionId,
    },
    /// The prepared entry is absent, stale, or has not been armed by core.
    OwnershipMismatch {
        /// Execution requested by the driver bridge.
        requested: BatchExecutionId,
        /// Execution retaining the unified entry, if any.
        retained: Option<BatchExecutionId>,
    },
    /// The deadline-order index does not contain the entry's exact facts.
    ScheduleInconsistent {
        /// Exact execution still retaining bytes and deadline facts.
        execution: BatchExecutionId,
        /// Unchanged original deadline still retained in the entry.
        deadline: OperationDeadline,
    },
    /// Encoded-byte accounting cannot release the still-retained entry.
    AccountingInconsistent {
        /// Exact execution still retaining bytes and deadline facts.
        execution: BatchExecutionId,
        /// Unchanged original deadline still retained in the entry.
        deadline: OperationDeadline,
        /// Accounting rejection observed before mutation.
        reason: PreparedProduceError,
    },
}

impl fmt::Display for PreparedProduceHandoffError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GroupingCapacity { requested } => write!(
                formatter,
                "prepared Produce candidate window could not reserve {requested} control entries"
            ),
            Self::RequestByteLimit {
                execution,
                encoded_bytes,
                limit,
            } => write!(
                formatter,
                "prepared Produce batch {} generation {} retains {encoded_bytes} encoded bytes above request limit {limit}",
                execution.batch_id().get(),
                execution.generation().get()
            ),
            Self::RouteSnapshotMismatch { execution } => write!(
                formatter,
                "prepared Produce route snapshot is stale for batch {} generation {}",
                execution.batch_id().get(),
                execution.generation().get()
            ),
            Self::OwnershipMismatch { requested, .. } => write!(
                formatter,
                "prepared Produce handoff ownership disagrees for batch {} generation {}",
                requested.batch_id().get(),
                requested.generation().get()
            ),
            Self::ScheduleInconsistent { execution, .. } => write!(
                formatter,
                "prepared Produce schedule disagrees for batch {} generation {}",
                execution.batch_id().get(),
                execution.generation().get()
            ),
            Self::AccountingInconsistent {
                execution, reason, ..
            } => write!(
                formatter,
                "prepared Produce accounting disagrees for batch {} generation {}: {reason}",
                execution.batch_id().get(),
                execution.generation().get()
            ),
        }
    }
}

impl Error for PreparedProduceHandoffError {}

impl PreparedExecution {
    pub(super) fn preflight_driver_submission(
        &self,
        execution: BatchExecutionId,
    ) -> Result<(super::SubmissionFacts, ScheduledDeadline, usize), PreparedProduceHandoffError>
    {
        let batch_id = execution.batch_id();
        let retained = self.entries.get(&batch_id).map(|entry| entry.execution);
        let Some(entry) = self
            .entries
            .get(&batch_id)
            .filter(|entry| entry.execution == execution)
        else {
            return Err(PreparedProduceHandoffError::OwnershipMismatch {
                requested: execution,
                retained,
            });
        };
        let Some(submission) = entry.submission else {
            return Err(PreparedProduceHandoffError::OwnershipMismatch {
                requested: execution,
                retained,
            });
        };
        let scheduled = ScheduledDeadline {
            deadline: submission.deadline.core(),
            execution,
        };
        if !self.schedule.contains(&scheduled) {
            return Err(PreparedProduceHandoffError::ScheduleInconsistent {
                execution,
                deadline: submission.deadline,
            });
        }
        let next_bytes = self
            .retained_bytes
            .checked_sub(entry.materialized.retained_record_bytes())
            .ok_or(PreparedProduceHandoffError::AccountingInconsistent {
                execution,
                deadline: submission.deadline,
                reason: PreparedProduceError::EncodedByteOverflow,
            })?;
        Ok((submission, scheduled, next_bytes))
    }

    /// Transfers one exact armed entry after checking every retained fact.
    pub(crate) fn take_driver_submission(
        &mut self,
        execution: BatchExecutionId,
    ) -> Result<PreparedProduceSubmission, PreparedProduceHandoffError> {
        let batch_id = execution.batch_id();
        let (submission, scheduled, next_bytes) = self.preflight_driver_submission(execution)?;

        if !self.schedule.remove(&scheduled) {
            return Err(PreparedProduceHandoffError::ScheduleInconsistent {
                execution,
                deadline: submission.deadline,
            });
        }
        let Some(entry) = self.entries.remove(&batch_id) else {
            self.schedule.insert(scheduled);
            return Err(PreparedProduceHandoffError::OwnershipMismatch {
                requested: execution,
                retained: None,
            });
        };
        self.retained_bytes = next_bytes;
        Ok(PreparedProduceSubmission::new(
            execution,
            submission.operation_id,
            submission.deadline,
            entry.materialized,
        ))
    }

    #[cfg(test)]
    pub(super) fn replace_retained_bytes_for_test(&mut self, replacement: usize) -> usize {
        std::mem::replace(&mut self.retained_bytes, replacement)
    }
}
