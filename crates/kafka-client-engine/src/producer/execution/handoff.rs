//! Atomic transfer of one prepared request and its original absolute deadline.

use std::{error::Error, fmt};

use kafka_client_core::BatchExecutionId;

use super::PreparedExecution;
use crate::{
    clock::OperationDeadline,
    producer::{
        prepared::PreparedProduceError, submission_deadline::handoff::SubmissionDeadlineHandoffPlan,
    },
    protocol::produce::MaterializedProduce,
};

/// Linear request owner ready for a future bounded driver admission attempt.
#[derive(Debug)]
pub(crate) struct PreparedProduceSubmission {
    execution: BatchExecutionId,
    deadline: OperationDeadline,
    materialized: MaterializedProduce,
}

impl PreparedProduceSubmission {
    /// Returns the exact sealed-batch execution identity.
    pub(crate) const fn execution(&self) -> BatchExecutionId {
        self.execution
    }

    /// Returns the unchanged deadline pair captured at the public boundary.
    pub(crate) const fn deadline(&self) -> OperationDeadline {
        self.deadline
    }

    /// Transfers every driver-handoff owner without rebuilding encoded bytes.
    pub(crate) fn into_parts(self) -> (BatchExecutionId, OperationDeadline, MaterializedProduce) {
        (self.execution, self.deadline, self.materialized)
    }
}

/// Linear proof that both exact pre-driver owners passed handoff preflight.
#[derive(Debug)]
pub(super) struct PreparedProduceHandoffPlan {
    execution: BatchExecutionId,
    deadline: SubmissionDeadlineHandoffPlan,
}

/// Exact rejection from the paired prepared-request and deadline transfer.
#[derive(Debug)]
pub(crate) enum PreparedProduceHandoffError {
    /// The two pre-driver owners do not both retain the requested execution.
    OwnershipMismatch {
        /// Execution requested by the future driver owner.
        requested: BatchExecutionId,
        /// Execution retaining encoded bytes, if any.
        prepared: Option<BatchExecutionId>,
        /// Execution retaining the original deadline, if any.
        deadline: Option<BatchExecutionId>,
    },
    /// Deadline planning or commit drift reports current stores and any returned plan.
    DeadlineInconsistent {
        /// Execution requested by the handoff.
        requested: BatchExecutionId,
        /// Execution currently retaining encoded bytes, if any.
        prepared: Option<BatchExecutionId>,
        /// Execution currently retained by the active deadline store, if any.
        active: Option<BatchExecutionId>,
        /// Unchanged original deadline from before the disagreement.
        deadline: OperationDeadline,
        /// Exact uncommitted removal plan returned after commit-time drift.
        plan: Option<Box<SubmissionDeadlineHandoffPlan>>,
    },
    /// Prepared-store preflight drift leaves both original owners retained.
    PreparedPreflightInconsistent {
        /// Requested execution whose encoded bytes and deadline remain retained.
        execution: BatchExecutionId,
        /// Original deadline still retained by the pre-driver deadline store.
        deadline: OperationDeadline,
        /// Exact prepared-store rejection that preserved the encoded request.
        reason: PreparedProduceError,
    },
    /// Prepared ownership changed after planning; its current location is reported.
    PreparedCommitInconsistent {
        /// Execution requested by the consumed handoff plan.
        requested: BatchExecutionId,
        /// Original deadline detached during the planned commit.
        deadline: OperationDeadline,
        /// Execution currently retaining encoded bytes, if still in the store.
        prepared: Option<BatchExecutionId>,
        /// Exact prepared-store rejection observed after deadline transfer.
        reason: PreparedProduceError,
    },
}

impl fmt::Display for PreparedProduceHandoffError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OwnershipMismatch { requested, .. } => write!(
                formatter,
                "prepared Produce handoff ownership disagrees for batch {} generation {}",
                requested.batch_id().get(),
                requested.generation().get()
            ),
            Self::DeadlineInconsistent { requested, .. } => write!(
                formatter,
                "prepared Produce deadline handoff disagrees for batch {} generation {}",
                requested.batch_id().get(),
                requested.generation().get()
            ),
            Self::PreparedPreflightInconsistent {
                execution, reason, ..
            } => write!(
                formatter,
                "prepared Produce handoff preflight failed for batch {} generation {}: {reason}",
                execution.batch_id().get(),
                execution.generation().get()
            ),
            Self::PreparedCommitInconsistent {
                requested, reason, ..
            } => write!(
                formatter,
                "prepared Produce handoff commit failed for batch {} generation {}: {reason}",
                requested.batch_id().get(),
                requested.generation().get()
            ),
        }
    }
}

impl Error for PreparedProduceHandoffError {}

impl PreparedExecution {
    /// Atomically transfers one exact prepared execution out of both stores.
    pub(crate) fn take_driver_submission(
        &mut self,
        execution: BatchExecutionId,
    ) -> Result<PreparedProduceSubmission, PreparedProduceHandoffError> {
        let plan = self.plan_driver_submission(execution)?;
        self.commit_driver_submission(plan)
    }

    /// Preflights both exact owners without mutating either store.
    pub(super) fn plan_driver_submission(
        &self,
        execution: BatchExecutionId,
    ) -> Result<PreparedProduceHandoffPlan, PreparedProduceHandoffError> {
        let batch_id = execution.batch_id();
        let prepared = self.prepared.execution(batch_id);
        let deadline = self.deadlines.execution(batch_id);
        if prepared != Some(execution) || deadline != Some(execution) {
            return Err(PreparedProduceHandoffError::OwnershipMismatch {
                requested: execution,
                prepared,
                deadline,
            });
        }
        let original_deadline = self.deadlines.deadline(execution).ok_or(
            PreparedProduceHandoffError::OwnershipMismatch {
                requested: execution,
                prepared,
                deadline,
            },
        )?;
        match self.prepared.preflight_release(execution) {
            Ok(true) => {}
            Ok(false) => {
                return Err(PreparedProduceHandoffError::OwnershipMismatch {
                    requested: execution,
                    prepared: self.prepared.execution(batch_id),
                    deadline: self.deadlines.execution(batch_id),
                });
            }
            Err(reason) => {
                return Err(PreparedProduceHandoffError::PreparedPreflightInconsistent {
                    execution,
                    deadline: original_deadline,
                    reason,
                });
            }
        }
        let deadline = self.deadlines.plan_handoff(execution).ok_or(
            PreparedProduceHandoffError::DeadlineInconsistent {
                requested: execution,
                prepared: self.prepared.execution(batch_id),
                active: self.deadlines.execution(batch_id),
                deadline: original_deadline,
                plan: None,
            },
        )?;
        Ok(PreparedProduceHandoffPlan {
            execution,
            deadline,
        })
    }

    /// Consumes one validated plan or reports every surviving owner location.
    pub(super) fn commit_driver_submission(
        &mut self,
        plan: PreparedProduceHandoffPlan,
    ) -> Result<PreparedProduceSubmission, PreparedProduceHandoffError> {
        let execution = plan.execution;
        let deadline = match self.deadlines.commit_handoff(plan.deadline) {
            Ok(deadline) => deadline,
            Err(returned) => {
                return Err(PreparedProduceHandoffError::DeadlineInconsistent {
                    requested: execution,
                    prepared: self.prepared.execution(execution.batch_id()),
                    active: self.deadlines.execution(execution.batch_id()),
                    deadline: returned.deadline(),
                    plan: Some(Box::new(returned)),
                });
            }
        };
        let materialized = self.prepared.take(execution).map_err(|reason| {
            PreparedProduceHandoffError::PreparedCommitInconsistent {
                requested: execution,
                deadline,
                prepared: self.prepared.execution(execution.batch_id()),
                reason,
            }
        })?;

        Ok(PreparedProduceSubmission {
            execution,
            deadline,
            materialized,
        })
    }
}
