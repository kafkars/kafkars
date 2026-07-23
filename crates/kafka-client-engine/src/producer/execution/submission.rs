//! Submission arming from core effects and original operation-deadline bindings.

use kafka_client_core::{AcknowledgementPolicy, ProducerEffect};

use super::{PreparedExecution, PreparedExecutionError};
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
        if !self.prepared.contains(execution) {
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
        self.deadlines
            .arm(execution, deadline_operation_id, operation_deadline)
            .map(|_newly_armed| ())
            .map_err(PreparedExecutionError::Deadline)
    }

    /// Returns the unchanged operation deadline retained for driver handoff.
    pub(crate) fn submission_deadline(
        &self,
        execution: kafka_client_core::BatchExecutionId,
    ) -> Option<crate::clock::OperationDeadline> {
        self.deadlines.deadline(execution)
    }
}
