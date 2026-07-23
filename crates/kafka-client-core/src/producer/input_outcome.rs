//! Materialization, driver, broker, and deadline outcome handling.

use crate::{
    BatchExecutionId, BatchId, DeliveryStatus, Moment, OperationId, ProducerBatchSuccess,
    ProducerBrokerFailure, ProducerFailure, ProducerMachineError, ProducerOperationState,
    ProducerTransition, TransitionError,
};

use super::{BatchState, ProducerMachine};

impl ProducerMachine {
    pub(crate) fn batch_materialized(
        &mut self,
        execution: BatchExecutionId,
        now: Moment,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        if !self.execution_is_current(execution) {
            return Ok(ProducerTransition::none());
        }
        let batch_id = execution.batch_id();
        self.require_batch_state(batch_id, BatchState::Materializing)?;
        let batch = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        let deadline = batch
            .earliest_deadline()
            .ok_or(ProducerMachineError::UnknownBatch)?;
        if deadline.is_elapsed_at(now) {
            return self.settle_batch_failed(batch_id, ProducerFailure::deadline_elapsed());
        }
        self.submit_materialized(execution)
    }

    pub(crate) fn materialization_failed(
        &mut self,
        execution: BatchExecutionId,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        if !self.execution_is_current(execution) {
            return Ok(ProducerTransition::none());
        }
        let batch_id = execution.batch_id();
        self.require_batch_state(batch_id, BatchState::Materializing)?;
        self.settle_batch_failed(batch_id, ProducerFailure::materialization_failed())
    }

    pub(crate) fn driver_accepted(
        &mut self,
        execution: BatchExecutionId,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let current = self.current_execution(execution.batch_id());
        if current != Some(execution) {
            return Err(ProducerMachineError::StaleDriverAcceptance {
                reported: execution,
                current,
            });
        }
        let batch_id = execution.batch_id();
        self.require_batch_state(batch_id, BatchState::AwaitingDriver)?;
        let members = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?
            .member_ids();
        self.mark_batch_submitted(&members, batch_id)?;
        let batch = self
            .batches
            .get_mut(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        batch.commit_submitted();
        Ok(ProducerTransition::none())
    }

    pub(crate) fn broker_succeeded(
        &mut self,
        execution: BatchExecutionId,
        success: ProducerBatchSuccess,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        if !self.execution_is_current(execution) {
            return Ok(ProducerTransition::none());
        }
        let batch_id = execution.batch_id();
        self.require_batch_state(batch_id, BatchState::Submitted)?;
        self.settle_batch_succeeded(batch_id, success)
    }

    pub(crate) fn broker_failed(
        &mut self,
        execution: BatchExecutionId,
        failure: ProducerBrokerFailure,
        delivery: DeliveryStatus,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        if !self.execution_is_current(execution) {
            return Ok(ProducerTransition::none());
        }
        let batch_id = execution.batch_id();
        self.require_batch_state(batch_id, BatchState::Submitted)?;
        self.settle_batch_failed(batch_id, ProducerFailure::broker(failure, delivery))
    }

    pub(crate) fn deadline_elapsed(
        &mut self,
        operation_id: OperationId,
        now: Moment,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let Some(operation) = self.operation(operation_id) else {
            return Ok(ProducerTransition::none());
        };
        match operation.state() {
            ProducerOperationState::Submitted { .. } | ProducerOperationState::Completed => {
                return Ok(ProducerTransition::none());
            }
            ProducerOperationState::WaitingForCapacity { .. }
            | ProducerOperationState::Accumulating { .. }
            | ProducerOperationState::Materializing { .. }
            | ProducerOperationState::AwaitingDriver { .. }
            | ProducerOperationState::RetryWaiting { .. } => {}
        }
        let deadline = operation
            .deadline()
            .ok_or(ProducerMachineError::Transition(
                TransitionError::AlreadyCompleted,
            ))?;
        if !deadline.is_elapsed_at(now) {
            return Err(ProducerMachineError::Transition(
                TransitionError::DeadlineNotElapsed,
            ));
        }
        let batch_id = operation
            .batch_id()
            .ok_or(ProducerMachineError::UnknownBatch)?;
        match operation.state() {
            ProducerOperationState::Accumulating { .. } => {
                self.expire_open_members(batch_id, &[operation_id], false)
            }
            ProducerOperationState::Materializing { .. }
            | ProducerOperationState::AwaitingDriver { .. }
            | ProducerOperationState::RetryWaiting { .. } => {
                self.settle_batch_failed(batch_id, ProducerFailure::deadline_elapsed())
            }
            ProducerOperationState::Submitted { .. } | ProducerOperationState::Completed => {
                Ok(ProducerTransition::none())
            }
            ProducerOperationState::WaitingForCapacity { .. } => Err(
                ProducerMachineError::Transition(TransitionError::InvalidState),
            ),
        }
    }

    pub(crate) fn require_batch_state(
        &self,
        batch_id: BatchId,
        expected: BatchState,
    ) -> Result<(), ProducerMachineError> {
        let batch = self
            .batches
            .get(&batch_id)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        if batch.state == expected {
            Ok(())
        } else {
            Err(ProducerMachineError::Transition(
                TransitionError::InvalidState,
            ))
        }
    }

    fn current_execution(&self, batch_id: BatchId) -> Option<BatchExecutionId> {
        self.batches
            .get(&batch_id)
            .and_then(|batch| batch.execution_id(batch_id))
    }

    pub(crate) fn execution_is_current(&self, execution: BatchExecutionId) -> bool {
        self.current_execution(execution.batch_id()) == Some(execution)
    }
}
