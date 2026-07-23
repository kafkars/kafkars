//! Materialization, driver, broker, and deadline outcome handling.

use crate::{
    BatchId, DeliveryStatus, Moment, OperationId, ProducerBatchSuccess, ProducerBrokerFailure,
    ProducerFailure, ProducerMachineError, ProducerOperationState, ProducerTransition,
    TransitionError,
};

use super::{BatchState, ProducerMachine};

impl ProducerMachine {
    pub(crate) fn batch_materialized(
        &mut self,
        batch_id: BatchId,
        now: Moment,
    ) -> Result<ProducerTransition, ProducerMachineError> {
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
        self.submit_materialized(batch_id)
    }

    pub(crate) fn materialization_failed(
        &mut self,
        batch_id: BatchId,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        self.require_batch_state(batch_id, BatchState::Materializing)?;
        self.settle_batch_failed(batch_id, ProducerFailure::materialization_failed())
    }

    pub(crate) fn driver_accepted(
        &mut self,
        batch_id: BatchId,
    ) -> Result<ProducerTransition, ProducerMachineError> {
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

    pub(crate) fn driver_rejected(
        &mut self,
        batch_id: BatchId,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        self.require_batch_state(batch_id, BatchState::AwaitingDriver)?;
        self.settle_batch_failed(batch_id, ProducerFailure::driver_rejected())
    }

    pub(crate) fn broker_succeeded(
        &mut self,
        batch_id: BatchId,
        success: ProducerBatchSuccess,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        self.require_batch_state(batch_id, BatchState::Submitted)?;
        self.settle_batch_succeeded(batch_id, success)
    }

    pub(crate) fn broker_failed(
        &mut self,
        batch_id: BatchId,
        failure: ProducerBrokerFailure,
        delivery: DeliveryStatus,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        self.require_batch_state(batch_id, BatchState::Submitted)?;
        self.settle_batch_failed(batch_id, ProducerFailure::broker(failure, delivery))
    }

    pub(crate) fn transport_failed(
        &mut self,
        batch_id: BatchId,
        delivery: DeliveryStatus,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        self.require_batch_state(batch_id, BatchState::Submitted)?;
        self.settle_batch_failed(batch_id, ProducerFailure::transport(delivery))
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
            | ProducerOperationState::AwaitingDriver { .. } => {}
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
            | ProducerOperationState::AwaitingDriver { .. } => {
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

    fn require_batch_state(
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
}
