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
            return self.settle_retry_terminal(batch_id, ProducerFailure::deadline_elapsed());
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
        self.settle_retry_terminal(batch_id, ProducerFailure::materialization_failed())
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
        now: Moment,
        failure: ProducerBrokerFailure,
        delivery: DeliveryStatus,
        route_refreshed: bool,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        if !self.execution_is_current(execution) {
            return Ok(ProducerTransition::none());
        }
        let batch_id = execution.batch_id();
        self.require_batch_state(batch_id, BatchState::Submitted)?;
        let retryable = failure.kind() == crate::ProducerBrokerFailureKind::Retriable
            || (failure.kind() == crate::ProducerBrokerFailureKind::Routing && route_refreshed);
        if retryable && !self.idempotence.is_fenced() && self.retry_available(batch_id)? {
            let deadline = self
                .batches
                .get(&batch_id)
                .and_then(super::ProducerBatch::earliest_deadline)
                .ok_or(ProducerMachineError::UnknownBatch)?;
            if deadline.is_elapsed_at(now) {
                return self.settle_retry_terminal(batch_id, ProducerFailure::deadline_elapsed());
            }
            return self.start_retry(execution, now, deadline, BatchState::Submitted, delivery);
        }
        if failure.kind() == crate::ProducerBrokerFailureKind::Routing {
            return self
                .settle_retry_terminal(batch_id, ProducerFailure::broker(failure, delivery));
        }
        if delivery == DeliveryStatus::PossiblySent {
            return self
                .settle_uncertain_delivery(batch_id, ProducerFailure::broker(failure, delivery));
        }
        self.settle_retry_terminal(batch_id, ProducerFailure::broker(failure, delivery))
    }

    pub(crate) fn route_refresh_deadline_elapsed(
        &mut self,
        execution: BatchExecutionId,
        now: Moment,
        delivery: DeliveryStatus,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        if !self.execution_is_current(execution) {
            return Ok(ProducerTransition::none());
        }
        let batch_id = execution.batch_id();
        self.require_batch_state(batch_id, BatchState::Submitted)?;
        let deadline = self
            .batches
            .get(&batch_id)
            .and_then(super::ProducerBatch::earliest_deadline)
            .ok_or(ProducerMachineError::UnknownBatch)?;
        if !deadline.is_elapsed_at(now) {
            return Err(ProducerMachineError::Transition(
                TransitionError::DeadlineNotElapsed,
            ));
        }
        let failure = ProducerFailure::deadline_elapsed().with_delivery(delivery);
        if delivery == DeliveryStatus::PossiblySent {
            self.settle_uncertain_delivery(batch_id, failure)
        } else {
            self.settle_retry_terminal(batch_id, failure)
        }
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
        match operation.state() {
            ProducerOperationState::WaitingForCapacity { .. } => {
                let effects = self.settle_waiting_operation(
                    operation_id,
                    super::lifecycle::Settlement::Expired,
                    ProducerFailure::waiting_deadline_elapsed(),
                )?;
                Ok(ProducerTransition::from_effects(effects))
            }
            ProducerOperationState::Accumulating { .. } => {
                let batch_id = operation
                    .batch_id()
                    .ok_or(ProducerMachineError::UnknownBatch)?;
                self.expire_open_members(batch_id, &[operation_id], false)
            }
            ProducerOperationState::Materializing { .. }
            | ProducerOperationState::AwaitingDriver { .. }
            | ProducerOperationState::RetryWaiting { .. } => {
                let batch_id = operation
                    .batch_id()
                    .ok_or(ProducerMachineError::UnknownBatch)?;
                self.settle_retry_terminal(batch_id, ProducerFailure::deadline_elapsed())
            }
            ProducerOperationState::Submitted { .. } | ProducerOperationState::Completed => {
                Ok(ProducerTransition::none())
            }
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
