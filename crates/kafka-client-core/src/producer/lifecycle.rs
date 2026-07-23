//! Producer lifecycle coordination across operation, budget, and completion owners.

use crate::{BatchId, DeliveryStatus, OperationId, ProducerMachineError, TerminalRelease};

use super::ProducerMachine;

impl ProducerMachine {
    /// Marks an accumulated operation as ready for driver submission.
    pub(crate) fn mark_ready(
        &mut self,
        id: OperationId,
        batch_id: BatchId,
    ) -> Result<(), ProducerMachineError> {
        let operation = self
            .operations
            .get_mut(&id)
            .ok_or(ProducerMachineError::UnknownOperation)?;
        operation
            .mark_ready(batch_id)
            .map_err(ProducerMachineError::Transition)
    }

    /// Transfers an accumulated operation into driver ownership.
    pub(crate) fn mark_submitted(
        &mut self,
        id: OperationId,
        batch_id: BatchId,
    ) -> Result<(), ProducerMachineError> {
        let operation = self
            .operations
            .get_mut(&id)
            .ok_or(ProducerMachineError::UnknownOperation)?;
        operation
            .mark_submitted(batch_id)
            .map_err(ProducerMachineError::Transition)
    }

    /// Marks broker acknowledgment without retaining the result payload.
    pub(crate) fn settle_delivered(&mut self, id: OperationId) -> Result<(), ProducerMachineError> {
        let operation = self
            .operations
            .get(&id)
            .ok_or(ProducerMachineError::UnknownOperation)?;
        let release = operation
            .plan_delivered()
            .map_err(ProducerMachineError::Transition)?;
        self.retain_terminal(id, release)
    }

    /// Marks failed settlement without retaining the result payload.
    pub(crate) fn settle_failed(
        &mut self,
        id: OperationId,
        delivery: DeliveryStatus,
    ) -> Result<(), ProducerMachineError> {
        let operation = self
            .operations
            .get(&id)
            .ok_or(ProducerMachineError::UnknownOperation)?;
        let release = operation
            .plan_failed(delivery)
            .map_err(ProducerMachineError::Transition)?;
        self.retain_terminal(id, release)
    }

    /// Expires accepted work before it enters driver ownership.
    pub(crate) fn expire_before_submission(
        &mut self,
        id: OperationId,
    ) -> Result<(), ProducerMachineError> {
        let operation = self
            .operations
            .get(&id)
            .ok_or(ProducerMachineError::UnknownOperation)?;
        let release = operation
            .plan_expire()
            .map_err(ProducerMachineError::Transition)?;
        self.retain_terminal(id, release)
    }

    fn retain_terminal(
        &mut self,
        id: OperationId,
        release: TerminalRelease,
    ) -> Result<(), ProducerMachineError> {
        let release_plan = release
            .released_bytes()
            .map(|bytes| self.byte_budget.plan_release(bytes))
            .transpose()
            .map_err(ProducerMachineError::Capacity)?;
        self.completions
            .mark_terminal(id)
            .map_err(ProducerMachineError::Completion)?;
        if let Some(plan) = release_plan {
            self.byte_budget.commit_release(plan);
        }
        self.commit_operation_terminal(id);
        Ok(())
    }

    fn commit_operation_terminal(&mut self, id: OperationId) {
        let operation = self.operations.get_mut(&id);
        debug_assert!(operation.is_some());
        if let Some(operation) = operation {
            operation.commit_terminal();
        }
    }

    /// Releases core lifecycle state after the engine reclaims its terminal result.
    pub(crate) fn reclaim_completion(
        &mut self,
        id: OperationId,
    ) -> Result<(), ProducerMachineError> {
        self.completions
            .reclaim(id)
            .map_err(ProducerMachineError::Completion)?;
        let removed = self.operations.remove(&id);
        debug_assert!(removed.is_some());
        self.records.remove(&id);
        Ok(())
    }
}
