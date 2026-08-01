//! Atomic settlement across operation, byte-budget, and completion owners.

use crate::{
    ByteCount, DeliveryStatus, OperationId, ProducerMachineError, ProducerTransition,
    TerminalRelease,
};

use super::ProducerMachine;

#[derive(Debug, Clone, Copy)]
pub(crate) enum Settlement {
    Cancelled,
    Expired,
    Failed(DeliveryStatus),
    FailedAfterPossibleDelivery,
    Delivered,
}

impl ProducerMachine {
    pub(crate) fn close_requested(&mut self) -> Result<ProducerTransition, ProducerMachineError> {
        let effects = self
            .flushes
            .request(self.next_operation_id, &self.operations)
            .map_err(ProducerMachineError::Flush)?;
        self.close_admission();
        Ok(ProducerTransition::from_effects(effects))
    }

    pub(crate) fn require_batch_accumulating(
        &self,
        ids: &[OperationId],
        batch_id: crate::BatchId,
    ) -> Result<(), ProducerMachineError> {
        for id in ids {
            let operation = self
                .operations
                .get(id)
                .ok_or(ProducerMachineError::UnknownOperation)?;
            operation
                .require_accumulating(batch_id)
                .map_err(ProducerMachineError::Transition)?;
        }
        Ok(())
    }

    pub(crate) fn commit_batch_ready(&mut self, ids: &[OperationId], batch_id: crate::BatchId) {
        for id in ids {
            let operation = self.operations.get_mut(id);
            debug_assert!(operation.is_some());
            if let Some(operation) = operation {
                operation.commit_ready(batch_id);
            }
        }
    }

    pub(crate) fn mark_batch_submitted(
        &mut self,
        ids: &[OperationId],
        batch_id: crate::BatchId,
    ) -> Result<(), ProducerMachineError> {
        for id in ids {
            let operation = self
                .operations
                .get(id)
                .ok_or(ProducerMachineError::UnknownOperation)?;
            operation
                .require_awaiting_driver(batch_id)
                .map_err(ProducerMachineError::Transition)?;
        }
        for id in ids {
            let operation = self.operations.get_mut(id);
            debug_assert!(operation.is_some());
            if let Some(operation) = operation {
                operation.commit_submitted(batch_id);
            }
        }
        Ok(())
    }

    pub(crate) fn mark_batch_materialized(
        &mut self,
        ids: &[OperationId],
        batch_id: crate::BatchId,
    ) -> Result<(), ProducerMachineError> {
        for id in ids {
            let operation = self
                .operations
                .get(id)
                .ok_or(ProducerMachineError::UnknownOperation)?;
            operation
                .require_materializing(batch_id)
                .map_err(ProducerMachineError::Transition)?;
        }
        for id in ids {
            let operation = self.operations.get_mut(id);
            debug_assert!(operation.is_some());
            if let Some(operation) = operation {
                operation.commit_materialized(batch_id);
            }
        }
        Ok(())
    }

    pub(crate) fn require_batch_execution_restart(
        &self,
        ids: &[OperationId],
        batch_id: crate::BatchId,
    ) -> Result<(), ProducerMachineError> {
        for id in ids {
            self.operations
                .get(id)
                .ok_or(ProducerMachineError::UnknownOperation)?
                .require_execution_restart(batch_id)
                .map_err(ProducerMachineError::Transition)?;
        }
        Ok(())
    }

    pub(crate) fn commit_batch_execution_restart(
        &mut self,
        ids: &[OperationId],
        batch_id: crate::BatchId,
    ) {
        for id in ids {
            let operation = self.operations.get_mut(id);
            debug_assert!(operation.is_some());
            if let Some(operation) = operation {
                operation.commit_execution_restart(batch_id);
            }
        }
    }

    pub(crate) fn settle_operations(
        &mut self,
        ids: &[OperationId],
        settlement: Settlement,
    ) -> Result<(), ProducerMachineError> {
        let settlements = ids.iter().map(|id| (*id, settlement)).collect::<Vec<_>>();
        self.settle_operations_with(&settlements)
    }

    pub(crate) fn settle_operations_with(
        &mut self,
        settlements: &[(OperationId, Settlement)],
    ) -> Result<(), ProducerMachineError> {
        let releases = settlements
            .iter()
            .map(|(id, settlement)| self.plan_settlement(*id, *settlement))
            .collect::<Result<Vec<_>, _>>()?;
        let ids = settlements
            .iter()
            .map(|(id, _settlement)| *id)
            .collect::<Vec<_>>();
        self.completions
            .require_pending(&ids)
            .map_err(ProducerMachineError::Completion)?;
        let total = releases
            .iter()
            .filter_map(|release| release.released_bytes())
            .try_fold(ByteCount::new(0), ByteCount::checked_add)
            .ok_or(ProducerMachineError::AccumulatorSizeOverflow)?;
        let release_plan = self
            .byte_budget
            .plan_release(total)
            .map_err(ProducerMachineError::Capacity)?;

        self.completions.commit_terminal_many(&ids);
        self.byte_budget.commit_release(release_plan);
        for id in &ids {
            let operation = self.operations.get_mut(id);
            debug_assert!(operation.is_some());
            if let Some(operation) = operation {
                operation.commit_terminal();
            }
        }
        Ok(())
    }

    fn plan_settlement(
        &self,
        id: OperationId,
        settlement: Settlement,
    ) -> Result<TerminalRelease, ProducerMachineError> {
        let operation = self
            .operations
            .get(&id)
            .ok_or(ProducerMachineError::UnknownOperation)?;
        match settlement {
            Settlement::Cancelled => operation.plan_cancel(),
            Settlement::Expired => operation.plan_expire(),
            Settlement::Failed(delivery) => operation.plan_failed(delivery),
            Settlement::FailedAfterPossibleDelivery => operation.plan_finish(),
            Settlement::Delivered => operation.plan_delivered(),
        }
        .map_err(ProducerMachineError::Transition)
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
