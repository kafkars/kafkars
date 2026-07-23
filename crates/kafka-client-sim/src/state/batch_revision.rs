//! Exact-generation replacement of virtual sealed-batch execution state.

use kafka_client_core::{BatchExecutionGeneration, BatchExecutionId, OperationId};

use super::{
    VirtualProducerState,
    batch::{VirtualBatch, VirtualBatchPhase},
};
use crate::SimulationError;

impl VirtualBatch {
    pub(super) fn revise(
        &mut self,
        previous: BatchExecutionId,
        replacement: Option<BatchExecutionId>,
        removed_operation_id: OperationId,
    ) -> Result<bool, SimulationError> {
        let current = match self.phase {
            VirtualBatchPhase::Ready(current)
            | VirtualBatchPhase::Materializing(current)
            | VirtualBatchPhase::Materialized(current)
            | VirtualBatchPhase::AwaitingDriver(current) => current,
            VirtualBatchPhase::Submitted(current) => {
                return Err(SimulationError::BatchExecutionAlreadySubmitted(current));
            }
            VirtualBatchPhase::Open => {
                return Err(SimulationError::BatchExecutionMismatch {
                    expected: None,
                    actual: previous,
                });
            }
        };
        if current != previous {
            return Err(SimulationError::BatchExecutionMismatch {
                expected: Some(current),
                actual: previous,
            });
        }
        let position = self
            .members
            .iter()
            .position(|member| *member == removed_operation_id)
            .ok_or(SimulationError::OperationNotInBatch(removed_operation_id))?;
        let surviving = self.members.len().saturating_sub(1);
        match replacement {
            Some(replacement) => {
                let next = previous
                    .generation()
                    .get()
                    .checked_add(1)
                    .and_then(BatchExecutionGeneration::try_from_raw)
                    .map(|generation| BatchExecutionId::new(previous.batch_id(), generation));
                if surviving == 0 || next != Some(replacement) {
                    return Err(SimulationError::BatchExecutionMismatch {
                        expected: next,
                        actual: replacement,
                    });
                }
            }
            None if surviving != 0 => {
                return Err(SimulationError::MissingReplacementExecution(previous));
            }
            None => {}
        }
        self.members.remove(position);
        if let Some(replacement) = replacement {
            self.phase = VirtualBatchPhase::Ready(replacement);
            Ok(false)
        } else {
            Ok(true)
        }
    }

    fn driver_accepted(&mut self, execution: BatchExecutionId) -> Result<(), SimulationError> {
        match self.phase {
            VirtualBatchPhase::AwaitingDriver(current) if current == execution => {
                self.phase = VirtualBatchPhase::Submitted(execution);
                Ok(())
            }
            VirtualBatchPhase::AwaitingDriver(current)
            | VirtualBatchPhase::Ready(current)
            | VirtualBatchPhase::Materializing(current)
            | VirtualBatchPhase::Materialized(current)
            | VirtualBatchPhase::Submitted(current) => {
                Err(SimulationError::BatchExecutionMismatch {
                    expected: Some(current),
                    actual: execution,
                })
            }
            VirtualBatchPhase::Open => Err(SimulationError::BatchExecutionMismatch {
                expected: None,
                actual: execution,
            }),
        }
    }
}

impl VirtualProducerState {
    pub(super) fn revise_batch_execution(
        &mut self,
        previous: BatchExecutionId,
        replacement: Option<BatchExecutionId>,
        removed_operation_id: OperationId,
    ) -> Result<(), SimulationError> {
        let empties_batch = self
            .batches
            .get_mut(&previous.batch_id())
            .ok_or(SimulationError::UnknownBatch(previous.batch_id()))?
            .revise(previous, replacement, removed_operation_id)?;
        self.submissions.remove(&previous);
        if empties_batch {
            self.batches.remove(&previous.batch_id());
            self.timers.remove(&previous.batch_id());
        }
        Ok(())
    }

    pub(crate) fn driver_accepted(
        &mut self,
        execution: BatchExecutionId,
    ) -> Result<(), SimulationError> {
        self.batches
            .get_mut(&execution.batch_id())
            .ok_or(SimulationError::UnknownBatch(execution.batch_id()))?
            .driver_accepted(execution)
    }
}
