//! Atomic producer settlement when production execution cannot continue.

use std::collections::BTreeSet;

use crate::{
    DeliveryStatus, OperationId, ProducerCompletion, ProducerEffect, ProducerFailure,
    ProducerMachineError, ProducerOperationState, ProducerTransition, TransitionError,
};

use super::{BatchState, ProducerMachine, lifecycle::Settlement};

#[derive(Debug)]
struct ExecutionStopPlan {
    settlements: Vec<(OperationId, Settlement)>,
    effects: Vec<ProducerEffect>,
}

impl ProducerMachine {
    pub(crate) fn execution_unavailable(
        &mut self,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let mut plan = self.plan_execution_unavailable()?;
        self.settle_operations_with(&plan.settlements)?;
        let flush_effects = self.settle_ready_flushes();
        plan.effects.extend(flush_effects);
        self.admission_open = false;
        self.open_batches.clear();
        self.batches.clear();
        Ok(ProducerTransition::from_effects(plan.effects))
    }

    fn plan_execution_unavailable(&self) -> Result<ExecutionStopPlan, ProducerMachineError> {
        let active_count = self
            .operations
            .values()
            .filter(|operation| operation.state() != ProducerOperationState::Completed)
            .count();
        if active_count > self.completion_slots() || self.batches.len() > active_count {
            return Err(invalid_state());
        }
        let max_effects = active_count.checked_mul(4).ok_or_else(invalid_state)?;
        let mut settlements = Vec::with_capacity(active_count);
        let mut seen = BTreeSet::new();
        let mut effects = Vec::with_capacity(active_count.saturating_mul(2));

        for (batch_id, batch) in &self.batches {
            let delivery = delivery_for(batch.state);
            if batch.state == BatchState::Open {
                effects.push(ProducerEffect::CancelBatchTimer {
                    batch_id: *batch_id,
                    generation: batch.timer_generation,
                });
            }
            for member in &batch.members {
                if !seen.insert(member.operation_id) {
                    return Err(invalid_state());
                }
                let operation = self
                    .operations
                    .get(&member.operation_id)
                    .ok_or(ProducerMachineError::UnknownOperation)?;
                if !stage_matches(operation.state(), *batch_id, batch.state) {
                    return Err(invalid_state());
                }
                settlements.push((member.operation_id, Settlement::Failed(delivery)));
            }
        }
        if settlements.len() != active_count {
            return Err(invalid_state());
        }

        effects.extend(
            self.batches
                .keys()
                .copied()
                .map(|batch_id| ProducerEffect::ReleaseBatch { batch_id }),
        );
        for (operation_id, _settlement) in &settlements {
            let record = self
                .record(*operation_id)
                .ok_or(ProducerMachineError::UnknownOperation)?;
            effects.push(ProducerEffect::ReleasePayload {
                payload_id: record.payload_id(),
                retained_bytes: record.retained_bytes(),
            });
        }
        for (operation_id, settlement) in &settlements {
            let Settlement::Failed(delivery) = settlement else {
                return Err(invalid_state());
            };
            effects.push(ProducerEffect::Complete {
                operation_id: *operation_id,
                completion: ProducerCompletion::Failed(ProducerFailure::execution_unavailable(
                    *delivery,
                )),
            });
        }
        if effects.len() > max_effects {
            return Err(invalid_state());
        }
        Ok(ExecutionStopPlan {
            settlements,
            effects,
        })
    }
}

const fn delivery_for(state: BatchState) -> DeliveryStatus {
    match state {
        BatchState::Open | BatchState::Materializing | BatchState::AwaitingDriver => {
            DeliveryStatus::NotSent
        }
        BatchState::Submitted => DeliveryStatus::PossiblySent,
    }
}

fn stage_matches(
    state: ProducerOperationState,
    batch_id: crate::BatchId,
    batch_state: BatchState,
) -> bool {
    matches!(
        (state, batch_state),
        (ProducerOperationState::Accumulating { batch_id: actual, .. }, BatchState::Open)
            | (
                ProducerOperationState::Materializing { batch_id: actual, .. },
                BatchState::Materializing
            )
            | (
                ProducerOperationState::AwaitingDriver { batch_id: actual, .. },
                BatchState::AwaitingDriver
            )
            | (
                ProducerOperationState::Submitted { batch_id: actual, .. },
                BatchState::Submitted
            ) if actual == batch_id
    )
}

const fn invalid_state() -> ProducerMachineError {
    ProducerMachineError::Transition(TransitionError::InvalidState)
}
