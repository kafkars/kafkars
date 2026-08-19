//! Atomic producer settlement when production execution cannot continue.

use std::collections::BTreeSet;

use crate::{
    DeliveryStatus, EXECUTION_STOP_EFFECTS_PER_RECORD, OperationId, ProducerCompletion,
    ProducerEffect, ProducerFailure, ProducerMachineError, ProducerOperationState,
    ProducerTransition, TransitionError, execution_stop_effect_capacity,
};

use super::{BatchState, ProducerMachine, lifecycle::Settlement};

#[derive(Debug)]
struct ExecutionStopPlan {
    settlements: Vec<(OperationId, Settlement)>,
    effects: Vec<ProducerEffect>,
    flush_effect_count: usize,
    final_effect_count: usize,
}

impl ProducerMachine {
    pub(crate) fn execution_unavailable(
        &mut self,
    ) -> Result<ProducerTransition, ProducerMachineError> {
        let mut plan = self.plan_execution_unavailable()?;
        self.settle_operations_with(&plan.settlements)?;
        let flush_effects = self.settle_ready_flushes();
        debug_assert_eq!(flush_effects.len(), plan.flush_effect_count);
        plan.effects.extend(flush_effects);
        debug_assert_eq!(plan.effects.len(), plan.final_effect_count);
        self.admission_open = false;
        self.open_batches.clear();
        self.batches.clear();
        Ok(ProducerTransition::from_effects(plan.effects))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the plan validates every retained producer owner before any shutdown mutation"
    )]
    fn plan_execution_unavailable(&self) -> Result<ExecutionStopPlan, ProducerMachineError> {
        let active_count = self
            .operations
            .values()
            .filter(|operation| operation.state() != ProducerOperationState::Completed)
            .count();
        if active_count > self.completion_slots() || self.batches.len() > active_count {
            return Err(invalid_state());
        }
        let record_effect_capacity = active_count
            .checked_mul(EXECUTION_STOP_EFFECTS_PER_RECORD)
            .ok_or_else(invalid_state)?;
        let effect_capacity = execution_stop_effect_capacity(active_count, self.flushes.len())
            .ok_or_else(invalid_state)?;
        let flush_effect_count = self.flushes.pending_len();
        let mut settlements = Vec::with_capacity(active_count);
        let mut payload_operations = Vec::with_capacity(active_count);
        let mut seen = BTreeSet::new();
        let mut effects = Vec::with_capacity(effect_capacity);

        for (batch_id, batch) in &self.batches {
            let delivery = delivery_for(batch.state, batch.prior_delivery());
            if matches!(
                batch.state,
                BatchState::Open | BatchState::AwaitingIdentity | BatchState::RetryWaiting
            ) {
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
                let settlement = if batch.state != BatchState::Submitted
                    && batch.prior_delivery() == DeliveryStatus::PossiblySent
                {
                    Settlement::FailedAfterPossibleDelivery
                } else {
                    Settlement::Failed(delivery)
                };
                settlements.push((member.operation_id, settlement));
                payload_operations.push(member.operation_id);
            }
        }
        let mut waiting = self
            .operations
            .iter()
            .filter(|(_id, operation)| {
                matches!(
                    operation.state(),
                    ProducerOperationState::WaitingForCapacity { .. }
                )
            })
            .map(|(operation_id, _operation)| *operation_id)
            .collect::<Vec<_>>();
        waiting.sort_unstable();
        for operation_id in waiting {
            if !seen.insert(operation_id) {
                return Err(invalid_state());
            }
            settlements.push((operation_id, Settlement::Failed(DeliveryStatus::NotSent)));
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
        for operation_id in payload_operations {
            let record = self
                .record(operation_id)
                .ok_or(ProducerMachineError::UnknownOperation)?;
            effects.push(ProducerEffect::ReleasePayload {
                payload_id: record.payload_id(),
                retained_bytes: record.retained_bytes(),
            });
        }
        for (operation_id, settlement) in &settlements {
            let delivery = match settlement {
                Settlement::Failed(delivery) => *delivery,
                Settlement::FailedAfterPossibleDelivery => DeliveryStatus::PossiblySent,
                Settlement::Cancelled | Settlement::Expired | Settlement::Delivered => {
                    return Err(invalid_state());
                }
            };
            effects.push(ProducerEffect::Complete {
                operation_id: *operation_id,
                completion: ProducerCompletion::Failed(ProducerFailure::execution_unavailable(
                    delivery,
                )),
            });
        }
        if effects.len() > record_effect_capacity {
            return Err(invalid_state());
        }
        let final_effect_count = effects
            .len()
            .checked_add(flush_effect_count)
            .ok_or_else(invalid_state)?;
        if final_effect_count > effect_capacity || final_effect_count > effects.capacity() {
            return Err(invalid_state());
        }
        Ok(ExecutionStopPlan {
            settlements,
            effects,
            flush_effect_count,
            final_effect_count,
        })
    }
}

const fn delivery_for(state: BatchState, prior: DeliveryStatus) -> DeliveryStatus {
    match state {
        BatchState::Submitted => DeliveryStatus::PossiblySent,
        BatchState::Open
        | BatchState::AwaitingIdentity
        | BatchState::Materializing
        | BatchState::AwaitingDriver
        | BatchState::RetryWaiting => prior,
    }
}

fn stage_matches(
    state: ProducerOperationState,
    batch_id: crate::BatchId,
    batch_state: BatchState,
) -> bool {
    matches!(
        (state, batch_state),
        (
            ProducerOperationState::Accumulating {
                batch_id: actual, ..
            },
            BatchState::Open,
        ) | (
            ProducerOperationState::Materializing {
                batch_id: actual, ..
            },
            BatchState::AwaitingIdentity | BatchState::Materializing,
        ) | (
            ProducerOperationState::AwaitingDriver {
                batch_id: actual, ..
            },
            BatchState::AwaitingDriver,
        ) | (
            ProducerOperationState::Submitted {
                batch_id: actual, ..
            },
            BatchState::Submitted,
        ) | (
            ProducerOperationState::RetryWaiting {
                batch_id: actual, ..
            },
            BatchState::RetryWaiting,
        ) if actual == batch_id
    )
}

const fn invalid_state() -> ProducerMachineError {
    ProducerMachineError::Transition(TransitionError::InvalidState)
}
