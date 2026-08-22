//! Explicit deterministic steps joining producer policy to virtual engine ownership.

#[path = "producer_time.rs"]
mod time;

use kafka_client_core::{
    BatchId, ByteCount, FlushId, Moment, OperationId, PayloadId, ProducerBatchPolicy,
    ProducerCompletion, ProducerEffect, ProducerInput, ProducerMachine, ProducerRetryPolicy,
    ProducerTransition,
};

use crate::{SimulationError, VirtualClock, state::VirtualProducerState};

/// Deterministic producer policy plus a minimal effect interpreter.
#[derive(Debug)]
pub struct ProducerScenario {
    clock: VirtualClock,
    core: ProducerMachine,
    engine: VirtualProducerState,
    auto_identity: bool,
}

impl ProducerScenario {
    /// Creates an empty scenario with hard byte and completion limits.
    pub fn new(retained_bytes: ByteCount, completion_capacity: usize) -> Self {
        Self {
            clock: VirtualClock::default(),
            core: ProducerMachine::new(retained_bytes, completion_capacity),
            engine: VirtualProducerState::new(completion_capacity),
            auto_identity: true,
        }
    }

    /// Creates a scenario with explicit deterministic batch thresholds.
    pub fn with_batch_policy(
        retained_bytes: ByteCount,
        completion_capacity: usize,
        batch_policy: ProducerBatchPolicy,
    ) -> Self {
        Self {
            clock: VirtualClock::default(),
            core: ProducerMachine::with_batch_policy(
                retained_bytes,
                completion_capacity,
                batch_policy,
            ),
            engine: VirtualProducerState::new(completion_capacity),
            auto_identity: true,
        }
    }

    /// Creates a scenario with explicit batching and producer retry policy.
    pub fn with_batch_and_retry_policy(
        retained_bytes: ByteCount,
        completion_capacity: usize,
        batch_policy: ProducerBatchPolicy,
        retry_policy: ProducerRetryPolicy,
    ) -> Self {
        Self {
            clock: VirtualClock::default(),
            core: ProducerMachine::with_batch_and_retry_policy(
                retained_bytes,
                completion_capacity,
                batch_policy,
                retry_policy,
            ),
            engine: VirtualProducerState::new(completion_capacity),
            auto_identity: true,
        }
    }

    /// Returns the current virtual monotonic observation.
    pub const fn now(&self) -> Moment {
        self.clock.now()
    }

    /// Gives the virtual engine ownership of bytes before core admission.
    pub fn retain_payload(
        &mut self,
        payload_id: PayloadId,
        bytes: ByteCount,
    ) -> Result<(), SimulationError> {
        self.engine.retain_payload(payload_id, bytes)
    }

    /// Applies one external fact and interprets every ordered core effect.
    pub fn step(&mut self, input: ProducerInput) -> Result<ProducerTransition, SimulationError> {
        let reserves_flush = matches!(
            input,
            ProducerInput::FlushRequested | ProducerInput::CloseRequested
        );
        if reserves_flush {
            self.engine.reserve_flush_completion()?;
        }
        let reclaimed = match input {
            ProducerInput::CompletionReclaimed { operation_id } => {
                self.engine.require_released_terminal(operation_id)?;
                Some(operation_id)
            }
            _ => None,
        };
        let reclaimed_flush = match input {
            ProducerInput::FlushCompletionReclaimed { flush_id } => {
                self.engine.require_released_flush(flush_id)?;
                Some(flush_id)
            }
            _ => None,
        };
        let transition = match self.core.apply(input) {
            Ok(transition) => transition,
            Err(error) => {
                if reserves_flush {
                    self.engine.rollback_flush_reservation();
                }
                return Err(SimulationError::Core(error));
            }
        };
        if let ProducerInput::DriverAccepted { execution } = input {
            self.engine.driver_accepted(execution)?;
        }
        self.interpret_effects(transition.effects())?;
        if let Some(operation_id) = reclaimed {
            self.engine.finish_reclaim(operation_id);
        }
        if let Some(flush_id) = reclaimed_flush {
            self.engine.finish_flush_reclaim(flush_id);
        }
        Ok(transition)
    }

    fn interpret_effects(&mut self, effects: &[ProducerEffect]) -> Result<(), SimulationError> {
        let mut pending = effects.to_vec();
        let mut index = 0;
        while let Some(effect) = pending.get(index).copied() {
            index += 1;
            self.engine.interpret(effect)?;
            let ProducerEffect::AcquireProducerIdentity { generation, .. } = effect else {
                continue;
            };
            if !self.auto_identity {
                continue;
            }
            let acquired = self
                .core
                .apply(ProducerInput::ProducerIdentityAcquired {
                    generation,
                    producer_id: 1,
                    producer_epoch: 0,
                    now: self.clock.now(),
                })
                .map_err(SimulationError::Core)?;
            pending.extend_from_slice(acquired.effects());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) const fn disable_automatic_identity_for_test(&mut self) {
        self.auto_identity = false;
    }

    /// Releases the engine-owned terminal result without reclaiming core capacity.
    pub fn release_terminal_result(
        &mut self,
        operation_id: OperationId,
    ) -> Result<ProducerCompletion, SimulationError> {
        self.engine.release_terminal(operation_id)
    }

    /// Releases one completed virtual flush result before core reclamation.
    pub fn release_flush_result(&mut self, flush_id: FlushId) -> Result<(), SimulationError> {
        self.engine.release_flush(flush_id)
    }

    /// Returns whether the virtual engine still retains this payload.
    pub fn contains_payload(&self, payload_id: PayloadId) -> bool {
        self.engine.contains_payload(payload_id)
    }

    /// Returns whether the virtual engine still retains this materialized batch.
    pub fn contains_batch(&self, batch_id: BatchId) -> bool {
        self.engine.contains_batch(batch_id)
    }

    /// Returns the engine-owned terminal result retained for observation.
    pub fn terminal_result(&self, operation_id: OperationId) -> Option<ProducerCompletion> {
        self.engine.terminal(operation_id)
    }

    /// Returns whether one completed flush result remains retained for observation.
    pub fn flush_result_is_retained(&self, flush_id: FlushId) -> bool {
        self.engine.flush_terminal_is_retained(flush_id)
    }

    /// Returns driver submissions requested by core policy.
    pub fn submission_count(&self) -> usize {
        self.engine.submission_count()
    }

    /// Returns every interpreted effect in semantic order.
    pub fn effect_trace(&self) -> &[ProducerEffect] {
        self.engine.trace()
    }

    /// Returns bytes charged to deterministic producer admission.
    pub const fn retained_bytes(&self) -> ByteCount {
        self.core.retained_bytes()
    }

    /// Returns core completion markers, including engine-retained results.
    pub fn completion_slots(&self) -> usize {
        self.core.completion_slots()
    }

    /// Returns core flush markers, including virtual-engine-retained results.
    pub fn flush_slots(&self) -> usize {
        self.core.flush_slots()
    }
}
