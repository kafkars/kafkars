//! Explicit deterministic steps joining producer policy to virtual engine ownership.

use kafka_client_core::{
    BatchId, ByteCount, Moment, OperationId, PayloadId, ProducerBatchPolicy, ProducerCompletion,
    ProducerEffect, ProducerInput, ProducerMachine, ProducerTransition,
};

use crate::{SimulationError, VirtualClock, state::VirtualProducerState};

/// Deterministic producer policy plus a minimal effect interpreter.
#[derive(Debug)]
pub struct ProducerScenario {
    clock: VirtualClock,
    core: ProducerMachine,
    engine: VirtualProducerState,
}

impl ProducerScenario {
    /// Creates an empty scenario with hard byte and completion limits.
    pub fn new(retained_bytes: ByteCount, completion_capacity: usize) -> Self {
        Self {
            clock: VirtualClock::default(),
            core: ProducerMachine::new(retained_bytes, completion_capacity),
            engine: VirtualProducerState::default(),
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
            engine: VirtualProducerState::default(),
        }
    }

    /// Returns the current virtual monotonic observation.
    pub const fn now(&self) -> Moment {
        self.clock.now()
    }

    /// Advances virtual time and deterministically dispatches every due batch timer.
    pub fn advance(&mut self, ticks: u64) -> Result<(), SimulationError> {
        let target = self
            .clock
            .target_after(ticks)
            .map_err(SimulationError::Time)?;
        while let Some((batch_id, generation, deadline)) = self.engine.take_timer_before(target) {
            self.clock.set(Moment::from_tick(deadline.tick()));
            self.step(ProducerInput::BatchTimerFired {
                batch_id,
                generation,
                now: self.clock.now(),
            })?;
        }
        self.clock.set(target);
        Ok(())
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
        let reclaimed = match input {
            ProducerInput::CompletionReclaimed { operation_id } => {
                self.engine.require_released_terminal(operation_id)?;
                Some(operation_id)
            }
            _ => None,
        };
        let transition = self.core.apply(input).map_err(SimulationError::Core)?;
        for effect in transition.effects() {
            self.engine.interpret(*effect)?;
        }
        if let Some(operation_id) = reclaimed {
            self.engine.finish_reclaim(operation_id);
        }
        Ok(transition)
    }

    /// Releases the engine-owned terminal result without reclaiming core capacity.
    pub fn release_terminal_result(
        &mut self,
        operation_id: OperationId,
    ) -> Result<ProducerCompletion, SimulationError> {
        self.engine.release_terminal(operation_id)
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
}
