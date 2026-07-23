//! Transition-level FIFO execution of core effects and generated mechanism facts.

use std::collections::VecDeque;

use kafka_client_core::{Moment, ProducerInput, ProducerTransition};

use super::{ProducerHost, ProducerHostInvariantError};

impl ProducerHost {
    pub(super) fn interpret_transition(
        &mut self,
        now: Moment,
        initial: ProducerTransition,
    ) -> Result<(), ProducerHostInvariantError> {
        let mut generated = VecDeque::with_capacity(self.effect_capacity);
        self.interpret_effects(now, initial, &mut generated)?;
        while let Some(input) = generated.pop_front() {
            let transition = self
                .core
                .apply(input)
                .map_err(ProducerHostInvariantError::Core)?;
            self.interpret_effects(now, transition, &mut generated)?;
        }
        Ok(())
    }

    /// Applies bounded due timer facts and drains each resulting transition.
    pub(crate) fn fire_due(
        &mut self,
        now: Moment,
        limit: usize,
    ) -> Result<usize, ProducerHostInvariantError> {
        if let Some(error) = self.poison_reason() {
            return Err(error);
        }
        let due = self.timers.drain_due(now, limit);
        let count = due.len();
        for timer in due {
            let transition = match self.core.apply(ProducerInput::BatchTimerFired {
                batch_id: timer.batch_id(),
                generation: timer.generation(),
                now,
            }) {
                Ok(transition) => transition,
                Err(error) => {
                    let invariant = ProducerHostInvariantError::Core(error);
                    return Err(self.poison(invariant));
                }
            };
            if let Err(error) = self.interpret_transition(now, transition) {
                return Err(self.poison(error));
            }
        }
        Ok(count)
    }

    fn interpret_effects(
        &mut self,
        now: Moment,
        transition: ProducerTransition,
        generated: &mut VecDeque<ProducerInput>,
    ) -> Result<(), ProducerHostInvariantError> {
        for effect in transition.into_effects() {
            if let Some(input) = self.interpret_effect(now, effect)? {
                if generated.len() >= self.effect_capacity {
                    return Err(ProducerHostInvariantError::GeneratedFactCapacity);
                }
                generated.push_back(input);
            }
        }
        Ok(())
    }
}
