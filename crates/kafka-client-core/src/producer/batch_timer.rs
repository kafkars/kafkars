//! Pure timer observation plans and their single batch-owned commit point.

use crate::{BatchTimerGeneration, Deadline, Moment, ProducerMachineError, TransitionError};

use super::{BatchState, BatchTimerObservation, ProducerBatch};

impl ProducerBatch {
    pub(crate) fn plan_timer_observation(
        &self,
        generation: BatchTimerGeneration,
        now: Moment,
    ) -> Result<Option<BatchTimerObservation>, ProducerMachineError> {
        if self.state != BatchState::Open || generation != self.timer_generation {
            return Ok(None);
        }
        if !self.timer_deadline.is_elapsed_at(now) {
            return Err(ProducerMachineError::Transition(
                TransitionError::DeadlineNotElapsed,
            ));
        }
        let linger_elapsed = self.linger_elapsed || self.linger_deadline.is_elapsed_at(now);
        Ok(Some(BatchTimerObservation {
            linger_elapsed,
            readies_batch: self.all_accumulated()
                && (linger_elapsed
                    || self.members.len() >= self.policy.max_records()
                    || self.accumulator_bytes >= self.policy.max_accumulator_bytes()),
        }))
    }

    pub(crate) fn plan_timer_rearm(
        &self,
        observation: BatchTimerObservation,
    ) -> Result<Option<(BatchTimerGeneration, Deadline)>, ProducerMachineError> {
        if observation.readies_batch || self.members.is_empty() {
            return Ok(None);
        }
        let earliest = self
            .earliest_deadline()
            .ok_or(ProducerMachineError::UnknownBatch)?;
        let next = self
            .timer_generation
            .get()
            .checked_add(1)
            .ok_or(ProducerMachineError::TimerGenerationExhausted)?;
        let deadline = if observation.linger_elapsed {
            earliest
        } else {
            earliest.min(self.linger_deadline)
        };
        Ok(Some((BatchTimerGeneration::from_raw(next), deadline)))
    }

    pub(crate) fn commit_timer_observation(
        &mut self,
        observation: BatchTimerObservation,
        timer_update: Option<(BatchTimerGeneration, Deadline)>,
    ) {
        self.linger_elapsed = observation.linger_elapsed;
        if let Some((generation, deadline)) = timer_update {
            self.timer_generation = generation;
            self.timer_deadline = deadline;
        }
    }
}
