//! Virtual-time ordering of batch timers and producer-identity backoff.

use kafka_client_core::{Moment, ProducerInput};

use crate::SimulationError;

use super::ProducerScenario;

impl ProducerScenario {
    /// Advances virtual time and dispatches every due mechanism in host order.
    pub fn advance(&mut self, ticks: u64) -> Result<(), SimulationError> {
        let target = self
            .clock
            .target_after(ticks)
            .map_err(SimulationError::Time)?;
        loop {
            let timer = self.engine.next_timer_deadline_before(target);
            let retry = self.engine.identity_retry_before(target);
            if timer.is_none() && retry.is_none() {
                break;
            }
            if retry.is_none_or(|schedule| {
                timer.is_some_and(|deadline| deadline < schedule.not_before())
            }) {
                let Some((batch_id, generation, deadline)) = self.engine.take_timer_before(target)
                else {
                    return Err(SimulationError::DuplicateProducerIdentityRetry);
                };
                self.clock.set(Moment::from_tick(deadline.tick()));
                self.step(ProducerInput::BatchTimerFired {
                    batch_id,
                    generation,
                    now: self.clock.now(),
                })?;
                continue;
            }
            let Some(schedule) = retry else {
                return Err(SimulationError::DuplicateProducerIdentityRetry);
            };
            let Some(schedule) = self.engine.take_identity_retry(schedule) else {
                return Err(SimulationError::DuplicateProducerIdentityRetry);
            };
            self.clock
                .set(Moment::from_tick(schedule.not_before().tick()));
            self.step(ProducerInput::ProducerIdentityRetryDue {
                schedule,
                now: self.clock.now(),
            })?;
        }
        self.clock.set(target);
        Ok(())
    }
}
