//! Non-blocking host turns for core-confirmed terminal completion reclamation.

use kafka_client_core::Moment;

use super::{
    ProducerHost, ProducerHostInvariantError,
    reclaim::{CompletionReclaimError, CompletionReclaimOutcome},
};

impl ProducerHost {
    pub(super) const fn reclaim_finish_pending(&self) -> bool {
        self.reclaimer.finish_pending()
    }

    /// Advances at most one reclaim identity without blocking the reactor.
    ///
    /// `Retry` retains the exact finishing phase. A later call retries only
    /// engine recycling and never re-emits `CompletionReclaimed` to core.
    pub(crate) fn reclaim_one(
        &mut self,
        now: Moment,
    ) -> Result<Option<CompletionReclaimOutcome>, ProducerHostInvariantError> {
        if let Some(error) = self.poison_reason() {
            return Err(error);
        }
        if self.reclaimer.finish_pending() {
            let result = {
                let reclaimer = &mut self.reclaimer;
                reclaimer.retry_finish(&mut self.completions, &mut self.bindings)
            };
            return self.finish_reclaim(result).map(Some);
        }

        let next = {
            let reclaimer = &mut self.reclaimer;
            reclaimer.next_input(&mut self.completions, &self.bindings)
        };
        let input = match next {
            Ok(Some(input)) => input,
            Ok(None) => return Ok(None),
            Err(error) => return Err(self.poison_reclaim(error)),
        };
        let transition = match self.core.apply(input) {
            Ok(transition) => transition,
            Err(error) => {
                return Err(self.poison(ProducerHostInvariantError::Core(error)));
            }
        };
        if let Err(error) = self.interpret_transition(now, transition) {
            return Err(self.poison(error));
        }
        let result = {
            let reclaimer = &mut self.reclaimer;
            reclaimer.confirm_core_applied(&mut self.completions, &mut self.bindings)
        };
        self.finish_reclaim(result).map(Some)
    }

    fn finish_reclaim(
        &mut self,
        result: Result<CompletionReclaimOutcome, CompletionReclaimError>,
    ) -> Result<CompletionReclaimOutcome, ProducerHostInvariantError> {
        result.map_err(|error| self.poison_reclaim(error))
    }

    fn poison_reclaim(&mut self, error: CompletionReclaimError) -> ProducerHostInvariantError {
        self.poison(ProducerHostInvariantError::Reclaim(error))
    }
}
