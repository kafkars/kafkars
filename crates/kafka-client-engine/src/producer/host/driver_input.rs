//! One-at-a-time application of concrete driver outcomes to producer policy.

use kafka_client_core::{Moment, ProducerInput};

use super::ProducerHost;
use crate::producer::ProducerHostInvariantError;

impl ProducerHost {
    /// Applies one transport-owned fact and interprets all resulting effects.
    pub(crate) fn apply_one_driver_input(
        &mut self,
        now: Moment,
        input: ProducerInput,
    ) -> Result<(), ProducerHostInvariantError> {
        if let Some(error) = self.poison_reason() {
            return Err(error);
        }
        if !is_driver_input(input) {
            return Err(self.poison(ProducerHostInvariantError::UnexpectedDriverInput));
        }
        let acceptance = match input {
            ProducerInput::DriverAccepted { execution } => Some(
                self.store
                    .plan_driver_accepted(execution)
                    .map_err(|error| self.poison(ProducerHostInvariantError::Store(error)))?,
            ),
            _ => None,
        };
        let transition = self
            .core
            .apply(input)
            .map_err(|error| self.poison(ProducerHostInvariantError::Core(error)))?;
        if let Some(acceptance) = acceptance {
            self.store.commit_driver_accepted(acceptance);
        }
        self.interpret_transition(now, transition)
            .map_err(|error| self.poison(error))
    }
}

const fn is_driver_input(input: ProducerInput) -> bool {
    matches!(
        input,
        ProducerInput::DriverAccepted { .. }
            | ProducerInput::DriverRejected { .. }
            | ProducerInput::BrokerSucceeded { .. }
            | ProducerInput::BrokerFailed { .. }
            | ProducerInput::RouteRefreshDeadlineElapsed { .. }
            | ProducerInput::TransportFailed { .. }
            | ProducerInput::ProducerIdentityAcquired { .. }
            | ProducerInput::ProducerIdentityFailed { .. }
            | ProducerInput::ProducerIdentityDeadlineElapsed { .. }
            | ProducerInput::ProducerIdentityRequestFailed { .. }
    )
}
