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
        let transition = self
            .core
            .apply(input)
            .map_err(|error| self.poison(ProducerHostInvariantError::Core(error)))?;
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
            | ProducerInput::TransportFailed { .. }
    )
}
