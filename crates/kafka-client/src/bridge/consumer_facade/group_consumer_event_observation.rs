//! Event observation methods on one unique classic-group bridge owner.

use super::{
    group_consumer::GroupConsumerEngine,
    group_consumer_next_event::GroupConsumerNextEvent,
    group_consumer_rebalance_event::{
        translate_group_consumer_event, translate_group_consumer_event_observation,
    },
};
use crate::{KafkaError, consumer::ConsumerEvent};

impl GroupConsumerEngine {
    /// Observes one retained assignment transition without protocol work.
    pub(crate) fn next_event(&mut self) -> GroupConsumerNextEvent<'_> {
        if let Some(error) = &self.startup_fault {
            GroupConsumerNextEvent::rejected(error.clone())
        } else {
            let revocation = self.handle.revocation_control();
            GroupConsumerNextEvent::from_engine(self.handle.next_event(), revocation)
        }
    }

    /// Transfers one retained assignment transition without waiting.
    pub(crate) fn try_take_event(&mut self) -> Result<Option<ConsumerEvent>, KafkaError> {
        if let Some(error) = &self.startup_fault {
            return Err(error.clone());
        }
        let revocation = self.handle.revocation_control();
        self.handle
            .try_take_event()
            .map(|event| event.map(|event| translate_group_consumer_event(event, revocation)))
            .map_err(translate_group_consumer_event_observation)
    }
}
