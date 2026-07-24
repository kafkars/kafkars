//! Unrestricted named-event extraction from one synchronized assigned consumer.

use super::super::assigned_owner::AssignedConsumerOwner;
use super::{
    event::{AssignedConsumerEvent, translate_retained_event},
    result::AssignedConsumerPortError,
    shard::AssignedConsumerPort,
};

impl AssignedConsumerPort {
    pub(crate) fn take_event(
        &self,
    ) -> Result<Option<AssignedConsumerEvent>, AssignedConsumerPortError> {
        self.shared
            .try_with_owner(AssignedConsumerOwner::take_event)
            .map(|event| event.map(translate_retained_event))
            .map_err(AssignedConsumerPortError::Lock)
    }
}
