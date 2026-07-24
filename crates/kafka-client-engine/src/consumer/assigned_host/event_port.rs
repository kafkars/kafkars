//! Unrestricted scalar-event extraction from one synchronized assigned consumer.

use super::super::{assigned_event::AssignedConsumerEvent, assigned_owner::AssignedConsumerOwner};
use super::{result::AssignedConsumerPortError, shard::AssignedConsumerPort};

impl AssignedConsumerPort {
    pub(crate) fn take_event(
        &self,
    ) -> Result<Option<AssignedConsumerEvent>, AssignedConsumerPortError> {
        self.shared
            .try_with_owner(AssignedConsumerOwner::take_event)
            .map_err(AssignedConsumerPortError::Lock)
    }
}
