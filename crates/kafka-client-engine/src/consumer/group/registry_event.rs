//! Immediate group-selected observation of retained assignment transitions.

use kafka_client_core::GroupId;

use crate::consumer::GroupConsumerEvent;

use super::{
    registry::GroupConsumerRegistry, registry_entry::GroupConsumerEntryState,
    registry_port::GroupConsumerPort, registry_shard::GroupConsumerShardLockError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerEventError {
    UnknownGroup,
    Closing,
    EntryFault,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerEventPortError {
    Closed,
    Lock(GroupConsumerShardLockError),
    Registry(GroupConsumerEventError),
}

impl GroupConsumerEventPortError {
    pub(in crate::consumer) const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Closed
                | Self::Registry(
                    GroupConsumerEventError::UnknownGroup
                        | GroupConsumerEventError::Closing
                        | GroupConsumerEventError::EntryFault
                )
        )
    }

    pub(in crate::consumer) const fn is_contended(self) -> bool {
        matches!(self, Self::Lock(GroupConsumerShardLockError::Contended))
    }

    pub(in crate::consumer) const fn is_host_unavailable(self) -> bool {
        matches!(self, Self::Lock(GroupConsumerShardLockError::Poisoned))
    }
}

impl GroupConsumerRegistry {
    pub(in crate::consumer::group) fn take_event(
        &mut self,
        group_id: GroupId,
    ) -> Result<Option<GroupConsumerEvent>, GroupConsumerEventError> {
        let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
        else {
            return Err(GroupConsumerEventError::UnknownGroup);
        };
        if let Some(event) = entry.catalog.take_event() {
            return Ok(Some(event));
        }
        if entry.state == GroupConsumerEntryState::Closing {
            return Err(GroupConsumerEventError::Closing);
        }
        if entry.fault.is_some() {
            return Err(GroupConsumerEventError::EntryFault);
        }
        Ok(None)
    }
}

impl GroupConsumerPort {
    pub(in crate::consumer) fn try_take_event(
        &self,
        group_id: GroupId,
    ) -> Result<Option<GroupConsumerEvent>, GroupConsumerEventPortError> {
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerEventPortError::Closed);
        }
        let mut registry = self
            .shared
            .try_registry()
            .map_err(GroupConsumerEventPortError::Lock)?;
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerEventPortError::Closed);
        }
        registry
            .take_event(group_id)
            .map_err(GroupConsumerEventPortError::Registry)
    }
}
