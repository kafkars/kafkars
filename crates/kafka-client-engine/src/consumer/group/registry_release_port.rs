//! Exact dormant-registration release over the synchronized group shard.

use kafka_client_core::GroupId;

use super::{
    registry_port::GroupConsumerPort, registry_shard::GroupConsumerShardLockError,
    registry_unregister::GroupConsumerDormantUnregisterError,
};

impl GroupConsumerPort {
    /// Waits for shard ownership and releases only an exact never-started registration.
    pub(crate) fn release_dormant_registration(
        &self,
        group_id: GroupId,
    ) -> Result<(), GroupConsumerPortDormantReleaseError> {
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerPortDormantReleaseError::Closed);
        }
        let mut registry = self
            .shared
            .registry()
            .map_err(GroupConsumerPortDormantReleaseError::lock)?;
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerPortDormantReleaseError::Closed);
        }
        registry
            .try_unregister_dormant(group_id)
            .map_err(GroupConsumerPortDormantReleaseError::registry)
    }
}

/// Stable rollback classification for one dormant registration release.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupConsumerPortDormantReleaseError {
    Closed,
    Contended,
    UnknownGroup,
    NotDormant,
    InternalInvariant,
}

impl GroupConsumerPortDormantReleaseError {
    const fn lock(error: GroupConsumerShardLockError) -> Self {
        match error {
            GroupConsumerShardLockError::Contended => Self::Contended,
            GroupConsumerShardLockError::Poisoned => Self::InternalInvariant,
        }
    }

    const fn registry(error: GroupConsumerDormantUnregisterError) -> Self {
        match error {
            GroupConsumerDormantUnregisterError::RegistryClosing => Self::Closed,
            GroupConsumerDormantUnregisterError::UnknownGroup => Self::UnknownGroup,
            GroupConsumerDormantUnregisterError::GroupClosing
            | GroupConsumerDormantUnregisterError::NotDormant => Self::NotDormant,
            GroupConsumerDormantUnregisterError::EntryFault
            | GroupConsumerDormantUnregisterError::RetainedBytesInvariant => {
                Self::InternalInvariant
            }
        }
    }
}
