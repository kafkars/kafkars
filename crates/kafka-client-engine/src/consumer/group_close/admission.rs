//! Lossless admission of one explicit linear group-consumer close.

use super::super::{
    GroupConsumerHandle,
    group::{
        GroupConsumerClosePortError, GroupConsumerShardLockError,
        GroupRegistryCloseError as RegistryCloseError,
    },
};

use super::GroupConsumerClose;

/// Stable reason explicit close did not transfer the unique consumer handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerCloseAdmissionErrorKind {
    /// Engine-wide group admission has already closed.
    Closed,
    /// Another host or application owner currently holds the group registry.
    Contended,
    /// The exact group is unavailable or already closing.
    GroupUnavailable,
    /// The group host retained a fault that requires engine shutdown recovery.
    HostUnavailable,
    /// Terminal reservation or registry ownership violated an invariant.
    InternalInvariant,
}

/// Pre-admission close rejection retaining the exact unique handle.
#[must_use = "close rejection retains the exact group-consumer handle"]
pub struct GroupConsumerCloseAdmissionError {
    kind: GroupConsumerCloseAdmissionErrorKind,
    handle: GroupConsumerHandle,
}

impl GroupConsumerCloseAdmissionError {
    /// Returns the stable pre-admission rejection category.
    pub const fn kind(&self) -> GroupConsumerCloseAdmissionErrorKind {
        self.kind
    }

    /// Recovers the exact consumer whose close did not begin.
    pub fn into_handle(self) -> GroupConsumerHandle {
        self.handle
    }
}

impl core::fmt::Debug for GroupConsumerCloseAdmissionError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("GroupConsumerCloseAdmissionError")
            .field("kind", &self.kind)
            .field("handle", &self.handle)
            .finish()
    }
}

impl core::fmt::Display for GroupConsumerCloseAdmissionError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            formatter,
            "classic-group consumer close rejected: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupConsumerCloseAdmissionError {}

impl GroupConsumerHandle {
    /// Begins one explicit close after reserving terminal observation capacity.
    ///
    /// Rejection returns this exact unique handle. Acceptance consumes it,
    /// permanently fences new group work, and continues if observation drops.
    pub fn try_close(self) -> Result<GroupConsumerClose, GroupConsumerCloseAdmissionError> {
        match self
            .port
            .try_begin_close(self.group_id, &self.close_authority)
        {
            Ok(admission) => {
                let wake_failed = admission.wake_failed();
                Ok(GroupConsumerClose::new(
                    self.group_id,
                    self.port,
                    admission.completion,
                    admission.registration,
                    wake_failed,
                    self.lifetime,
                ))
            }
            Err(error) => Err(GroupConsumerCloseAdmissionError {
                kind: admission_error_kind(error),
                handle: self,
            }),
        }
    }
}

pub(super) const fn admission_error_kind(
    error: GroupConsumerClosePortError,
) -> GroupConsumerCloseAdmissionErrorKind {
    match error {
        GroupConsumerClosePortError::Closed => GroupConsumerCloseAdmissionErrorKind::Closed,
        GroupConsumerClosePortError::Clock(_)
        | GroupConsumerClosePortError::Notification
        | GroupConsumerClosePortError::Lock(GroupConsumerShardLockError::Poisoned) => {
            GroupConsumerCloseAdmissionErrorKind::InternalInvariant
        }
        GroupConsumerClosePortError::Lock(GroupConsumerShardLockError::Contended) => {
            GroupConsumerCloseAdmissionErrorKind::Contended
        }
        GroupConsumerClosePortError::Registry(RegistryCloseError::AuthorityContended) => {
            GroupConsumerCloseAdmissionErrorKind::Contended
        }
        GroupConsumerClosePortError::Registry(
            RegistryCloseError::UnknownGroup | RegistryCloseError::AlreadyClosing,
        ) => GroupConsumerCloseAdmissionErrorKind::GroupUnavailable,
        GroupConsumerClosePortError::Registry(RegistryCloseError::EntryFault) => {
            GroupConsumerCloseAdmissionErrorKind::HostUnavailable
        }
    }
}
