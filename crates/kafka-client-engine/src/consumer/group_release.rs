//! Lossless public release of a never-started classic-group registration.

use super::{GroupConsumerHandle, GroupConsumerPortDormantReleaseError};

/// Stable reason a dormant registration could not be released.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerDormantReleaseErrorKind {
    /// Engine or group admission has closed.
    Closed,
    /// The exact registration is no longer present.
    GroupUnavailable,
    /// Membership or another group mechanism has already begun.
    NotDormant,
    /// An engine ownership invariant prevented release.
    Internal,
}

/// Failed dormant release retaining the exact unique handle.
#[must_use = "release rejection retains the exact registered group handle"]
pub struct GroupConsumerDormantReleaseError {
    kind: GroupConsumerDormantReleaseErrorKind,
    handle: GroupConsumerHandle,
}

impl GroupConsumerDormantReleaseError {
    /// Returns the stable release-failure category.
    pub const fn kind(&self) -> GroupConsumerDormantReleaseErrorKind {
        self.kind
    }

    /// Returns the exact handle whose registration remains retained.
    pub fn into_handle(self) -> GroupConsumerHandle {
        self.handle
    }
}

impl core::fmt::Debug for GroupConsumerDormantReleaseError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("GroupConsumerDormantReleaseError")
            .field("kind", &self.kind)
            .field("handle", &self.handle)
            .finish()
    }
}

impl core::fmt::Display for GroupConsumerDormantReleaseError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            formatter,
            "dormant group-consumer release failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupConsumerDormantReleaseError {}

impl GroupConsumerHandle {
    /// Releases this exact registration only if membership never started.
    ///
    /// This waits for the short registry critical section so transient host
    /// contention cannot leak bounded registration capacity during rollback.
    pub fn release_dormant(self) -> Result<(), GroupConsumerDormantReleaseError> {
        match self.port.release_dormant_registration(self.group_id) {
            Ok(()) => Ok(()),
            Err(error) => {
                let kind = match error {
                    GroupConsumerPortDormantReleaseError::Closed => {
                        GroupConsumerDormantReleaseErrorKind::Closed
                    }
                    GroupConsumerPortDormantReleaseError::UnknownGroup => {
                        GroupConsumerDormantReleaseErrorKind::GroupUnavailable
                    }
                    GroupConsumerPortDormantReleaseError::NotDormant => {
                        GroupConsumerDormantReleaseErrorKind::NotDormant
                    }
                    GroupConsumerPortDormantReleaseError::Contended
                    | GroupConsumerPortDormantReleaseError::InternalInvariant => {
                        GroupConsumerDormantReleaseErrorKind::Internal
                    }
                };
                Err(GroupConsumerDormantReleaseError { kind, handle: self })
            }
        }
    }
}
