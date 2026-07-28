//! Stable failure categories for classic-group event observation.

use crate::consumer::{
    ClassicGroupRevocationAcknowledgeError, GroupConsumerEventPortError,
    GroupConsumerRevocationPortError, GroupConsumerShardLockError,
};

/// Stable reason graceful assignment release could not be completed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerRevocationAcknowledgeErrorKind {
    /// Group or engine admission has closed.
    Closed,
    /// Another owner currently holds the classic-group shard.
    Contended,
    /// The synchronized engine host can no longer expose the group owner.
    HostUnavailable,
    /// The registered group no longer accepts this operation.
    GroupUnavailable,
    /// No active graceful lease matches the supplied assignment epoch.
    StaleAssignmentEpoch,
    /// The one absolute graceful-release deadline already elapsed.
    DeadlineElapsed,
    /// Monotonic time could not be observed.
    Clock,
    /// Internal ownership was inconsistent.
    InternalInvariant,
}

/// Failure to complete one exact graceful-release assignment epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupConsumerRevocationAcknowledgeError {
    kind: GroupConsumerRevocationAcknowledgeErrorKind,
}

impl GroupConsumerRevocationAcknowledgeError {
    pub(in crate::consumer) const fn from_port(error: GroupConsumerRevocationPortError) -> Self {
        use GroupConsumerRevocationAcknowledgeErrorKind as Kind;
        let kind = match error {
            GroupConsumerRevocationPortError::Closed => Kind::Closed,
            GroupConsumerRevocationPortError::Clock(_) => Kind::Clock,
            GroupConsumerRevocationPortError::Lock(GroupConsumerShardLockError::Contended) => {
                Kind::Contended
            }
            GroupConsumerRevocationPortError::Lock(GroupConsumerShardLockError::Poisoned) => {
                Kind::HostUnavailable
            }
            GroupConsumerRevocationPortError::UnknownGroup
            | GroupConsumerRevocationPortError::GroupUnavailable => Kind::GroupUnavailable,
            GroupConsumerRevocationPortError::Acknowledge(
                ClassicGroupRevocationAcknowledgeError::NoActiveLease
                | ClassicGroupRevocationAcknowledgeError::AssignmentEpochMismatch,
            ) => Kind::StaleAssignmentEpoch,
            GroupConsumerRevocationPortError::Acknowledge(
                ClassicGroupRevocationAcknowledgeError::DeadlineElapsed,
            ) => Kind::DeadlineElapsed,
            GroupConsumerRevocationPortError::Acknowledge(
                ClassicGroupRevocationAcknowledgeError::Core(_)
                | ClassicGroupRevocationAcknowledgeError::UnexpectedEffect,
            ) => Kind::InternalInvariant,
        };
        Self { kind }
    }

    /// Returns the stable completion failure category.
    pub const fn kind(self) -> GroupConsumerRevocationAcknowledgeErrorKind {
        self.kind
    }
}

impl std::fmt::Display for GroupConsumerRevocationAcknowledgeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "classic-group revocation completion failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupConsumerRevocationAcknowledgeError {}

/// Stable reason current confirmed membership could not be observed immediately.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerStateErrorKind {
    /// Another owner currently holds the classic-group shard.
    Contended,
    /// The synchronized engine host can no longer expose group state.
    HostUnavailable,
    /// Snapshot storage could not be reserved within the bounded assignment.
    Allocation,
    /// Internal group ownership was inconsistent.
    InternalInvariant,
}

/// Failure to observe current confirmed membership without waiting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupConsumerStateError {
    kind: GroupConsumerStateErrorKind,
}

impl GroupConsumerStateError {
    pub(in crate::consumer) const fn new(kind: GroupConsumerStateErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable immediate-observation category.
    pub const fn kind(self) -> GroupConsumerStateErrorKind {
        self.kind
    }
}

impl std::fmt::Display for GroupConsumerStateError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "classic-group state observation failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupConsumerStateError {}

/// Stable reason immediate event observation could not complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerTryTakeEventErrorKind {
    /// Another owner or caller currently holds the classic-group shard.
    Contended,
    /// The synchronized engine host can no longer expose retained events.
    HostUnavailable,
    /// A non-semantic engine mechanism violated its ownership contract.
    InternalInvariant,
}

/// Immediate observation failure before a retained event transferred.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupConsumerTryTakeEventError {
    kind: GroupConsumerTryTakeEventErrorKind,
}

impl GroupConsumerTryTakeEventError {
    pub(in crate::consumer) const fn from_port(error: GroupConsumerEventPortError) -> Self {
        Self {
            kind: match error {
                GroupConsumerEventPortError::Lock(GroupConsumerShardLockError::Contended) => {
                    GroupConsumerTryTakeEventErrorKind::Contended
                }
                GroupConsumerEventPortError::Lock(GroupConsumerShardLockError::Poisoned) => {
                    GroupConsumerTryTakeEventErrorKind::HostUnavailable
                }
                GroupConsumerEventPortError::Closed | GroupConsumerEventPortError::Registry(_) => {
                    GroupConsumerTryTakeEventErrorKind::InternalInvariant
                }
            },
        }
    }

    /// Returns the stable immediate-observation category.
    pub const fn kind(&self) -> GroupConsumerTryTakeEventErrorKind {
        self.kind
    }
}

impl std::fmt::Display for GroupConsumerTryTakeEventError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "classic-group immediate event observation failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupConsumerTryTakeEventError {}

/// Stable reason one event observation could not continue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerNextEventErrorKind {
    /// The embedded host or registered group is unavailable.
    HostUnavailable,
    /// Event ownership or notification state was inconsistent.
    InternalInvariant,
}

/// Event observation failure with a stable public category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupConsumerNextEventError {
    kind: GroupConsumerNextEventErrorKind,
}

impl GroupConsumerNextEventError {
    pub(in crate::consumer) const fn host_unavailable() -> Self {
        Self {
            kind: GroupConsumerNextEventErrorKind::HostUnavailable,
        }
    }

    pub(in crate::consumer) const fn internal_invariant() -> Self {
        Self {
            kind: GroupConsumerNextEventErrorKind::InternalInvariant,
        }
    }

    /// Returns the stable event-observation category.
    pub const fn kind(self) -> GroupConsumerNextEventErrorKind {
        self.kind
    }
}

impl std::fmt::Display for GroupConsumerNextEventError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "classic-group event observation failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupConsumerNextEventError {}
