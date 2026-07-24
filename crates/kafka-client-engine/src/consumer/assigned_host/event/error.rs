//! Stable failures for immediate assigned-consumer event observation.

use super::super::{result::AssignedConsumerPortError, shard::AssignedConsumerShardLockError};
use crate::consumer::assigned_owner_model::AssignedConsumerOwnerError;

/// Stable reason an immediate event observation could not complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerTryTakeEventErrorKind {
    /// Another owner or caller currently holds the assigned-consumer shard.
    Contended,
    /// The synchronized engine host can no longer expose retained events.
    HostUnavailable,
    /// A non-semantic engine mechanism violated its ownership contract.
    InternalInvariant,
}

/// Immediate observation failure before a retained event transferred.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssignedConsumerTryTakeEventError {
    kind: AssignedConsumerTryTakeEventErrorKind,
}

impl AssignedConsumerTryTakeEventError {
    pub(in crate::consumer::assigned_host) const fn from_port(
        error: &AssignedConsumerPortError,
    ) -> Self {
        Self {
            kind: match error {
                AssignedConsumerPortError::Lock(AssignedConsumerShardLockError::Contended) => {
                    AssignedConsumerTryTakeEventErrorKind::Contended
                }
                AssignedConsumerPortError::Lock(
                    AssignedConsumerShardLockError::Poisoned
                    | AssignedConsumerShardLockError::OwnerMissing,
                )
                | AssignedConsumerPortError::Owner {
                    error: AssignedConsumerOwnerError::Faulted,
                    ..
                } => AssignedConsumerTryTakeEventErrorKind::HostUnavailable,
                AssignedConsumerPortError::Clock(_)
                | AssignedConsumerPortError::Closed
                | AssignedConsumerPortError::Owner { .. } => {
                    AssignedConsumerTryTakeEventErrorKind::InternalInvariant
                }
            },
        }
    }

    /// Returns the stable observation-failure category.
    pub const fn kind(&self) -> AssignedConsumerTryTakeEventErrorKind {
        self.kind
    }
}

impl std::fmt::Display for AssignedConsumerTryTakeEventError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "assigned-consumer event observation failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AssignedConsumerTryTakeEventError {}
