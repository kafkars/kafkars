//! Stable immediate-observation failures for assigned-consumer batches.

use super::super::{result::AssignedConsumerPortError, shard::AssignedConsumerShardLockError};
use crate::consumer::assigned_owner_model::AssignedConsumerOwnerError;

/// Stable reason an immediate batch observation could not complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerTryTakeBatchErrorKind {
    /// Another owner or caller currently holds the assigned-consumer shard.
    Contended,
    /// Consumer admission is permanently closed.
    Closed,
    /// Accepted control effects must settle before another batch can transfer.
    Pending,
    /// The synchronized engine host cannot execute consumer work.
    HostUnavailable,
    /// A non-semantic engine mechanism violated its ownership contract.
    InternalInvariant,
}

/// Immediate observation rejection before any delivery lease transferred.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssignedConsumerTryTakeBatchError {
    kind: AssignedConsumerTryTakeBatchErrorKind,
}

impl AssignedConsumerTryTakeBatchError {
    pub(crate) const fn from_port(error: &AssignedConsumerPortError) -> Self {
        Self {
            kind: error_kind(error),
        }
    }

    /// Returns the stable observation-failure category.
    pub const fn kind(&self) -> AssignedConsumerTryTakeBatchErrorKind {
        self.kind
    }
}

impl std::fmt::Display for AssignedConsumerTryTakeBatchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "assigned-consumer batch observation failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AssignedConsumerTryTakeBatchError {}

const fn error_kind(error: &AssignedConsumerPortError) -> AssignedConsumerTryTakeBatchErrorKind {
    match error {
        AssignedConsumerPortError::Closed => AssignedConsumerTryTakeBatchErrorKind::Closed,
        AssignedConsumerPortError::Lock(AssignedConsumerShardLockError::Contended) => {
            AssignedConsumerTryTakeBatchErrorKind::Contended
        }
        AssignedConsumerPortError::Lock(
            AssignedConsumerShardLockError::Poisoned | AssignedConsumerShardLockError::OwnerMissing,
        )
        | AssignedConsumerPortError::Owner {
            error: AssignedConsumerOwnerError::Faulted,
            ..
        } => AssignedConsumerTryTakeBatchErrorKind::HostUnavailable,
        AssignedConsumerPortError::Owner {
            error:
                AssignedConsumerOwnerError::EffectsPending
                | AssignedConsumerOwnerError::DeliveryUnavailable,
            ..
        } => AssignedConsumerTryTakeBatchErrorKind::Pending,
        AssignedConsumerPortError::Clock(_) | AssignedConsumerPortError::Owner { .. } => {
            AssignedConsumerTryTakeBatchErrorKind::InternalInvariant
        }
    }
}
