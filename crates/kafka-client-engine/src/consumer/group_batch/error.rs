//! Stable immediate-observation errors for classic-group batches.

use crate::consumer::group::{
    ClassicGroupFetchDeliveryError, GroupConsumerDeliveryError, GroupConsumerDeliveryPortError,
    GroupConsumerShardLockError,
};

/// Stable reason an immediate group-batch observation could not complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerTryTakeBatchErrorKind {
    /// Another owner currently holds the classic-group registry.
    Contended,
    /// Group-consumer observation admission is permanently closed.
    Closed,
    /// The exact registered group is closing or no longer available.
    GroupUnavailable,
    /// Accepted internal effects must settle before delivery can transfer.
    Pending,
    /// The application-processing lease expired at the observation boundary.
    ProcessingExpired,
    /// The synchronized engine host cannot execute group-consumer work.
    HostUnavailable,
    /// A non-semantic engine mechanism violated its ownership contract.
    InternalInvariant,
}

/// Immediate observation rejection before any group byte lease transferred.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupConsumerTryTakeBatchError {
    kind: GroupConsumerTryTakeBatchErrorKind,
}

impl GroupConsumerTryTakeBatchError {
    pub(in crate::consumer) const fn from_port(error: &GroupConsumerDeliveryPortError) -> Self {
        Self {
            kind: error_kind(error),
        }
    }

    /// Returns the stable observation-failure category.
    pub const fn kind(&self) -> GroupConsumerTryTakeBatchErrorKind {
        self.kind
    }
}

impl std::fmt::Display for GroupConsumerTryTakeBatchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "classic-group batch observation failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupConsumerTryTakeBatchError {}

const fn error_kind(error: &GroupConsumerDeliveryPortError) -> GroupConsumerTryTakeBatchErrorKind {
    match error {
        GroupConsumerDeliveryPortError::Closed => GroupConsumerTryTakeBatchErrorKind::Closed,
        GroupConsumerDeliveryPortError::Lock(GroupConsumerShardLockError::Contended) => {
            GroupConsumerTryTakeBatchErrorKind::Contended
        }
        GroupConsumerDeliveryPortError::Lock(GroupConsumerShardLockError::Poisoned)
        | GroupConsumerDeliveryPortError::Registry(
            GroupConsumerDeliveryError::EntryFault
            | GroupConsumerDeliveryError::Clock { .. }
            | GroupConsumerDeliveryError::Fetch(ClassicGroupFetchDeliveryError::Faulted),
        ) => GroupConsumerTryTakeBatchErrorKind::HostUnavailable,
        GroupConsumerDeliveryPortError::Registry(
            GroupConsumerDeliveryError::UnknownGroup | GroupConsumerDeliveryError::Closing,
        ) => GroupConsumerTryTakeBatchErrorKind::GroupUnavailable,
        GroupConsumerDeliveryPortError::Registry(GroupConsumerDeliveryError::Fetch(
            ClassicGroupFetchDeliveryError::EffectsPending,
        )) => GroupConsumerTryTakeBatchErrorKind::Pending,
        GroupConsumerDeliveryPortError::Registry(
            GroupConsumerDeliveryError::ProcessingExpired { .. },
        ) => GroupConsumerTryTakeBatchErrorKind::ProcessingExpired,
        GroupConsumerDeliveryPortError::Registry(
            GroupConsumerDeliveryError::Fetch(_)
            | GroupConsumerDeliveryError::Processing { .. }
            | GroupConsumerDeliveryError::ProcessingEffect { .. },
        ) => GroupConsumerTryTakeBatchErrorKind::InternalInvariant,
    }
}
