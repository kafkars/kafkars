//! Stable immediate-observation errors for classic-group batches.

use crate::consumer::group::{
    ClassicGroupFetchDeliveryError, GroupConsumerDeliveryError, GroupConsumerDeliveryPortError,
    GroupConsumerShardLockError,
};

/// Stable group-position bootstrap or reset failure preserved at delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerPositionFailureKind {
    /// The configured Error policy found an assigned partition without an offset.
    MissingOffset,
    /// The original absolute position-bootstrap deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected the position request before accepting transport ownership.
    DriverRejected,
    /// Transport failed after accepting the position request.
    Transport,
    /// Kafka rejected the group or one assigned partition with this exact signed code.
    Broker(i16),
    /// The broker cannot execute the selected position protocol shape.
    Compatibility,
    /// Kafka returned a structurally invalid position response.
    InvalidResponse,
    /// The position response exceeded its configured retained-byte limit.
    ResponseTooLarge,
}

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
    /// Group-position bootstrap or reset reached one exact terminal outcome.
    Position(GroupConsumerPositionFailureKind),
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
        ))
        | GroupConsumerDeliveryPortError::Registry(GroupConsumerDeliveryError::Revoking) => {
            GroupConsumerTryTakeBatchErrorKind::Pending
        }
        GroupConsumerDeliveryPortError::Registry(
            GroupConsumerDeliveryError::ProcessingExpired { .. },
        ) => GroupConsumerTryTakeBatchErrorKind::ProcessingExpired,
        GroupConsumerDeliveryPortError::Registry(GroupConsumerDeliveryError::PositionFailure(
            failure,
        )) => GroupConsumerTryTakeBatchErrorKind::Position(*failure),
        GroupConsumerDeliveryPortError::Registry(
            GroupConsumerDeliveryError::Fetch(_)
            | GroupConsumerDeliveryError::Processing { .. }
            | GroupConsumerDeliveryError::ProcessingEffect { .. },
        ) => GroupConsumerTryTakeBatchErrorKind::InternalInvariant,
    }
}
