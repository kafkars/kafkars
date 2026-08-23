//! Stable immediate-observation errors for share-consumer batches.

use crate::consumer::share::{
    ShareConsumerDeliveryError, ShareConsumerDeliveryPortError, ShareConsumerShardLockError,
};

/// Stable reason an immediate share-batch observation could not complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareConsumerTryTakeBatchErrorKind {
    /// Another owner currently holds the share registry.
    Contended,
    /// Share-consumer observation admission is permanently closed.
    Closed,
    /// The exact registered share member is closing or no longer available.
    ConsumerUnavailable,
    /// Membership, routing, or broker-session effects must settle first.
    Pending,
    /// Hosted membership or Fetch execution ended terminally.
    HostUnavailable,
    /// A non-semantic engine mechanism violated its ownership contract.
    InternalInvariant,
}

/// Immediate observation rejection before any share capability transferred.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareConsumerTryTakeBatchError {
    kind: ShareConsumerTryTakeBatchErrorKind,
}

impl ShareConsumerTryTakeBatchError {
    pub(in crate::consumer) const fn from_port(error: ShareConsumerDeliveryPortError) -> Self {
        Self {
            kind: match error {
                ShareConsumerDeliveryPortError::Closed => {
                    ShareConsumerTryTakeBatchErrorKind::Closed
                }
                ShareConsumerDeliveryPortError::Lock(ShareConsumerShardLockError::Contended) => {
                    ShareConsumerTryTakeBatchErrorKind::Contended
                }
                ShareConsumerDeliveryPortError::Clock
                | ShareConsumerDeliveryPortError::Lock(ShareConsumerShardLockError::Poisoned)
                | ShareConsumerDeliveryPortError::Registry(
                    ShareConsumerDeliveryError::MembershipFault
                    | ShareConsumerDeliveryError::FetchFault,
                ) => ShareConsumerTryTakeBatchErrorKind::HostUnavailable,
                ShareConsumerDeliveryPortError::Registry(
                    ShareConsumerDeliveryError::UnknownConsumer
                    | ShareConsumerDeliveryError::Closing,
                ) => ShareConsumerTryTakeBatchErrorKind::ConsumerUnavailable,
                ShareConsumerDeliveryPortError::Registry(ShareConsumerDeliveryError::Pending) => {
                    ShareConsumerTryTakeBatchErrorKind::Pending
                }
                ShareConsumerDeliveryPortError::Registry(
                    ShareConsumerDeliveryError::TransferInvariant,
                ) => ShareConsumerTryTakeBatchErrorKind::InternalInvariant,
            },
        }
    }

    /// Returns the stable observation-failure category.
    pub const fn kind(self) -> ShareConsumerTryTakeBatchErrorKind {
        self.kind
    }
}

impl std::fmt::Display for ShareConsumerTryTakeBatchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "share-consumer batch observation failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for ShareConsumerTryTakeBatchError {}
