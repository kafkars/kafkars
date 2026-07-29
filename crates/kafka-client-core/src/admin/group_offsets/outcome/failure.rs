//! Whole-operation group-offset failures and terminal decisions.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::{ListConsumerGroupOffsetsBatch, ListConsumerGroupsOffsetsBatch};

/// Whole-operation failure category outside partition results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListConsumerGroupOffsetsFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected the request before transport ownership.
    DriverRejected,
    /// Transport failed after driver ownership.
    Transport,
    /// Kafka rejected the named group with this exact signed code.
    Broker(NonZeroI16),
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected broker version cannot represent required semantics.
    Compatibility,
    /// A broker response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListConsumerGroupOffsetsFailure {
    kind: ListConsumerGroupOffsetsFailureKind,
    delivery: DeliveryStatus,
}

impl ListConsumerGroupOffsetsFailure {
    pub(crate) const fn new(
        kind: ListConsumerGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the core-owned failure category.
    pub const fn kind(self) -> ListConsumerGroupOffsetsFailureKind {
        self.kind
    }

    /// Returns transport delivery certainty without inventing retry policy.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for a consumer-group offset query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListConsumerGroupOffsetsTerminal {
    /// Ordered topic-partition offset outcomes and broker throttle.
    Offsets(ListConsumerGroupOffsetsBatch),
    /// Every requested group settled in original caller order.
    Batch(ListConsumerGroupsOffsetsBatch),
    /// Whole-operation failure outside partition results.
    Failed(ListConsumerGroupOffsetsFailure),
}
