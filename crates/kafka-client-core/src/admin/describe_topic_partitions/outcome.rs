//! Exactly one explicit page terminal or delivery-aware whole-operation failure.

use crate::DeliveryStatus;

use super::DescribeTopicPartitionsPage;

/// Stable alias used by adapters without owning another delivery vocabulary.
pub type DescribeTopicPartitionsDeliveryStatus = DeliveryStatus;

/// Whole-operation failure outside topic- and partition-scoped broker facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeTopicPartitionsFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the sole request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// Valid response facts exceeded the bounded page envelope.
    ResponseTooLarge,
    /// The selected broker version cannot represent API-key 75 semantics.
    Compatibility,
    /// The response was malformed or could not be correlated to the request.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeTopicPartitionsFailure {
    kind: DescribeTopicPartitionsFailureKind,
    delivery: DeliveryStatus,
}

impl DescribeTopicPartitionsFailure {
    pub(crate) const fn new(
        kind: DescribeTopicPartitionsFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable mechanism or structural category.
    pub const fn kind(self) -> DescribeTopicPartitionsFailureKind {
        self.kind
    }

    /// Returns driver-authoritative delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for one explicit page request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeTopicPartitionsTerminal {
    /// Kafka returned one bounded subset/page and optional next cursor.
    Page(DescribeTopicPartitionsPage),
    /// A whole-operation failure prevented a usable page.
    Failed(DescribeTopicPartitionsFailure),
}
