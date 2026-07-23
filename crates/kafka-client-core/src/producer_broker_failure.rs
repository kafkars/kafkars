//! Semantic broker facts received after engine-owned Produce normalization.

use core::num::NonZeroI16;

/// Broker-declared Produce failure category consumed by deterministic policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerBrokerFailureKind {
    /// Topic identity, partition leadership, or routing metadata changed.
    Routing,
    /// Kafka asked the producer to retry later without a routing change.
    Retriable,
    /// Authentication or authorization permanently rejected the operation.
    AccessRejected,
    /// Kafka rejected record or batch content.
    InvalidRecord,
    /// The negotiated request or record format is incompatible with the broker.
    Compatibility,
    /// Idempotent identity or sequence state requires core recovery policy.
    ProducerIdentity,
    /// Producer identity or transaction fencing is terminal.
    ProducerFenced,
    /// The engine does not recognize this signed broker code.
    Unknown,
}

/// Engine-normalized broker fact with its exact signed diagnostic code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerBrokerFailure {
    kind: ProducerBrokerFailureKind,
    code: NonZeroI16,
}

impl ProducerBrokerFailure {
    /// Creates a semantic fact after protocol normalization.
    pub const fn new(kind: ProducerBrokerFailureKind, code: NonZeroI16) -> Self {
        Self { kind, code }
    }

    /// Returns the broker-declared semantic category.
    pub const fn kind(self) -> ProducerBrokerFailureKind {
        self.kind
    }

    /// Returns Kafka's exact non-success signed error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }
}
