//! Exact terminal values for transactional-producer initialization.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::TransactionalProducerIdentity;

/// Semantic category supplied by the future protocol adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionInitializationBrokerCategory {
    /// Kafka fenced the transactional identity owner.
    Fenced,
    /// Kafka rejected initialization without declaring the owner fenced.
    Rejected,
}

/// Exact broker rejection without retry or coordinator policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionInitializationBrokerFailure {
    code: NonZeroI16,
    category: TransactionInitializationBrokerCategory,
}

impl TransactionInitializationBrokerFailure {
    /// Retains one exact signed Kafka code and normalized fencing category.
    pub const fn new(code: NonZeroI16, category: TransactionInitializationBrokerCategory) -> Self {
        Self { code, category }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }

    /// Returns whether Kafka declared the transactional owner fenced.
    pub const fn category(self) -> TransactionInitializationBrokerCategory {
        self.category
    }
}

/// Whole-operation failure category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionInitializationFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected the request before transport ownership.
    DriverRejected,
    /// Transport failed after driver ownership.
    Transport,
    /// Kafka rejected initialization with exact diagnostic facts.
    Broker(TransactionInitializationBrokerFailure),
    /// A broker success response contained an invalid identity.
    InvalidResponse,
}

/// Initialization failure with authoritative delivery certainty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionInitializationFailure {
    kind: TransactionInitializationFailureKind,
    delivery: DeliveryStatus,
}

impl TransactionInitializationFailure {
    pub(super) const fn new(
        kind: TransactionInitializationFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the deterministic failure category.
    pub const fn kind(self) -> TransactionInitializationFailureKind {
        self.kind
    }

    /// Returns transport-authoritative delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal initialization decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionInitializationTerminal {
    /// Kafka issued a valid identity for the fenced transactional owner.
    Initialized(TransactionalProducerIdentity),
    /// Initialization failed without inventing retry policy.
    Failed(TransactionInitializationFailure),
}
