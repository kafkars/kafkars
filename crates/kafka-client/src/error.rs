//! Stable semantic errors exposed by the curated facade.

use core::fmt;

use kafka_client_core::DeliveryStatus;

/// Stable top-level category for a client failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Local configuration is incomplete or contradictory.
    Configuration,
    /// A bounded local resource rejected admission.
    Backpressure,
    /// The operation's absolute deadline elapsed.
    Timeout,
    /// Explicit cancellation completed before transport ownership.
    Cancelled,
    /// The requested operation conflicts with the handle lifecycle.
    State,
    /// The implementation violated an internal contract.
    Internal,
}

/// Extensible client error shared by producer, consumer, admin, and transactions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaError {
    kind: ErrorKind,
    message: String,
    delivery_status: Option<DeliveryStatus>,
}

impl KafkaError {
    /// Creates a semantic client error.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            delivery_status: None,
        }
    }

    /// Attaches producer delivery certainty.
    pub fn with_delivery_status(mut self, status: DeliveryStatus) -> Self {
        self.delivery_status = Some(status);
        self
    }

    /// Returns the stable error category.
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns producer delivery certainty when relevant.
    pub const fn delivery_status(&self) -> Option<DeliveryStatus> {
        self.delivery_status
    }
}

impl fmt::Display for KafkaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for KafkaError {}
