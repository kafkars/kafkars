//! Generated-free public terminal facts for one accepted `ShareAcknowledge` call.

use bytes::Bytes;

use crate::{
    consumer::ShareAcknowledgement,
    protocol::consumer::share_acknowledge::{
        ShareAcknowledgePartitionOutcome as ProtocolOutcome, ShareAcknowledgeSuccess,
    },
};

/// Exact terminal result of one accepted share acknowledgement.
#[derive(Debug)]
pub enum ShareAcknowledgeOutcome {
    /// Kafka accepted the request and returned every correlated partition outcome.
    Responded(ShareAcknowledgeResponse),
    /// Execution failed, retaining retry ownership only when definitely unsent.
    Failed(ShareAcknowledgeFailure),
}

/// Generated-free response to one accepted share acknowledgement.
#[derive(Debug)]
pub struct ShareAcknowledgeResponse(pub(super) ShareAcknowledgeSuccess);

impl ShareAcknowledgeResponse {
    /// Returns Kafka's nonnegative response throttle in milliseconds.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.0.throttle_time_ms
    }

    /// Returns each request-correlated partition outcome in canonical order.
    pub fn partitions(
        &self,
    ) -> impl ExactSizeIterator<Item = ShareAcknowledgePartitionOutcome<'_>> {
        self.0.outcomes.iter().map(ShareAcknowledgePartitionOutcome)
    }
}

/// Borrowed result for one acknowledged topic partition.
#[derive(Clone, Copy, Debug)]
pub struct ShareAcknowledgePartitionOutcome<'response>(pub(super) &'response ProtocolOutcome);

impl<'response> ShareAcknowledgePartitionOutcome<'response> {
    /// Returns the exact Kafka topic UUID bytes.
    pub const fn topic_id(self) -> [u8; 16] {
        self.0.topic_id
    }

    /// Returns the zero-based partition index.
    pub const fn partition(self) -> u32 {
        self.0.partition
    }

    /// Returns the exact nonzero Kafka partition error code, if any.
    pub const fn broker_code(self) -> Option<i16> {
        match self.0.error_code {
            Some(code) => Some(code.get()),
            None => None,
        }
    }

    /// Returns Kafka's bounded diagnostic bytes without UTF-8 coercion.
    pub fn error_message(self) -> Option<&'response [u8]> {
        self.0.error_message.as_deref()
    }

    /// Returns Kafka's current leader id and epoch when provided.
    pub const fn current_leader(self) -> Option<(i32, i32)> {
        self.0.current_leader
    }
}

/// Exact broker-level top-level rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShareAcknowledgeBrokerError {
    pub(super) throttle_time_ms: u32,
    pub(super) broker_code: i16,
    pub(super) message: Option<Bytes>,
}

impl ShareAcknowledgeBrokerError {
    /// Returns Kafka's nonnegative response throttle in milliseconds.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns the exact nonzero Kafka error code.
    pub const fn broker_code(&self) -> i16 {
        self.broker_code
    }

    /// Returns Kafka's bounded diagnostic bytes without UTF-8 coercion.
    pub fn message(&self) -> Option<&[u8]> {
        self.message.as_deref()
    }
}

/// Certainty retained after an acknowledgement fails.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareAcknowledgeDeliveryStatus {
    /// The request definitely did not cross transport ownership.
    NotSent,
    /// Kafka may have applied some or all acknowledgements.
    PossiblySent,
}

/// Stable terminal failure category for an accepted acknowledgement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareAcknowledgeFailureKind {
    /// The original public deadline elapsed.
    DeadlineElapsed,
    /// Kafka does not expose a compatible request version.
    Compatibility,
    /// Driver capacity or request admission rejected execution.
    DriverRejected,
    /// Transport or routing ended terminally.
    Transport,
    /// Kafka's response violated bounded protocol shape.
    InvalidResponse,
    /// Response materialization exceeded a configured byte bound.
    ResponseTooLarge,
    /// The engine completion or ownership state became inconsistent.
    Internal,
    /// Kafka returned one nonzero top-level broker code.
    BrokerRejected,
}

/// Terminal failure retaining exact certainty and safe retry ownership.
#[derive(Debug)]
pub struct ShareAcknowledgeFailure {
    pub(super) kind: ShareAcknowledgeFailureKind,
    pub(super) delivery: ShareAcknowledgeDeliveryStatus,
    pub(super) broker: Option<ShareAcknowledgeBrokerError>,
    pub(super) retry: Option<ShareAcknowledgement>,
}

impl ShareAcknowledgeFailure {
    /// Returns the stable terminal failure category.
    pub const fn kind(&self) -> ShareAcknowledgeFailureKind {
        self.kind
    }

    /// Returns whether transport ownership may have been crossed.
    pub const fn delivery_status(&self) -> ShareAcknowledgeDeliveryStatus {
        self.delivery
    }

    /// Returns the exact top-level broker rejection when available.
    pub const fn broker(&self) -> Option<&ShareAcknowledgeBrokerError> {
        self.broker.as_ref()
    }

    /// Recovers the exact definitely-unsent acknowledgement, when safe.
    pub fn into_retry(self) -> Option<ShareAcknowledgement> {
        self.retry
    }
}
