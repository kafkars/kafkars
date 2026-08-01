//! Engine-owned producer delivery values translated from closed core policy types.

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, ProducerFailure as CoreProducerFailure,
    ProducerFailureKind as CoreProducerFailureKind, RecordMetadata as CoreRecordMetadata,
};

/// Metadata Kafka acknowledged for one produced record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerRecordMetadata {
    partition: u32,
    offset: i64,
    append_timestamp: Option<i64>,
    leader_epoch: Option<i32>,
}

impl ProducerRecordMetadata {
    pub(crate) const fn from_core(metadata: CoreRecordMetadata) -> Self {
        Self {
            partition: metadata.partition().get(),
            offset: metadata.offset(),
            append_timestamp: metadata.append_timestamp(),
            leader_epoch: metadata.leader_epoch(),
        }
    }

    /// Returns the acknowledged zero-based partition.
    pub const fn partition(self) -> u32 {
        self.partition
    }

    /// Returns the record's absolute Kafka offset.
    pub const fn offset(self) -> i64 {
        self.offset
    }

    /// Returns Kafka's append timestamp when the broker supplied one.
    pub const fn append_timestamp(self) -> Option<i64> {
        self.append_timestamp
    }

    /// Returns the acknowledged leader epoch when the broker supplied one.
    pub const fn leader_epoch(self) -> Option<i32> {
        self.leader_epoch
    }
}

/// Certainty retained after a producer operation fails.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerDeliveryStatus {
    /// The operation definitely did not cross transport ownership.
    NotSent,
    /// Kafka may have accepted or persisted the operation.
    PossiblySent,
}

impl ProducerDeliveryStatus {
    const fn from_core(status: CoreDeliveryStatus) -> Self {
        match status {
            CoreDeliveryStatus::NotSent => Self::NotSent,
            CoreDeliveryStatus::PossiblySent => Self::PossiblySent,
        }
    }
}

/// Semantic reason a terminal producer operation failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerDeliveryFailureKind {
    /// The caller cancelled the operation before driver ownership.
    Cancelled,
    /// The driver rejected the request before taking transport ownership.
    DriverRejected,
    /// Record-batch materialization failed before driver ownership.
    MaterializationFailed,
    /// Routing metadata or leadership changed.
    Routing,
    /// Kafka reported a retryable failure without a routing change.
    BrokerRetriable,
    /// Authentication or authorization rejected the operation.
    AccessRejected,
    /// Kafka rejected record or batch content.
    InvalidRecord,
    /// The request or record format is incompatible with the broker.
    Compatibility,
    /// Producer identity or transaction fencing is terminal.
    ProducerFenced,
    /// Idempotent identity or sequence state requires recovery.
    ProducerIdentity,
    /// The transport failed while the request was active.
    Transport,
    /// Kafka returned a response that could not be correlated as valid success.
    InvalidResponse,
    /// The engine execution owner stopped before delivery settled.
    ExecutionUnavailable,
    /// The absolute delivery deadline elapsed before driver ownership.
    DeadlineElapsed,
    /// A future broker error is not classified by this engine version.
    UnknownBroker,
}

impl ProducerDeliveryFailureKind {
    const fn from_core(kind: CoreProducerFailureKind) -> Self {
        match kind {
            CoreProducerFailureKind::Cancelled => Self::Cancelled,
            CoreProducerFailureKind::DriverRejected => Self::DriverRejected,
            CoreProducerFailureKind::MaterializationFailed => Self::MaterializationFailed,
            CoreProducerFailureKind::Routing => Self::Routing,
            CoreProducerFailureKind::BrokerRetriable => Self::BrokerRetriable,
            CoreProducerFailureKind::AccessRejected => Self::AccessRejected,
            CoreProducerFailureKind::InvalidRecord => Self::InvalidRecord,
            CoreProducerFailureKind::Compatibility => Self::Compatibility,
            CoreProducerFailureKind::ProducerFenced => Self::ProducerFenced,
            CoreProducerFailureKind::ProducerIdentity => Self::ProducerIdentity,
            CoreProducerFailureKind::Transport => Self::Transport,
            CoreProducerFailureKind::InvalidResponse => Self::InvalidResponse,
            CoreProducerFailureKind::ExecutionUnavailable => Self::ExecutionUnavailable,
            CoreProducerFailureKind::DeadlineElapsed => Self::DeadlineElapsed,
            CoreProducerFailureKind::UnknownBroker => Self::UnknownBroker,
        }
    }
}

/// Terminal producer failure with exact delivery certainty and broker code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerDeliveryFailure {
    kind: ProducerDeliveryFailureKind,
    status: ProducerDeliveryStatus,
    broker_code: Option<i16>,
}

impl ProducerDeliveryFailure {
    pub(crate) const fn from_core(failure: CoreProducerFailure) -> Self {
        Self {
            kind: ProducerDeliveryFailureKind::from_core(failure.kind()),
            status: ProducerDeliveryStatus::from_core(failure.delivery()),
            broker_code: failure.broker_code(),
        }
    }

    /// Returns the semantic failure classification.
    pub const fn kind(self) -> ProducerDeliveryFailureKind {
        self.kind
    }

    /// Returns whether transport ownership may have been crossed.
    pub const fn delivery_status(self) -> ProducerDeliveryStatus {
        self.status
    }

    /// Returns Kafka's exact error code when the failure came from a broker.
    pub const fn broker_code(self) -> Option<i16> {
        self.broker_code
    }
}
