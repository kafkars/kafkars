//! Core-owned Produce failure classification and diagnostic preservation.

use crate::DeliveryStatus;

/// Normalized failure reason interpreted by producer policy without wire types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerFailureKind {
    /// Local submission failed before the driver accepted ownership.
    DriverRejected,
    /// Record-batch materialization failed before driver ownership.
    MaterializationFailed,
    /// Routing metadata or leadership changed.
    Routing,
    /// Kafka asked the client to retry later without a routing change.
    BrokerRetriable,
    /// Authentication or authorization permanently rejected the operation.
    AccessRejected,
    /// Kafka rejected record or batch content.
    InvalidRecord,
    /// The negotiated request or record format is incompatible with the broker.
    Compatibility,
    /// Producer identity or transaction fencing is terminal.
    ProducerFenced,
    /// Idempotent identity or sequence state requires core recovery policy.
    ProducerIdentity,
    /// The transport failed while the request was active.
    Transport,
    /// The public absolute deadline elapsed before driver ownership.
    DeadlineElapsed,
    /// A future broker error has not yet been classified by this core version.
    UnknownBroker,
}

/// Semantic producer failure with monotonic delivery certainty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerFailure {
    kind: ProducerFailureKind,
    delivery: DeliveryStatus,
    broker_code: Option<i16>,
}

impl ProducerFailure {
    const fn new(kind: ProducerFailureKind, delivery: DeliveryStatus) -> Self {
        Self {
            kind,
            delivery,
            broker_code: None,
        }
    }

    pub(crate) const fn broker(broker_code: i16, delivery: DeliveryStatus) -> Self {
        Self {
            kind: classify_broker_error(broker_code),
            delivery,
            broker_code: Some(broker_code),
        }
    }

    pub(crate) const fn driver_rejected() -> Self {
        Self::new(ProducerFailureKind::DriverRejected, DeliveryStatus::NotSent)
    }

    pub(crate) const fn materialization_failed() -> Self {
        Self::new(
            ProducerFailureKind::MaterializationFailed,
            DeliveryStatus::NotSent,
        )
    }

    pub(crate) const fn transport(delivery: DeliveryStatus) -> Self {
        Self::new(ProducerFailureKind::Transport, delivery)
    }

    pub(crate) const fn deadline_elapsed() -> Self {
        Self::new(
            ProducerFailureKind::DeadlineElapsed,
            DeliveryStatus::NotSent,
        )
    }

    /// Returns the core-owned semantic classification.
    pub const fn kind(self) -> ProducerFailureKind {
        self.kind
    }

    /// Returns delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }

    /// Returns Kafka's exact error code when the failure came from a broker.
    pub const fn broker_code(self) -> Option<i16> {
        self.broker_code
    }
}

const fn classify_broker_error(code: i16) -> ProducerFailureKind {
    match code {
        3 | 5 | 6 | 74 | 75 | 100 | 103 => ProducerFailureKind::Routing,
        2 | 7 | 13 | 19 | 20 | 56 | 89 => ProducerFailureKind::BrokerRetriable,
        29 | 58 => ProducerFailureKind::AccessRejected,
        10 | 17 | 18 | 21 | 32 | 42 | 44 | 87 => ProducerFailureKind::InvalidRecord,
        35 | 43 | 76 => ProducerFailureKind::Compatibility,
        45 | 46 | 47 | 59 => ProducerFailureKind::ProducerIdentity,
        90 => ProducerFailureKind::ProducerFenced,
        _ => ProducerFailureKind::UnknownBroker,
    }
}
