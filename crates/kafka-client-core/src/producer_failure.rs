//! Core-owned Produce failure policy and diagnostic preservation.

use crate::{DeliveryStatus, ProducerBrokerFailure, ProducerBrokerFailureKind};

/// Normalized failure reason interpreted by producer policy without wire types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerFailureKind {
    /// The caller cancelled the operation before driver ownership.
    Cancelled,
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
    /// Kafka returned a response that could not be correlated as valid success.
    InvalidResponse,
    /// Production execution stopped permanently before the operation settled.
    ExecutionUnavailable,
    /// The public absolute deadline elapsed before driver ownership.
    DeadlineElapsed,
    /// A broker error was not recognized by the engine protocol normalizer.
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

    pub(crate) const fn broker(failure: ProducerBrokerFailure, delivery: DeliveryStatus) -> Self {
        Self {
            kind: broker_failure_kind(failure.kind()),
            delivery,
            broker_code: Some(failure.code()),
        }
    }

    pub(crate) const fn driver_rejected() -> Self {
        Self::new(ProducerFailureKind::DriverRejected, DeliveryStatus::NotSent)
    }

    pub(crate) const fn cancelled() -> Self {
        Self::new(ProducerFailureKind::Cancelled, DeliveryStatus::NotSent)
    }

    /// Creates a terminal for a caller cancelled before active admission.
    pub const fn waiting_cancelled() -> Self {
        Self::new(ProducerFailureKind::Cancelled, DeliveryStatus::NotSent)
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

    pub(crate) const fn attempt(
        failure: crate::ProducerAttemptFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        match failure {
            crate::ProducerAttemptFailureKind::Compatibility => {
                Self::new(ProducerFailureKind::Compatibility, delivery)
            }
            crate::ProducerAttemptFailureKind::InvalidResponse => {
                Self::new(ProducerFailureKind::InvalidResponse, delivery)
            }
            crate::ProducerAttemptFailureKind::LocalCapacity
            | crate::ProducerAttemptFailureKind::RouteUnavailable
            | crate::ProducerAttemptFailureKind::NameResolutionUnavailable
            | crate::ProducerAttemptFailureKind::ConnectionUnavailable
            | crate::ProducerAttemptFailureKind::Permanent => Self::transport(delivery),
        }
    }

    pub(crate) const fn producer_identity(broker_code: Option<core::num::NonZeroI16>) -> Self {
        Self {
            kind: ProducerFailureKind::ProducerIdentity,
            delivery: DeliveryStatus::NotSent,
            broker_code: match broker_code {
                Some(code) => Some(code.get()),
                None => None,
            },
        }
    }

    /// Creates a permanent execution-loss failure with conservative certainty.
    ///
    /// Production mechanisms use this only as terminal fallback when the
    /// deterministic execution-loss transition itself cannot be interpreted.
    pub const fn execution_unavailable(delivery: DeliveryStatus) -> Self {
        Self::new(ProducerFailureKind::ExecutionUnavailable, delivery)
    }

    pub(crate) const fn deadline_elapsed() -> Self {
        Self::new(
            ProducerFailureKind::DeadlineElapsed,
            DeliveryStatus::NotSent,
        )
    }

    pub(crate) const fn with_delivery(self, delivery: DeliveryStatus) -> Self {
        Self { delivery, ..self }
    }

    /// Creates a terminal for a public deadline elapsed before active admission.
    pub const fn waiting_deadline_elapsed() -> Self {
        Self::new(
            ProducerFailureKind::DeadlineElapsed,
            DeliveryStatus::NotSent,
        )
    }

    /// Creates a definitely-unsent terminal when partition metadata cannot
    /// provide a usable route before active admission.
    pub const fn metadata_unavailable(broker_code: Option<i16>) -> Self {
        Self {
            kind: ProducerFailureKind::Routing,
            delivery: DeliveryStatus::NotSent,
            broker_code,
        }
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

const fn broker_failure_kind(kind: ProducerBrokerFailureKind) -> ProducerFailureKind {
    match kind {
        ProducerBrokerFailureKind::Routing => ProducerFailureKind::Routing,
        ProducerBrokerFailureKind::Retriable => ProducerFailureKind::BrokerRetriable,
        ProducerBrokerFailureKind::AccessRejected => ProducerFailureKind::AccessRejected,
        ProducerBrokerFailureKind::InvalidRecord => ProducerFailureKind::InvalidRecord,
        ProducerBrokerFailureKind::Compatibility => ProducerFailureKind::Compatibility,
        ProducerBrokerFailureKind::ProducerIdentity => ProducerFailureKind::ProducerIdentity,
        ProducerBrokerFailureKind::ProducerFenced => ProducerFailureKind::ProducerFenced,
        ProducerBrokerFailureKind::Unknown => ProducerFailureKind::UnknownBroker,
    }
}
