//! Exact protocol-neutral failure facts for one accepted transaction end.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::TransactionEndMode;

/// Broker-owned category for one exact nonzero `EndTxn` rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionEndBrokerFailureKind {
    /// Authentication or authorization rejected the transactional identity.
    Access,
    /// The transaction coordinator could not serve the request.
    Coordinator,
    /// Kafka explicitly fenced the transactional producer identity.
    Fenced,
    /// Kafka returned another exact signed rejection.
    Rejected,
}

/// Stable cause of one failed accepted transaction end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionEndFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected the request before transport ownership.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// The negotiated request version was incompatible with Kafka.
    Compatibility,
    /// Kafka returned a malformed or uncorrelatable response.
    InvalidResponse,
    /// The driver or its completion owner closed.
    DriverClosed,
    /// Internal request or completion correlation failed.
    Correlation,
    /// Kafka returned one exact signed broker rejection.
    Broker(TransactionEndBrokerFailureKind),
}

/// Exact cause, intent, delivery certainty, and signed broker code of a failed end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionEndFailure {
    mode: TransactionEndMode,
    kind: TransactionEndFailureKind,
    delivery: DeliveryStatus,
    broker_code: Option<NonZeroI16>,
}

impl TransactionEndFailure {
    /// Creates a non-broker failure without manufacturing a broker code.
    pub const fn local(
        mode: TransactionEndMode,
        kind: TransactionEndFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        debug_assert!(!matches!(kind, TransactionEndFailureKind::Broker(_)));
        Self {
            mode,
            kind,
            delivery,
            broker_code: None,
        }
    }

    /// Creates one broker failure with its exact nonzero signed code.
    pub const fn broker(
        mode: TransactionEndMode,
        kind: TransactionEndBrokerFailureKind,
        delivery: DeliveryStatus,
        broker_code: NonZeroI16,
    ) -> Self {
        Self {
            mode,
            kind: TransactionEndFailureKind::Broker(kind),
            delivery,
            broker_code: Some(broker_code),
        }
    }

    /// Returns whether commit or abort was requested.
    pub const fn mode(self) -> TransactionEndMode {
        self.mode
    }

    /// Returns the stable protocol-neutral cause.
    pub const fn kind(self) -> TransactionEndFailureKind {
        self.kind
    }

    /// Returns authoritative transport certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }

    /// Applies retained delivery evidence without ever strengthening certainty.
    pub const fn with_delivery_floor(self, floor: DeliveryStatus) -> Self {
        let delivery = if matches!(self.delivery, DeliveryStatus::PossiblySent)
            || matches!(floor, DeliveryStatus::PossiblySent)
        {
            DeliveryStatus::PossiblySent
        } else {
            DeliveryStatus::NotSent
        };
        Self { delivery, ..self }
    }

    /// Returns Kafka's exact signed broker code when the cause came from Kafka.
    pub const fn broker_code(self) -> Option<i16> {
        match self.broker_code {
            Some(code) => Some(code.get()),
            None => None,
        }
    }
}
