//! Normalized facts accepted by transaction-initialization policy.

use crate::{DeliveryStatus, Moment};

use super::TransactionInitializationBrokerFailure;

/// One external fact applied to a fenced initialization owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionInitializationInput {
    /// Starts the accepted operation at the supplied monotonic observation.
    Start {
        /// Current monotonic observation.
        now: Moment,
    },
    /// Reports that the driver accepted the only request.
    DriverAccepted,
    /// Reports definite rejection before driver ownership.
    DriverRejected,
    /// Reports original-deadline expiry before driver ownership.
    DeadlineElapsed,
    /// Reports original-deadline expiry after driver ownership.
    DriverDeadlineElapsed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports Kafka's successful producer identity fields.
    BrokerInitialized {
        /// Kafka's signed producer ID field.
        producer_id: i64,
        /// Kafka's signed producer epoch field.
        producer_epoch: i16,
    },
    /// Authorizes one replacement after an exact rejection and causal route refresh.
    RetryableBrokerRejected,
    /// Reports one exact normalized Kafka broker rejection.
    BrokerRejected {
        /// Exact signed code plus its fencing category.
        failure: TransactionInitializationBrokerFailure,
    },
    /// Reports a driver-owned transport terminal.
    TransportFailed {
        /// Driver-authoritative delivery certainty.
        delivery: DeliveryStatus,
    },
    /// Reports a broker response that cannot represent a valid identity.
    InvalidResponse,
}
