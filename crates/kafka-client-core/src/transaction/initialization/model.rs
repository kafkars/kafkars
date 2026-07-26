//! Scalar owner, request, and broker-issued identity values.

use core::fmt;

/// Maximum transaction timeout representable by Kafka's signed millisecond field.
pub(super) const MAX_TRANSACTION_TIMEOUT_MS: u32 = i32::MAX as u32;

/// Opaque engine identity for one retained transactional-id owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransactionalOwnerId(u64);

impl TransactionalOwnerId {
    /// Creates an owner fence from an engine-assigned scalar.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the engine-assigned scalar.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Validated scalar intent for one `InitProducerId` request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionInitializationPlan {
    transaction_timeout_ms: u32,
}

impl TransactionInitializationPlan {
    /// Validates a positive timeout representable by Kafka.
    pub const fn new(
        transaction_timeout_ms: u32,
    ) -> Result<Self, TransactionInitializationPlanError> {
        if transaction_timeout_ms == 0 {
            return Err(TransactionInitializationPlanError::ZeroTimeout);
        }
        if transaction_timeout_ms > MAX_TRANSACTION_TIMEOUT_MS {
            return Err(TransactionInitializationPlanError::TimeoutTooLarge);
        }
        Ok(Self {
            transaction_timeout_ms,
        })
    }

    /// Returns the exact broker-facing transaction timeout.
    pub const fn transaction_timeout_ms(self) -> u32 {
        self.transaction_timeout_ms
    }
}

/// Invalid transaction-initialization intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionInitializationPlanError {
    /// Kafka transactions require a positive timeout.
    ZeroTimeout,
    /// Kafka's signed millisecond field cannot represent the timeout.
    TimeoutTooLarge,
}

impl fmt::Display for TransactionInitializationPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ZeroTimeout => "transaction timeout must be positive",
            Self::TimeoutTooLarge => "transaction timeout exceeds Kafka's signed field",
        })
    }
}

impl std::error::Error for TransactionInitializationPlanError {}

/// Broker-issued identity reserved for one transactional producer owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionalProducerIdentity {
    producer_id: i64,
    producer_epoch: i16,
}

impl TransactionalProducerIdentity {
    /// Validates Kafka's nonnegative transactional producer identity.
    pub const fn try_new(producer_id: i64, producer_epoch: i16) -> Option<Self> {
        if producer_id < 0 || producer_epoch < 0 {
            None
        } else {
            Some(Self {
                producer_id,
                producer_epoch,
            })
        }
    }

    /// Returns Kafka's broker-issued producer ID.
    pub const fn producer_id(self) -> i64 {
        self.producer_id
    }

    /// Returns Kafka's broker-issued producer epoch.
    pub const fn producer_epoch(self) -> i16 {
        self.producer_epoch
    }
}
