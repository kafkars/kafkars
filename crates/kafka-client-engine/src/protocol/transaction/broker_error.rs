//! Lossless broker-error facts shared by transactional protocol adapters.

use core::num::NonZeroI16;

const INVALID_PRODUCER_EPOCH: i16 = 47;
const PRODUCER_FENCED: i16 = 90;
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const NOT_COORDINATOR: i16 = 16;
const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;
const SASL_AUTHENTICATION_FAILED: i16 = 58;

/// Whether one exact broker rejection fences the transactional producer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionBrokerCategory {
    Access,
    Coordinator,
    Fenced,
    Rejected,
}

/// One signed broker error with only wire-local fencing classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TransactionBrokerError {
    code: NonZeroI16,
    category: TransactionBrokerCategory,
}

impl TransactionBrokerError {
    pub(crate) const fn code(self) -> NonZeroI16 {
        self.code
    }

    pub(crate) const fn category(self) -> TransactionBrokerCategory {
        self.category
    }
}

/// Preserves one nonzero signed code without adding retry or fatal policy.
pub(super) const fn transaction_broker_error(code: i16) -> Option<TransactionBrokerError> {
    let Some(code) = NonZeroI16::new(code) else {
        return None;
    };
    let category = match code.get() {
        INVALID_PRODUCER_EPOCH | PRODUCER_FENCED => TransactionBrokerCategory::Fenced,
        COORDINATOR_LOAD_IN_PROGRESS | COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR => {
            TransactionBrokerCategory::Coordinator
        }
        CLUSTER_AUTHORIZATION_FAILED
        | TRANSACTIONAL_ID_AUTHORIZATION_FAILED
        | SASL_AUTHENTICATION_FAILED => TransactionBrokerCategory::Access,
        _ => TransactionBrokerCategory::Rejected,
    };
    Some(TransactionBrokerError { code, category })
}
