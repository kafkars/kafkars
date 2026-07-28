//! Lossless broker-error facts shared by transactional protocol adapters.

use core::num::NonZeroI16;

const INVALID_PRODUCER_EPOCH: i16 = 47;
const PRODUCER_FENCED: i16 = 90;

/// Whether one exact broker rejection fences the transactional producer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionBrokerCategory {
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
        _ => TransactionBrokerCategory::Rejected,
    };
    Some(TransactionBrokerError { code, category })
}
