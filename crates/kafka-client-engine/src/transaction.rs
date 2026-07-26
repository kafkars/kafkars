//! Declarative facade for private transactional execution ownership.

mod initialization;

#[cfg(test)]
pub(crate) use initialization::TransactionInitializationAdmissionErrorKind;
pub(crate) use initialization::{
    TransactionInitializationAccepted, TransactionInitializationAdmissionError,
    TransactionInitializationAdmissionPort, TransactionInitializationHost,
    TransactionInitializationHostError, TransactionInitializationRequest,
    TransactionInitializationShardLockError, TransactionInitializationShardOwner,
    TransactionInitializationTurn,
};
