//! Declarative boundary for generated transactional protocol messages.

mod broker_error;
#[cfg(test)]
mod broker_error_test;
mod end_txn;
#[cfg(test)]
mod end_txn_test;
mod init_producer_id;

#[cfg(test)]
pub(crate) use broker_error::TransactionBrokerCategory;
pub(crate) use broker_error::TransactionBrokerError;
pub(crate) use end_txn::{
    EndTxnDisposition, EndTxnOutcome, EndTxnResponseFailure, end_txn_v3_request,
    normalize_end_txn_v3_response,
};
pub(crate) use init_producer_id::{
    TransactionInitBrokerCategory, TransactionInitResponseFailure,
    normalize_transaction_init_response, transaction_init_request,
};

#[cfg(test)]
mod init_producer_id_test;
