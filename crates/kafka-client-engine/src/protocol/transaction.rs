//! Declarative boundary for generated transactional protocol messages.

mod add_partitions_to_txn_request;
mod add_partitions_to_txn_response;
mod broker_error;
#[cfg(test)]
mod broker_error_test;
mod end_txn;
#[cfg(test)]
mod end_txn_test;
mod init_producer_id;

pub(crate) use add_partitions_to_txn_request::{
    AddPartitionsToTxnRequestFailure, TransactionPartitionRef, add_partitions_to_txn_v3_request,
};
pub(crate) use add_partitions_to_txn_response::{
    AddPartitionsToTxnPartitionOutcome, AddPartitionsToTxnPartitionResultRef,
    AddPartitionsToTxnResponseFailure, ValidatedAddPartitionsToTxnResponse,
    normalize_add_partitions_to_txn_v3_response,
};
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
mod add_partitions_to_txn_request_test;
#[cfg(test)]
mod add_partitions_to_txn_response_test;
#[cfg(test)]
mod init_producer_id_test;
