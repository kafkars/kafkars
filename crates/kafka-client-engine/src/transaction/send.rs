//! Declarative boundary for one fixed-slot transactional Produce owner.

#[cfg(test)]
mod admission_test;
mod aggregate;
mod automatic;
#[cfg(test)]
mod automatic_test;
mod completion;
#[cfg(test)]
mod failure_test;
mod input;
mod model;
mod owner;
mod partitioning;
mod port;
mod produce_settlement;
mod recovery;
mod scheduling;
mod settlement;
mod terminal;
#[cfg(test)]
mod test_support;
mod turn;
#[cfg(test)]
mod turn_retry_fencing_test;
#[cfg(test)]
mod turn_retry_test;
#[cfg(test)]
mod turn_test;

pub(crate) use input::{
    TransactionSendAdmissionFailureKind, TransactionSendInput, TransactionSendRequest,
};
#[cfg(test)]
pub(super) use model::TransactionSendFailureKind;
pub(crate) use model::{
    TransactionSendAccepted, TransactionSendFailure as InternalTransactionSendFailure,
    TransactionSendFailureKind as InternalTransactionSendFailureKind, TransactionSendTerminal,
    TransactionSendTurn,
};
pub(crate) use owner::TransactionSendOwner;
pub(crate) use partitioning::TransactionPartitioningFailure as InternalTransactionPartitioningFailure;
#[cfg(test)]
pub(in crate::transaction) use port::{
    TransactionSendProduceCall, TransactionSendProduceEvidence, TransactionSendProducePort,
    TransactionSendProduceRequest, TransactionSendProduceSubmissionFailure,
};
