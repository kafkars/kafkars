//! Isolated names for the engine-owned partition-transaction abort contract.

pub(super) use kafka_client_engine::{
    AbortPartitionTransactionAccepted as Accepted,
    AbortPartitionTransactionAcceptedFaultKind as AcceptedFaultKind,
    AbortPartitionTransactionAdmissionError as AdmissionError,
    AbortPartitionTransactionAdmissionErrorKind as AdmissionErrorKind,
    AbortPartitionTransactionBrokerError as BrokerError,
    AbortPartitionTransactionDeliveryStatus as DeliveryStatus,
    AbortPartitionTransactionFailure as Failure,
    AbortPartitionTransactionFailureKind as FailureKind,
    AbortPartitionTransactionObserver as Observer,
    AbortPartitionTransactionObserverError as ObserverError,
    AbortPartitionTransactionOutcome as Outcome,
};
