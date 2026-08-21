//! Isolated names for the engine-owned Admin `DescribeTransactions` contract.

pub(super) use kafka_client_engine::{
    AdminDescribeTransactionDescription as Description,
    AdminDescribeTransactionEngineBrokerError as BrokerError,
    AdminDescribeTransactionTopic as Topic, AdminDescribeTransactionsAccepted as Accepted,
    AdminDescribeTransactionsAcceptedFaultKind as AcceptedFaultKind,
    AdminDescribeTransactionsAdmissionError as AdmissionError,
    AdminDescribeTransactionsAdmissionErrorKind as AdmissionErrorKind,
    AdminDescribeTransactionsDeliveryStatus as DeliveryStatus,
    AdminDescribeTransactionsFailure as Failure,
    AdminDescribeTransactionsFailureKind as FailureKind,
    AdminDescribeTransactionsObserver as Observer,
    AdminDescribeTransactionsObserverError as ObserverError,
    AdminDescribeTransactionsOutcome as Outcome, AdminDescribeTransactionsRequest as Request,
};
