//! Isolated names for the engine-owned Admin `DescribeProducers` contract.

pub(super) use kafka_client_engine::{
    AdminDescribeProducerEngineBrokerError as BrokerError,
    AdminDescribeProducerState as ProducerState, AdminDescribeProducersAccepted as Accepted,
    AdminDescribeProducersAcceptedFaultKind as AcceptedFaultKind,
    AdminDescribeProducersAdmissionError as AdmissionError,
    AdminDescribeProducersAdmissionErrorKind as AdmissionErrorKind,
    AdminDescribeProducersDeliveryStatus as DeliveryStatus,
    AdminDescribeProducersFailure as Failure, AdminDescribeProducersFailureKind as FailureKind,
    AdminDescribeProducersObserver as Observer,
    AdminDescribeProducersObserverError as ObserverError, AdminDescribeProducersOutcome as Outcome,
    AdminDescribeProducersRequest as Request, AdminDescribeProducersRequestTarget as RequestTarget,
};
