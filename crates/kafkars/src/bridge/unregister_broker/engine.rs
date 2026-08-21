//! Isolated names for the engine-owned broker-unregistration contract.

pub(super) use kafka_client_engine::{
    UnregisterBrokerAccepted as Accepted, UnregisterBrokerAcceptedFaultKind as AcceptedFaultKind,
    UnregisterBrokerAdmissionError as AdmissionError,
    UnregisterBrokerAdmissionErrorKind as AdmissionErrorKind,
    UnregisterBrokerBrokerError as BrokerError, UnregisterBrokerDeliveryStatus as DeliveryStatus,
    UnregisterBrokerFailure as Failure, UnregisterBrokerFailureKind as FailureKind,
    UnregisterBrokerObserver as Observer, UnregisterBrokerObserverError as ObserverError,
    UnregisterBrokerOutcome as Outcome, UnregisterBrokerResult as Result,
};
