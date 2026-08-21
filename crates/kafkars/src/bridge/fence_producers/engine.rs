//! Isolated aliases for the anticipated engine-owned producer-fencing contract.

pub(super) use kafka_client_engine::{
    AdminFenceProducerEngineBrokerError as BrokerError,
    AdminFenceProducerEngineResult as ItemResult, AdminFenceProducersAccepted as Accepted,
    AdminFenceProducersAcceptedFaultKind as AcceptedFaultKind,
    AdminFenceProducersAdmissionError as AdmissionError,
    AdminFenceProducersAdmissionErrorKind as AdmissionErrorKind,
    AdminFenceProducersDeliveryStatus as DeliveryStatus, AdminFenceProducersEngineBatch as Batch,
    AdminFenceProducersFailure as Failure, AdminFenceProducersFailureKind as FailureKind,
    AdminFenceProducersObserver as Observer, AdminFenceProducersObserverError as ObserverError,
    AdminFenceProducersOutcome as Outcome, AdminFenceProducersRequest as Request,
    AdminFencedProducerEngineIdentity as Identity,
};
