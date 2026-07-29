//! Isolated names for the engine-owned configuration-resource contract.

pub(super) use kafka_client_engine::{
    ListConfigResource as Resource, ListConfigResourcesAccepted as Accepted,
    ListConfigResourcesAcceptedFaultKind as AcceptedFaultKind,
    ListConfigResourcesAdmissionError as AdmissionError,
    ListConfigResourcesAdmissionErrorKind as AdmissionErrorKind,
    ListConfigResourcesBrokerError as BrokerError,
    ListConfigResourcesDeliveryStatus as DeliveryStatus, ListConfigResourcesFailure as Failure,
    ListConfigResourcesFailureKind as FailureKind, ListConfigResourcesListing as Listing,
    ListConfigResourcesObserver as Observer, ListConfigResourcesObserverError as ObserverError,
    ListConfigResourcesOutcome as Outcome,
};
