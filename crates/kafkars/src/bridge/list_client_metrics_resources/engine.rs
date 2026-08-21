//! Isolated names for the engine-owned client-metrics resource contract.

pub(super) use kafka_client_engine::{
    ListClientMetricsResourcesAccepted as Accepted,
    ListClientMetricsResourcesAcceptedFaultKind as AcceptedFaultKind,
    ListClientMetricsResourcesAdmissionError as AdmissionError,
    ListClientMetricsResourcesAdmissionErrorKind as AdmissionErrorKind,
    ListClientMetricsResourcesBrokerError as BrokerError,
    ListClientMetricsResourcesDeliveryStatus as DeliveryStatus,
    ListClientMetricsResourcesFailure as Failure,
    ListClientMetricsResourcesFailureKind as FailureKind,
    ListClientMetricsResourcesListing as Listing, ListClientMetricsResourcesObserver as Observer,
    ListClientMetricsResourcesObserverError as ObserverError,
    ListClientMetricsResourcesOutcome as Outcome,
};
