//! Isolated names for the engine-owned UpdateFeatures adapter contract.

pub(super) use kafka_client_engine::{
    UpdateFeature as Feature, UpdateFeatureIntent as FeatureIntent,
    UpdateFeatureOutcome as FeatureOutcome, UpdateFeatureResult as FeatureResult,
    UpdateFeaturesAccepted as Accepted, UpdateFeaturesAcceptedFaultKind as AcceptedFaultKind,
    UpdateFeaturesAdmissionError as AdmissionError,
    UpdateFeaturesAdmissionErrorKind as AdmissionErrorKind, UpdateFeaturesBatch as Batch,
    UpdateFeaturesBrokerError as BrokerError, UpdateFeaturesDeliveryStatus as DeliveryStatus,
    UpdateFeaturesFailure as Failure, UpdateFeaturesFailureKind as FailureKind,
    UpdateFeaturesObserver as Observer, UpdateFeaturesObserverError as ObserverError,
    UpdateFeaturesOutcome as Outcome, UpdateFeaturesRequest as Request,
};
