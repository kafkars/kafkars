//! Declarative facade for the concrete Admin `UpdateFeatures` engine owner.

pub(crate) mod api;
mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub use error::{UpdateFeaturesAdmissionError, UpdateFeaturesAdmissionErrorKind};
pub use handle::{UpdateFeaturesAccepted, UpdateFeaturesAcceptedFaultKind, UpdateFeaturesCapture};
pub use model::{UpdateFeature, UpdateFeatureIntent, UpdateFeaturesRequest};
pub use observer::UpdateFeaturesObserver;
pub use outcome::{
    UpdateFeatureOutcome, UpdateFeatureResult, UpdateFeaturesBatch, UpdateFeaturesBrokerError,
    UpdateFeaturesDeliveryStatus, UpdateFeaturesFailure, UpdateFeaturesFailureKind,
    UpdateFeaturesObserverError, UpdateFeaturesOutcome,
};

pub(crate) use error::UpdateFeaturesHostError;
pub(crate) use host::{UPDATE_FEATURES_CAPACITY, UpdateFeaturesHost, UpdateFeaturesTurn};
pub(crate) use shard::{
    UpdateFeaturesAdmissionPort, UpdateFeaturesShardLockError, UpdateFeaturesShardOwner,
    UpdateFeaturesShardWake, UpdateFeaturesShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
