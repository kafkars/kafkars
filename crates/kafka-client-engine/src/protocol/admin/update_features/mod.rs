//! Flexible API-key 57 request construction and bounded response normalization.

mod materialize;
mod model;
mod prepared;
mod request;
mod response;
mod retention;
mod validation;
mod version;

pub(crate) use model::{
    NormalizedUpdateFeatureResult, NormalizedUpdateFeaturesError, NormalizedUpdateFeaturesOutcome,
    NormalizedUpdateFeaturesResponse, UpdateFeatureMode, UpdateFeatureRef,
    UpdateFeaturesRequestPlan,
};
pub(crate) use prepared::PreparedUpdateFeaturesRequest;
pub(crate) use request::{UpdateFeaturesRequestFailure, update_features_request};
pub(crate) use response::{UpdateFeaturesProtocolFailure, normalize_update_features_response};
pub(crate) use version::UPDATE_FEATURES_MAX_VERSION;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_failure_test;
#[cfg(test)]
mod response_success_test;
