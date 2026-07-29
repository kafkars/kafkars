//! Deterministic policy for one controller-owned Admin `UpdateFeatures` batch.

mod failure;
mod machine;
mod model;
mod outcome;
mod transition;

pub use failure::{
    UPDATE_FEATURES_DIAGNOSTIC_BYTES, UpdateFeaturesBrokerError, UpdateFeaturesFailure,
    UpdateFeaturesFailureKind, UpdateFeaturesTerminal,
};
pub use machine::{
    UpdateFeaturesEffect, UpdateFeaturesInput, UpdateFeaturesMachine, UpdateFeaturesMachineError,
    UpdateFeaturesState, UpdateFeaturesTransition,
};
pub use model::{
    UPDATE_FEATURES_MAX_FEATURE_NAME_BYTES, UPDATE_FEATURES_MAX_FEATURE_TEXT_BYTES,
    UPDATE_FEATURES_MAX_UPDATES, UpdateFeature, UpdateFeatureIntent, UpdateFeaturesPlan,
    UpdateFeaturesPlanError,
};
pub use outcome::{
    UpdateFeatureOutcome, UpdateFeatureResult, UpdateFeaturesBatch, UpdateFeaturesBrokerResponse,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
