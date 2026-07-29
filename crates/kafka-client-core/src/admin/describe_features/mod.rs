//! Deterministic policy and bounded values for one Admin `DescribeFeatures` query.

mod description;
mod failure;
mod feature;
mod machine;
mod transition;
mod value_error;

pub use description::{
    DESCRIBE_FEATURES_MAX_FEATURE_NAME_BYTES, DESCRIBE_FEATURES_MAX_FEATURE_TEXT_BYTES,
    DESCRIBE_FEATURES_MAX_FEATURES_PER_COLLECTION, DESCRIBE_FEATURES_MAX_RETAINED_BYTES,
    DescribeFeaturesDescription,
};
pub use failure::{
    DescribeFeaturesBrokerError, DescribeFeaturesFailure, DescribeFeaturesFailureKind,
    DescribeFeaturesTerminal,
};
pub use feature::{DescribeFeaturesFinalizedFeature, DescribeFeaturesSupportedFeature};
pub use machine::{
    DescribeFeaturesEffect, DescribeFeaturesInput, DescribeFeaturesMachine,
    DescribeFeaturesMachineError, DescribeFeaturesState, DescribeFeaturesTransition,
};
pub use value_error::DescribeFeaturesValueError;

#[cfg(test)]
mod description_test;
#[cfg(test)]
mod failure_test;
#[cfg(test)]
mod feature_test;
#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod transition_test;
