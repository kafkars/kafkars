//! API-key 18 v3-v5 request construction and bounded feature normalization.

mod materialize;
mod model;
mod request;
mod response;
mod retention;
mod validation;

pub(crate) use model::{
    NormalizedDescribeFeaturesFinalizedFeature, NormalizedDescribeFeaturesResponse,
    NormalizedDescribeFeaturesSupportedFeature,
};
pub(crate) use request::describe_features_request;
pub(crate) use response::{DescribeFeaturesProtocolFailure, normalize_describe_features_response};
#[cfg(test)]
pub(crate) use validation::DESCRIBE_FEATURES_MAX_RETAINED_BYTES;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_failure_test;
#[cfg(test)]
mod response_success_test;
