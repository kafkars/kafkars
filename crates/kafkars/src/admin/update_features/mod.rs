//! Declarative facade for finalized-feature updates and observation.

mod builder;
mod feature;
mod operation;
mod request;
mod result;

pub use builder::UpdateFeaturesBuilder;
pub use feature::{FeatureUpdate, FeatureUpdateIntent};
pub use operation::UpdateFeatures;
pub use result::UpdateFeaturesResult;

pub(crate) use request::{UpdateFeaturesRequest, UpdateFeaturesRequestError};

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod feature_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
