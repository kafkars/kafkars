//! Declarative facade for supported and finalized Kafka feature discovery.

mod builder;
mod feature;
mod operation;
mod result;

pub use builder::DescribeFeaturesBuilder;
pub use feature::{FinalizedFeature, SupportedFeature};
pub use operation::DescribeFeatures;
pub use result::DescribeFeaturesResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod feature_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
