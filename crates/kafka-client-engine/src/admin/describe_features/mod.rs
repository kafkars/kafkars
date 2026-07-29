//! Declarative facade for the concrete Admin `DescribeFeatures` engine owner.

pub(crate) mod api;
mod description;
mod error;
mod handle;
mod host;
mod observer;
mod outcome;
mod shard;

pub use description::{
    DescribeFeaturesDescription, DescribeFeaturesFinalizedFeature, DescribeFeaturesSupportedFeature,
};
pub use error::{DescribeFeaturesAdmissionError, DescribeFeaturesAdmissionErrorKind};
pub use handle::{DescribeFeaturesAccepted, DescribeFeaturesAcceptedFaultKind};
pub use observer::DescribeFeaturesObserver;
pub use outcome::{
    DescribeFeaturesBrokerError, DescribeFeaturesDeliveryStatus, DescribeFeaturesFailure,
    DescribeFeaturesFailureKind, DescribeFeaturesObserverError, DescribeFeaturesOutcome,
};

pub(crate) use error::DescribeFeaturesHostError;
pub(crate) use host::{DESCRIBE_FEATURES_CAPACITY, DescribeFeaturesHost, DescribeFeaturesTurn};
pub(crate) use shard::{
    DescribeFeaturesAdmissionPort, DescribeFeaturesShardLockError, DescribeFeaturesShardOwner,
    DescribeFeaturesShardWake, DescribeFeaturesShardWakeError,
};

#[cfg(test)]
mod description_test;
#[cfg(test)]
mod host_test;
#[cfg(test)]
mod outcome_test;
