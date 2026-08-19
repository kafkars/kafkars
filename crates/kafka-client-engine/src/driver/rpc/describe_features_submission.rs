//! Tracked `AnyBroker` submission policy for Admin `DescribeFeatures`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{ApiVersionsRequest, ApiVersionsResponse};

use super::super::DriverOwner;

const DESCRIBE_FEATURES_MIN_VERSION: ApiVersion = ApiVersion::new(3);
const DESCRIBE_FEATURES_MAX_VERSION: ApiVersion = ApiVersion::new(5);

/// Definitely-unsent bounded-driver rejection.
#[derive(Debug)]
pub(crate) struct DescribeFeaturesSubmitError {
    source: SubmitError,
}

impl fmt::Display for DescribeFeaturesSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected DescribeFeatures request: {}",
            self.source
        )
    }
}

impl Error for DescribeFeaturesSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    /// Submits one feature query through any broker without retry policy.
    pub(crate) fn submit_tracked_describe_features(
        &self,
        request: ApiVersionsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<ApiVersionsResponse>, DescribeFeaturesSubmitError> {
        self.driver
            .request_tracked_with(
                describe_features_route(),
                request,
                describe_features_options(deadline),
            )
            .map_err(|source| DescribeFeaturesSubmitError { source })
    }
}

pub(super) const fn describe_features_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn describe_features_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(DESCRIBE_FEATURES_MIN_VERSION)
        .with_maximum_version(DESCRIBE_FEATURES_MAX_VERSION)
}
