//! Any-broker tracked submission of one topic `DescribeConfigs` request.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{DescribeConfigsRequest, DescribeConfigsResponse};

use super::super::DriverOwner;

const DESCRIBE_CONFIGS_MAX_VERSION: ApiVersion = ApiVersion::new(4);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) struct DescribeConfigsSubmitError {
    source: SubmitError,
}

impl fmt::Display for DescribeConfigsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected topic DescribeConfigs request: {}",
            self.source
        )
    }
}

impl Error for DescribeConfigsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    pub(crate) fn submit_tracked_describe_configs(
        &self,
        request: DescribeConfigsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<DescribeConfigsResponse>, DescribeConfigsSubmitError> {
        self.driver
            .request_tracked_with(
                describe_configs_route(),
                request,
                describe_configs_options(deadline),
            )
            .map_err(|source| DescribeConfigsSubmitError { source })
    }
}

pub(super) const fn describe_configs_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn describe_configs_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_maximum_version(DESCRIBE_CONFIGS_MAX_VERSION)
}
