//! Any-broker submission policy for Admin `DescribeClientQuotas`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{DescribeClientQuotasRequest, DescribeClientQuotasResponse};

use super::super::DriverOwner;

const DESCRIBE_CLIENT_QUOTAS_MIN_VERSION: ApiVersion = ApiVersion::new(0);
const DESCRIBE_CLIENT_QUOTAS_MAX_VERSION: ApiVersion = ApiVersion::new(1);

/// Definitely-unsent bounded-driver rejection.
#[derive(Debug)]
pub(crate) struct DescribeClientQuotasSubmitError {
    source: SubmitError,
}

impl fmt::Display for DescribeClientQuotasSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected DescribeClientQuotas request: {}",
            self.source
        )
    }
}

impl Error for DescribeClientQuotasSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    /// Submits one read-only client quota filter through an arbitrary broker.
    pub(crate) fn submit_describe_client_quotas(
        &self,
        request: DescribeClientQuotasRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<DescribeClientQuotasResponse>, DescribeClientQuotasSubmitError> {
        self.driver
            .request_tracked_with(
                describe_client_quotas_route(),
                request,
                describe_client_quotas_options(deadline),
            )
            .map_err(|source| DescribeClientQuotasSubmitError { source })
    }
}

pub(super) const fn describe_client_quotas_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn describe_client_quotas_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(DESCRIBE_CLIENT_QUOTAS_MIN_VERSION)
        .with_maximum_version(DESCRIBE_CLIENT_QUOTAS_MAX_VERSION)
}
