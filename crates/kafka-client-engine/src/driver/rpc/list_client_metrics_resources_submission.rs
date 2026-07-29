//! Tracked AnyBroker submission policy for Admin `ListClientMetricsResources`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{ListConfigResourcesRequest, ListConfigResourcesResponse};

use super::super::DriverOwner;

const LIST_CLIENT_METRICS_RESOURCES_VERSION: ApiVersion = ApiVersion::new(0);

/// Definitely-unsent failure before the driver accepted request ownership.
#[derive(Debug)]
pub(crate) struct ListClientMetricsResourcesSubmitError {
    source: SubmitError,
}

impl fmt::Display for ListClientMetricsResourcesSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected ListClientMetricsResources request: {}",
            self.source
        )
    }
}

impl Error for ListClientMetricsResourcesSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    /// Submits one v0 client-metrics resource listing through any broker.
    pub(crate) fn submit_tracked_list_client_metrics_resources(
        &self,
        request: ListConfigResourcesRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<ListConfigResourcesResponse>, ListClientMetricsResourcesSubmitError>
    {
        self.driver
            .request_tracked_with(
                list_client_metrics_resources_route(),
                request,
                list_client_metrics_resources_options(deadline),
            )
            .map_err(|source| ListClientMetricsResourcesSubmitError { source })
    }
}

pub(super) const fn list_client_metrics_resources_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn list_client_metrics_resources_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(LIST_CLIENT_METRICS_RESOURCES_VERSION)
        .with_maximum_version(LIST_CLIENT_METRICS_RESOURCES_VERSION)
}
