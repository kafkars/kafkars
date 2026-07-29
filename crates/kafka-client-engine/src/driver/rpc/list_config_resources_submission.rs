//! Tracked AnyBroker submission policy for Admin `ListConfigResources`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{ListConfigResourcesRequest, ListConfigResourcesResponse};

use super::super::DriverOwner;

const LIST_CONFIG_RESOURCES_VERSION: ApiVersion = ApiVersion::new(1);

/// Definitely-unsent failure before the driver accepted request ownership.
#[derive(Debug)]
pub(crate) struct ListConfigResourcesSubmitError {
    source: SubmitError,
}

impl fmt::Display for ListConfigResourcesSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected ListConfigResources request: {}",
            self.source
        )
    }
}

impl Error for ListConfigResourcesSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    /// Submits one exact-v1 configuration-resource listing through any broker.
    pub(crate) fn submit_tracked_list_config_resources(
        &self,
        request: ListConfigResourcesRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<ListConfigResourcesResponse>, ListConfigResourcesSubmitError> {
        self.driver
            .request_tracked_with(
                list_config_resources_route(),
                request,
                list_config_resources_options(deadline),
            )
            .map_err(|source| ListConfigResourcesSubmitError { source })
    }
}

pub(super) const fn list_config_resources_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn list_config_resources_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(LIST_CONFIG_RESOURCES_VERSION)
        .with_maximum_version(LIST_CONFIG_RESOURCES_VERSION)
}
