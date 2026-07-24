//! Any-broker tracked submission of one topic `IncrementalAlterConfigs` request.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse};

use super::super::DriverOwner;

const INCREMENTAL_ALTER_CONFIGS_MAX_VERSION: ApiVersion = ApiVersion::new(1);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) struct IncrementalAlterConfigsSubmitError {
    source: SubmitError,
}

impl fmt::Display for IncrementalAlterConfigsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected topic IncrementalAlterConfigs request: {}",
            self.source
        )
    }
}

impl Error for IncrementalAlterConfigsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    pub(crate) fn submit_tracked_incremental_alter_configs(
        &self,
        request: IncrementalAlterConfigsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<IncrementalAlterConfigsResponse>, IncrementalAlterConfigsSubmitError>
    {
        self.driver
            .request_tracked_with(
                incremental_alter_configs_route(),
                request,
                incremental_alter_configs_options(deadline),
            )
            .map_err(|source| IncrementalAlterConfigsSubmitError { source })
    }
}

pub(super) const fn incremental_alter_configs_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn incremental_alter_configs_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_maximum_version(INCREMENTAL_ALTER_CONFIGS_MAX_VERSION)
}
