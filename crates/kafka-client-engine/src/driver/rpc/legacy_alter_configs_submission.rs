//! Any-broker single-attempt submission policy for legacy resource configuration replacement.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{AlterConfigsRequest, AlterConfigsResponse};

use super::super::DriverOwner;

const LEGACY_ALTER_CONFIGS_MIN_VERSION: ApiVersion = ApiVersion::new(0);
const LEGACY_ALTER_CONFIGS_MAX_VERSION: ApiVersion = ApiVersion::new(2);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) struct LegacyAlterConfigsSubmitError {
    source: SubmitError,
}

impl fmt::Display for LegacyAlterConfigsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected legacy resource AlterConfigs request: {}",
            self.source
        )
    }
}

impl Error for LegacyAlterConfigsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    pub(crate) fn submit_tracked_legacy_alter_configs(
        &self,
        request: AlterConfigsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<AlterConfigsResponse>, LegacyAlterConfigsSubmitError> {
        self.driver
            .request_tracked_with(
                legacy_alter_configs_route(),
                request,
                legacy_alter_configs_options(deadline),
            )
            .map_err(|source| LegacyAlterConfigsSubmitError { source })
    }
}

pub(super) const fn legacy_alter_configs_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn legacy_alter_configs_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(LEGACY_ALTER_CONFIGS_MIN_VERSION)
        .with_maximum_version(LEGACY_ALTER_CONFIGS_MAX_VERSION)
}
