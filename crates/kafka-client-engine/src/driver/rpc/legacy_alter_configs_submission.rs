//! Route-exact single-attempt submission for legacy resource configuration replacement.

use std::{error::Error, fmt, time::Instant};

use kafka_client_core::LegacyAlterConfigsRoute;
use kafka_driver::{
    ApiVersion, BrokerId, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{AlterConfigsRequest, AlterConfigsResponse};

use super::super::DriverOwner;

const LEGACY_ALTER_CONFIGS_MIN_VERSION: ApiVersion = ApiVersion::new(0);
const LEGACY_ALTER_CONFIGS_MAX_VERSION: ApiVersion = ApiVersion::new(2);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) enum LegacyAlterConfigsSubmitError {
    InvalidBroker(InvalidBroker),
    Driver(SubmitError),
}

impl fmt::Display for LegacyAlterConfigsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBroker(source) => {
                write!(
                    formatter,
                    "invalid legacy AlterConfigs broker route: {source}"
                )
            }
            Self::Driver(source) => {
                write!(
                    formatter,
                    "driver rejected legacy resource AlterConfigs request: {source}"
                )
            }
        }
    }
}

impl Error for LegacyAlterConfigsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidBroker(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    pub(crate) fn submit_tracked_legacy_alter_configs(
        &self,
        request: AlterConfigsRequest,
        route: LegacyAlterConfigsRoute,
        deadline: Instant,
    ) -> Result<RoutedCall<AlterConfigsResponse>, LegacyAlterConfigsSubmitError> {
        self.driver
            .request_tracked_with(
                legacy_alter_configs_route(route)?,
                request,
                legacy_alter_configs_options(deadline),
            )
            .map_err(LegacyAlterConfigsSubmitError::Driver)
    }
}

pub(super) fn legacy_alter_configs_route(
    route: LegacyAlterConfigsRoute,
) -> Result<Route, LegacyAlterConfigsSubmitError> {
    match route {
        LegacyAlterConfigsRoute::AnyBroker => Ok(Route::AnyBroker),
        LegacyAlterConfigsRoute::ExactBroker(raw) => {
            let broker_id = BrokerId::new(raw).map_err(|_error| {
                LegacyAlterConfigsSubmitError::InvalidBroker(InvalidBroker(raw))
            })?;
            Ok(Route::Broker { broker_id })
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InvalidBroker(i32);

impl fmt::Display for InvalidBroker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "broker ID {} must be nonnegative", self.0)
    }
}

impl Error for InvalidBroker {}

pub(super) const fn legacy_alter_configs_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(LEGACY_ALTER_CONFIGS_MIN_VERSION)
        .with_maximum_version(LEGACY_ALTER_CONFIGS_MAX_VERSION)
}
