//! Route-exact tracked submission of one resource-generic `IncrementalAlterConfigs` request.

use std::{error::Error, fmt, time::Instant};

use kafka_client_core::IncrementalAlterConfigsRoute;
use kafka_driver::{
    ApiVersion, BrokerId, BrokerIdError, RequestOptions, Route, RoutedCall, SubmitError,
    TrafficClass,
};
use kafka_wire::{IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse};

use super::super::DriverOwner;

const INCREMENTAL_ALTER_CONFIGS_MAX_VERSION: ApiVersion = ApiVersion::new(1);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) enum IncrementalAlterConfigsSubmitError {
    InvalidBroker(BrokerIdError),
    Driver(SubmitError),
}

impl fmt::Display for IncrementalAlterConfigsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBroker(source) => {
                write!(
                    formatter,
                    "invalid IncrementalAlterConfigs broker route: {source}"
                )
            }
            Self::Driver(source) => {
                write!(
                    formatter,
                    "driver rejected IncrementalAlterConfigs request: {source}"
                )
            }
        }
    }
}

impl Error for IncrementalAlterConfigsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidBroker(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    pub(crate) fn submit_tracked_incremental_alter_configs(
        &self,
        request: IncrementalAlterConfigsRequest,
        route: IncrementalAlterConfigsRoute,
        deadline: Instant,
    ) -> Result<RoutedCall<IncrementalAlterConfigsResponse>, IncrementalAlterConfigsSubmitError>
    {
        self.driver
            .request_tracked_with(
                incremental_alter_configs_route(route)?,
                request,
                incremental_alter_configs_options(deadline),
            )
            .map_err(IncrementalAlterConfigsSubmitError::Driver)
    }
}

pub(super) fn incremental_alter_configs_route(
    route: IncrementalAlterConfigsRoute,
) -> Result<Route, IncrementalAlterConfigsSubmitError> {
    match route {
        IncrementalAlterConfigsRoute::AnyBroker => Ok(Route::AnyBroker),
        IncrementalAlterConfigsRoute::ExactBroker(raw) => BrokerId::new(raw)
            .map(|broker_id| Route::Broker { broker_id })
            .map_err(IncrementalAlterConfigsSubmitError::InvalidBroker),
    }
}

pub(super) const fn incremental_alter_configs_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_maximum_version(INCREMENTAL_ALTER_CONFIGS_MAX_VERSION)
}
