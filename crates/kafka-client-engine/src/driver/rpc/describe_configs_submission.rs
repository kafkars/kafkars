//! Route-exact tracked submission of one resource-generic `DescribeConfigs` request.

use std::{error::Error, fmt, time::Instant};

use kafka_client_core::DescribeConfigsRoute;
use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{DescribeConfigsRequest, DescribeConfigsResponse};

use super::super::DriverOwner;

const DESCRIBE_CONFIGS_MAX_VERSION: ApiVersion = ApiVersion::new(4);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) enum DescribeConfigsSubmitError {
    InvalidBroker(InvalidBroker),
    Driver(SubmitError),
}

impl fmt::Display for DescribeConfigsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBroker(source) => {
                write!(formatter, "invalid DescribeConfigs broker route: {source}")
            }
            Self::Driver(source) => {
                write!(
                    formatter,
                    "driver rejected DescribeConfigs request: {source}"
                )
            }
        }
    }
}

impl Error for DescribeConfigsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidBroker(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    pub(crate) fn submit_tracked_describe_configs(
        &self,
        request: DescribeConfigsRequest,
        route: DescribeConfigsRoute,
        deadline: Instant,
    ) -> Result<RoutedCall<DescribeConfigsResponse>, DescribeConfigsSubmitError> {
        let route = describe_configs_route(route)?;
        self.driver
            .request_tracked_with(route, request, describe_configs_options(deadline))
            .map_err(DescribeConfigsSubmitError::Driver)
    }
}

pub(super) fn describe_configs_route(
    route: DescribeConfigsRoute,
) -> Result<Route, DescribeConfigsSubmitError> {
    match route {
        DescribeConfigsRoute::AnyBroker => Ok(Route::AnyBroker),
        DescribeConfigsRoute::ExactBroker(raw) => {
            validate_broker_id(raw).map_err(DescribeConfigsSubmitError::InvalidBroker)?;
            Ok(Route::AnyBroker)
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

const fn validate_broker_id(broker_id: i32) -> Result<(), InvalidBroker> {
    if broker_id < 0 {
        return Err(InvalidBroker(broker_id));
    }
    Ok(())
}

pub(super) const fn describe_configs_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_maximum_version(DESCRIBE_CONFIGS_MAX_VERSION)
}
