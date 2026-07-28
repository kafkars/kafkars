//! Exact-broker submission policy for Admin `DescribeLogDirs`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{DescribeLogDirsRequest, DescribeLogDirsResponse};

use super::super::DriverOwner;

const DESCRIBE_LOG_DIRS_MIN_VERSION: ApiVersion = ApiVersion::new(1);
const DESCRIBE_LOG_DIRS_MAX_VERSION: ApiVersion = ApiVersion::new(5);

/// Definitely-unsent exact-route construction or driver-admission failure.
#[derive(Debug)]
pub(crate) enum DescribeLogDirsSubmitError {
    InvalidBroker(InvalidBroker),
    Driver(SubmitError),
}

impl fmt::Display for DescribeLogDirsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBroker(source) => {
                write!(formatter, "invalid DescribeLogDirs broker route: {source}")
            }
            Self::Driver(source) => {
                write!(formatter, "driver rejected DescribeLogDirs call: {source}")
            }
        }
    }
}

impl Error for DescribeLogDirsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidBroker(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    /// Submits one read-only log-directory query to an exact broker.
    pub(crate) fn submit_tracked_describe_log_dirs(
        &self,
        broker_id: i32,
        request: DescribeLogDirsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<DescribeLogDirsResponse>, DescribeLogDirsSubmitError> {
        let route = describe_log_dirs_route(broker_id)
            .map_err(DescribeLogDirsSubmitError::InvalidBroker)?;
        self.driver
            .request_tracked_with(route, request, describe_log_dirs_options(deadline))
            .map_err(DescribeLogDirsSubmitError::Driver)
    }
}

pub(super) fn describe_log_dirs_route(broker_id: i32) -> Result<Route, InvalidBroker> {
    validate_broker_id(broker_id)?;
    Ok(Route::AnyBroker)
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

pub(super) const fn describe_log_dirs_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(DESCRIBE_LOG_DIRS_MIN_VERSION)
        .with_maximum_version(DESCRIBE_LOG_DIRS_MAX_VERSION)
}
