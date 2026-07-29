//! Exact-broker submission policy for Admin `DescribeReplicaLogDirs`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, BrokerId, BrokerIdError, RequestOptions, Route, RoutedCall, SubmitError,
    TrafficClass,
};
use kafka_wire::{DescribeLogDirsRequest, DescribeLogDirsResponse};

use super::super::DriverOwner;

const DESCRIBE_REPLICA_LOG_DIRS_MIN_VERSION: ApiVersion = ApiVersion::new(1);
const DESCRIBE_REPLICA_LOG_DIRS_MAX_VERSION: ApiVersion = ApiVersion::new(5);

/// Definitely-unsent exact-route construction or driver-admission failure.
#[derive(Debug)]
pub(crate) enum DescribeReplicaLogDirsSubmitError {
    InvalidBroker(BrokerIdError),
    Driver(SubmitError),
}

impl fmt::Display for DescribeReplicaLogDirsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBroker(source) => {
                write!(
                    formatter,
                    "invalid DescribeReplicaLogDirs broker route: {source}"
                )
            }
            Self::Driver(source) => {
                write!(
                    formatter,
                    "driver rejected DescribeReplicaLogDirs call: {source}"
                )
            }
        }
    }
}

impl Error for DescribeReplicaLogDirsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidBroker(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    /// Submits one selected-replica query to an exact broker.
    pub(crate) fn submit_tracked_describe_replica_log_dirs(
        &self,
        broker_id: i32,
        request: DescribeLogDirsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<DescribeLogDirsResponse>, DescribeReplicaLogDirsSubmitError> {
        let route = describe_replica_log_dirs_route(broker_id)
            .map_err(DescribeReplicaLogDirsSubmitError::InvalidBroker)?;
        self.driver
            .request_tracked_with(route, request, describe_replica_log_dirs_options(deadline))
            .map_err(DescribeReplicaLogDirsSubmitError::Driver)
    }
}

pub(super) fn describe_replica_log_dirs_route(broker_id: i32) -> Result<Route, BrokerIdError> {
    Ok(Route::Broker {
        broker_id: BrokerId::new(broker_id)?,
    })
}

pub(super) const fn describe_replica_log_dirs_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(DESCRIBE_REPLICA_LOG_DIRS_MIN_VERSION)
        .with_maximum_version(DESCRIBE_REPLICA_LOG_DIRS_MAX_VERSION)
}
