//! Exact-broker submission policy for Admin `AlterReplicaLogDirs`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, BrokerId, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse};

use crate::protocol::admin::alter_replica_log_dirs::{
    ALTER_REPLICA_LOG_DIRS_MAX_VERSION as ALTER_REPLICA_LOG_DIRS_MAX_VERSION_VALUE,
    ALTER_REPLICA_LOG_DIRS_MIN_VERSION as ALTER_REPLICA_LOG_DIRS_MIN_VERSION_VALUE,
};

use super::super::DriverOwner;

const ALTER_REPLICA_LOG_DIRS_MIN_VERSION: ApiVersion =
    ApiVersion::new(ALTER_REPLICA_LOG_DIRS_MIN_VERSION_VALUE);
const ALTER_REPLICA_LOG_DIRS_MAX_VERSION: ApiVersion =
    ApiVersion::new(ALTER_REPLICA_LOG_DIRS_MAX_VERSION_VALUE);

/// Definitely-unsent exact-route construction or driver-admission failure.
#[derive(Debug)]
pub(crate) enum AlterReplicaLogDirsSubmitError {
    InvalidBroker(InvalidBroker),
    Driver(SubmitError),
}

impl fmt::Display for AlterReplicaLogDirsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBroker(source) => {
                write!(
                    formatter,
                    "invalid AlterReplicaLogDirs broker route: {source}"
                )
            }
            Self::Driver(source) => {
                write!(
                    formatter,
                    "driver rejected AlterReplicaLogDirs call: {source}"
                )
            }
        }
    }
}

impl Error for AlterReplicaLogDirsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidBroker(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    /// Submits one mutation batch to an exact broker.
    pub(crate) fn submit_tracked_alter_replica_log_dirs(
        &self,
        broker_id: i32,
        request: AlterReplicaLogDirsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<AlterReplicaLogDirsResponse>, AlterReplicaLogDirsSubmitError> {
        let route = alter_replica_log_dirs_route(broker_id)
            .map_err(AlterReplicaLogDirsSubmitError::InvalidBroker)?;
        self.driver
            .request_tracked_with(route, request, alter_replica_log_dirs_options(deadline))
            .map_err(AlterReplicaLogDirsSubmitError::Driver)
    }
}

pub(super) fn alter_replica_log_dirs_route(broker_id: i32) -> Result<Route, InvalidBroker> {
    let broker_id = BrokerId::new(broker_id).map_err(|_error| InvalidBroker(broker_id))?;
    Ok(Route::Broker { broker_id })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InvalidBroker(i32);

impl fmt::Display for InvalidBroker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "broker ID {} must be nonnegative", self.0)
    }
}

impl Error for InvalidBroker {}

pub(super) const fn alter_replica_log_dirs_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(ALTER_REPLICA_LOG_DIRS_MIN_VERSION)
        .with_maximum_version(ALTER_REPLICA_LOG_DIRS_MAX_VERSION)
}
