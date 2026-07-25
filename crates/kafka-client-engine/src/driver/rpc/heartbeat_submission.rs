//! Concrete coordinator-routed submission of one generated classic Heartbeat request.

use std::{error::Error, fmt};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{HeartbeatRequest, HeartbeatResponse};

use crate::{
    clock::OperationDeadline,
    protocol::consumer::{CLASSIC_HEARTBEAT_MAX_VERSION, CLASSIC_HEARTBEAT_MIN_VERSION},
};

use super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

// v2 is the last version before group_instance_id introduces static membership.
pub(super) const HEARTBEAT_MIN_VERSION: ApiVersion = ApiVersion::new(CLASSIC_HEARTBEAT_MIN_VERSION);
pub(super) const HEARTBEAT_MAX_VERSION: ApiVersion = ApiVersion::new(CLASSIC_HEARTBEAT_MAX_VERSION);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) enum ClassicHeartbeatSubmitError {
    GroupMismatch,
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for ClassicHeartbeatSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GroupMismatch => {
                write!(formatter, "Heartbeat route and request groups differ")
            }
            Self::InvalidGroup(source) => {
                write!(formatter, "invalid Heartbeat coordinator key: {source}")
            }
            Self::Driver(source) => write!(formatter, "driver rejected Heartbeat: {source}"),
        }
    }
}

impl Error for ClassicHeartbeatSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::GroupMismatch => None,
            Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    pub(crate) fn submit_tracked_classic_heartbeat(
        &self,
        group: &str,
        request: HeartbeatRequest,
        deadline: OperationDeadline,
    ) -> Result<RoutedCall<HeartbeatResponse>, ClassicHeartbeatSubmitError> {
        let route = classic_heartbeat_route(group, &request)?;
        self.driver
            .request_tracked_with(route, request, classic_heartbeat_options(deadline))
            .map_err(ClassicHeartbeatSubmitError::Driver)
    }
}

pub(super) fn classic_heartbeat_route(
    group: &str,
    request: &HeartbeatRequest,
) -> Result<Route, ClassicHeartbeatSubmitError> {
    if request.group_id.as_str() != group {
        return Err(ClassicHeartbeatSubmitError::GroupMismatch);
    }
    group_coordinator_route(group).map_err(ClassicHeartbeatSubmitError::InvalidGroup)
}

pub(super) const fn classic_heartbeat_options(deadline: OperationDeadline) -> RequestOptions {
    RequestOptions::new(deadline.transport())
        .with_traffic_class(TrafficClass::Control)
        .with_minimum_version(HEARTBEAT_MIN_VERSION)
        .with_maximum_version(HEARTBEAT_MAX_VERSION)
}
