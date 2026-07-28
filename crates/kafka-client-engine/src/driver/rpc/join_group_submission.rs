//! Concrete coordinator-routed submission of one generated `JoinGroup` request.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{JoinGroupRequest, JoinGroupResponse};

use crate::protocol::consumer::{
    CLASSIC_JOIN_MAX_VERSION, CLASSIC_JOIN_MIN_VERSION, CLASSIC_STATIC_JOIN_VERSION,
};

use super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

// v1 carries the separate rebalance timeout. v3 is the last version before the
// MEMBER_ID_REQUIRED handshake that deterministic membership policy does not
// yet own.
pub(super) const JOIN_GROUP_MIN_VERSION: ApiVersion = ApiVersion::new(CLASSIC_JOIN_MIN_VERSION);
pub(super) const JOIN_GROUP_MAX_VERSION: ApiVersion = ApiVersion::new(CLASSIC_JOIN_MAX_VERSION);
pub(super) const STATIC_JOIN_GROUP_VERSION: ApiVersion =
    ApiVersion::new(CLASSIC_STATIC_JOIN_VERSION);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) enum JoinGroupSubmitError {
    GroupMismatch,
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for JoinGroupSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GroupMismatch => {
                write!(formatter, "JoinGroup route and request groups differ")
            }
            Self::InvalidGroup(source) => {
                write!(formatter, "invalid JoinGroup coordinator key: {source}")
            }
            Self::Driver(source) => write!(formatter, "driver rejected JoinGroup: {source}"),
        }
    }
}

impl Error for JoinGroupSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::GroupMismatch => None,
            Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    pub(crate) fn submit_tracked_join_group(
        &self,
        group: &str,
        request: JoinGroupRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<JoinGroupResponse>, JoinGroupSubmitError> {
        let route = join_group_route(group, &request)?;
        let static_membership = request.group_instance_id.is_some();
        self.driver
            .request_tracked_with(
                route,
                request,
                join_group_options(deadline, static_membership),
            )
            .map_err(JoinGroupSubmitError::Driver)
    }
}

pub(super) fn join_group_route(
    group: &str,
    request: &JoinGroupRequest,
) -> Result<Route, JoinGroupSubmitError> {
    if request.group_id.as_str() != group {
        return Err(JoinGroupSubmitError::GroupMismatch);
    }
    group_coordinator_route(group).map_err(JoinGroupSubmitError::InvalidGroup)
}

pub(super) const fn join_group_options(
    deadline: Instant,
    static_membership: bool,
) -> RequestOptions {
    let options = RequestOptions::new(deadline).with_traffic_class(TrafficClass::Interactive);
    if static_membership {
        options
            .with_minimum_version(STATIC_JOIN_GROUP_VERSION)
            .with_maximum_version(STATIC_JOIN_GROUP_VERSION)
    } else {
        options
            .with_minimum_version(JOIN_GROUP_MIN_VERSION)
            .with_maximum_version(JOIN_GROUP_MAX_VERSION)
    }
}
