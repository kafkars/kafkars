//! Concrete coordinator-routed submission of one generated `LeaveGroup` request.

use std::{error::Error, fmt};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{LeaveGroupRequest, LeaveGroupResponse};

use crate::{
    clock::OperationDeadline,
    protocol::consumer::{
        CLASSIC_LEAVE_MAX_VERSION, CLASSIC_LEAVE_MIN_VERSION, CLASSIC_STATIC_LEAVE_VERSION,
    },
};

use super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

// v2 is the last single-dynamic-member request shape.
pub(super) const LEAVE_GROUP_MIN_VERSION: ApiVersion = ApiVersion::new(CLASSIC_LEAVE_MIN_VERSION);
pub(super) const LEAVE_GROUP_MAX_VERSION: ApiVersion = ApiVersion::new(CLASSIC_LEAVE_MAX_VERSION);
pub(super) const STATIC_LEAVE_GROUP_VERSION: ApiVersion =
    ApiVersion::new(CLASSIC_STATIC_LEAVE_VERSION);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) enum LeaveGroupSubmitError {
    GroupMismatch,
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for LeaveGroupSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GroupMismatch => {
                formatter.write_str("LeaveGroup route and request groups differ")
            }
            Self::InvalidGroup(source) => {
                write!(formatter, "invalid LeaveGroup coordinator key: {source}")
            }
            Self::Driver(source) => write!(formatter, "driver rejected LeaveGroup: {source}"),
        }
    }
}

impl Error for LeaveGroupSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::GroupMismatch => None,
            Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    pub(crate) fn submit_tracked_leave_group(
        &self,
        group: &str,
        request: LeaveGroupRequest,
        deadline: OperationDeadline,
    ) -> Result<RoutedCall<LeaveGroupResponse>, LeaveGroupSubmitError> {
        let route = leave_group_route(group, &request)?;
        let static_membership = !request.members.is_empty();
        self.driver
            .request_tracked_with(
                route,
                request,
                leave_group_options(deadline, static_membership),
            )
            .map_err(LeaveGroupSubmitError::Driver)
    }
}

pub(super) fn leave_group_route(
    group: &str,
    request: &LeaveGroupRequest,
) -> Result<Route, LeaveGroupSubmitError> {
    if request.group_id.as_str() != group {
        return Err(LeaveGroupSubmitError::GroupMismatch);
    }
    group_coordinator_route(group).map_err(LeaveGroupSubmitError::InvalidGroup)
}

pub(super) const fn leave_group_options(
    deadline: OperationDeadline,
    static_membership: bool,
) -> RequestOptions {
    let options =
        RequestOptions::new(deadline.transport()).with_traffic_class(TrafficClass::Control);
    if static_membership {
        options
            .with_minimum_version(STATIC_LEAVE_GROUP_VERSION)
            .with_maximum_version(STATIC_LEAVE_GROUP_VERSION)
    } else {
        options
            .with_minimum_version(LEAVE_GROUP_MIN_VERSION)
            .with_maximum_version(LEAVE_GROUP_MAX_VERSION)
    }
}
