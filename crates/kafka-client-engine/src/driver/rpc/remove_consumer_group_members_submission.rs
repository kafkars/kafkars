//! Tracked coordinator submission of one generated static-member `LeaveGroup` request.

use std::{error::Error, fmt};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{LeaveGroupRequest, LeaveGroupResponse};

use crate::clock::OperationDeadline;

use super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

const REMOVE_MEMBERS_MAX_VERSION: ApiVersion = ApiVersion::new(5);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) enum RemoveConsumerGroupMembersSubmitError {
    GroupMismatch,
    InvalidVersionFloor { actual: i16 },
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for RemoveConsumerGroupMembersSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GroupMismatch => {
                formatter.write_str("member-removal route and request groups differ")
            }
            Self::InvalidVersionFloor { actual } => {
                write!(
                    formatter,
                    "invalid member-removal API-version floor {actual}"
                )
            }
            Self::InvalidGroup(source) => {
                write!(
                    formatter,
                    "invalid member-removal coordinator key: {source}"
                )
            }
            Self::Driver(source) => {
                write!(
                    formatter,
                    "driver rejected member-removal LeaveGroup: {source}"
                )
            }
        }
    }
}

impl Error for RemoveConsumerGroupMembersSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
            Self::GroupMismatch | Self::InvalidVersionFloor { .. } => None,
        }
    }
}

impl DriverOwner {
    pub(super) fn submit_tracked_remove_consumer_group_members(
        &self,
        group: &str,
        request: LeaveGroupRequest,
        minimum_version: i16,
        deadline: OperationDeadline,
    ) -> Result<RoutedCall<LeaveGroupResponse>, RemoveConsumerGroupMembersSubmitError> {
        let route = remove_consumer_group_members_route(group, &request)?;
        let options = remove_consumer_group_members_options(deadline, minimum_version)?;
        self.driver
            .request_tracked_with(route, request, options)
            .map_err(RemoveConsumerGroupMembersSubmitError::Driver)
    }
}

pub(super) fn remove_consumer_group_members_route(
    group: &str,
    request: &LeaveGroupRequest,
) -> Result<Route, RemoveConsumerGroupMembersSubmitError> {
    if request.group_id.as_str() != group {
        return Err(RemoveConsumerGroupMembersSubmitError::GroupMismatch);
    }
    group_coordinator_route(group).map_err(RemoveConsumerGroupMembersSubmitError::InvalidGroup)
}

pub(super) fn remove_consumer_group_members_options(
    deadline: OperationDeadline,
    minimum_version: i16,
) -> Result<RequestOptions, RemoveConsumerGroupMembersSubmitError> {
    if !(3..=5).contains(&minimum_version) {
        return Err(RemoveConsumerGroupMembersSubmitError::InvalidVersionFloor {
            actual: minimum_version,
        });
    }
    Ok(RequestOptions::new(deadline.transport())
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(ApiVersion::new(minimum_version))
        .with_maximum_version(REMOVE_MEMBERS_MAX_VERSION))
}
