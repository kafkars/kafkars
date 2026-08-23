//! Group-coordinator-routed submission of one generated API 76 v1 request.
#![allow(
    dead_code,
    reason = "closed submission adapter checkpoint precedes its hosted membership owner"
)]

use std::{error::Error, fmt};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{ShareGroupHeartbeatRequest, ShareGroupHeartbeatResponse};

use crate::{
    clock::OperationDeadline,
    protocol::consumer::share_group::{
        SHARE_GROUP_HEARTBEAT_MAX_VERSION, SHARE_GROUP_HEARTBEAT_MIN_VERSION,
    },
};

use super::super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

pub(super) const SHARE_HEARTBEAT_MIN_VERSION: ApiVersion =
    ApiVersion::new(SHARE_GROUP_HEARTBEAT_MIN_VERSION);
pub(super) const SHARE_HEARTBEAT_MAX_VERSION: ApiVersion =
    ApiVersion::new(SHARE_GROUP_HEARTBEAT_MAX_VERSION);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) enum ShareGroupHeartbeatSubmitError {
    GroupMismatch,
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareGroupHeartbeatSubmitErrorKind {
    Full,
    Terminal,
}

impl ShareGroupHeartbeatSubmitError {
    #[allow(
        clippy::match_same_arms,
        unreachable_patterns,
        reason = "published driver RC is non-exhaustive while the reviewed path is exhaustive"
    )]
    pub(crate) const fn kind(&self) -> ShareGroupHeartbeatSubmitErrorKind {
        match self {
            Self::Driver(SubmitError::Full) => ShareGroupHeartbeatSubmitErrorKind::Full,
            Self::GroupMismatch | Self::InvalidGroup(_) | Self::Driver(_) => {
                ShareGroupHeartbeatSubmitErrorKind::Terminal
            }
        }
    }
}

impl fmt::Display for ShareGroupHeartbeatSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GroupMismatch => write!(
                formatter,
                "ShareGroupHeartbeat route and request groups differ"
            ),
            Self::InvalidGroup(source) => {
                write!(
                    formatter,
                    "invalid ShareGroupHeartbeat coordinator key: {source}"
                )
            }
            Self::Driver(source) => {
                write!(formatter, "driver rejected ShareGroupHeartbeat: {source}")
            }
        }
    }
}

impl Error for ShareGroupHeartbeatSubmitError {}

impl DriverOwner {
    pub(crate) fn submit_tracked_share_group_heartbeat(
        &self,
        group: &str,
        request: ShareGroupHeartbeatRequest,
        deadline: OperationDeadline,
    ) -> Result<RoutedCall<ShareGroupHeartbeatResponse>, ShareGroupHeartbeatSubmitError> {
        let route = share_group_heartbeat_route(group, &request)?;
        self.driver
            .request_tracked_with(route, request, share_group_heartbeat_options(deadline))
            .map_err(ShareGroupHeartbeatSubmitError::Driver)
    }
}

pub(super) fn share_group_heartbeat_route(
    group: &str,
    request: &ShareGroupHeartbeatRequest,
) -> Result<Route, ShareGroupHeartbeatSubmitError> {
    if request.group_id.as_str() != group {
        return Err(ShareGroupHeartbeatSubmitError::GroupMismatch);
    }
    group_coordinator_route(group).map_err(ShareGroupHeartbeatSubmitError::InvalidGroup)
}

pub(super) const fn share_group_heartbeat_options(deadline: OperationDeadline) -> RequestOptions {
    RequestOptions::new(deadline.transport())
        .with_traffic_class(TrafficClass::Control)
        .with_minimum_version(SHARE_HEARTBEAT_MIN_VERSION)
        .with_maximum_version(SHARE_HEARTBEAT_MAX_VERSION)
}
