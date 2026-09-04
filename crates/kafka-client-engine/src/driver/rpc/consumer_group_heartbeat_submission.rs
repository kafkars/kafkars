//! Coordinator-routed submission of one generated KIP-848 heartbeat request.

use std::{error::Error, fmt};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatResponse};

use crate::{
    clock::OperationDeadline,
    protocol::consumer::{
        CONSUMER_GROUP_HEARTBEAT_MAX_VERSION, CONSUMER_GROUP_HEARTBEAT_MIN_VERSION,
    },
};

use super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

pub(super) const CONSUMER_HEARTBEAT_MIN_VERSION: ApiVersion =
    ApiVersion::new(CONSUMER_GROUP_HEARTBEAT_MIN_VERSION);
pub(super) const CONSUMER_HEARTBEAT_MAX_VERSION: ApiVersion =
    ApiVersion::new(CONSUMER_GROUP_HEARTBEAT_MAX_VERSION);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) enum ConsumerGroupHeartbeatSubmitError {
    GroupMismatch,
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConsumerGroupHeartbeatSubmitErrorKind {
    Full,
    Terminal,
}

impl ConsumerGroupHeartbeatSubmitError {
    #[allow(
        clippy::match_same_arms,
        unreachable_patterns,
        reason = "the published driver RC exposes a non-exhaustive admission error while the reviewed path dependency is exhaustive"
    )]
    pub(crate) const fn kind(&self) -> ConsumerGroupHeartbeatSubmitErrorKind {
        match self {
            Self::Driver(SubmitError::Full) => ConsumerGroupHeartbeatSubmitErrorKind::Full,
            Self::GroupMismatch
            | Self::InvalidGroup(_)
            | Self::Driver(
                SubmitError::Closed
                | SubmitError::Wake(_)
                | SubmitError::IdentityExhausted
                | SubmitError::ForeignDriver
                | SubmitError::VersionBoundsInvalid { .. },
            ) => ConsumerGroupHeartbeatSubmitErrorKind::Terminal,
            Self::Driver(_) => ConsumerGroupHeartbeatSubmitErrorKind::Terminal,
        }
    }
}

impl fmt::Display for ConsumerGroupHeartbeatSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GroupMismatch => write!(
                formatter,
                "ConsumerGroupHeartbeat route and request groups differ"
            ),
            Self::InvalidGroup(source) => write!(
                formatter,
                "invalid ConsumerGroupHeartbeat coordinator key: {source}"
            ),
            Self::Driver(source) => {
                write!(
                    formatter,
                    "driver rejected ConsumerGroupHeartbeat: {source}"
                )
            }
        }
    }
}

impl Error for ConsumerGroupHeartbeatSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::GroupMismatch => None,
            Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    pub(crate) fn submit_tracked_consumer_group_heartbeat(
        &self,
        group: &str,
        request: ConsumerGroupHeartbeatRequest,
        deadline: OperationDeadline,
    ) -> Result<RoutedCall<ConsumerGroupHeartbeatResponse>, ConsumerGroupHeartbeatSubmitError> {
        let route = consumer_group_heartbeat_route(group, &request)?;
        self.driver
            .request_tracked_with(route, request, consumer_group_heartbeat_options(deadline))
            .map_err(ConsumerGroupHeartbeatSubmitError::Driver)
    }
}

pub(super) fn consumer_group_heartbeat_route(
    group: &str,
    request: &ConsumerGroupHeartbeatRequest,
) -> Result<Route, ConsumerGroupHeartbeatSubmitError> {
    if request.group_id.as_str() != group {
        return Err(ConsumerGroupHeartbeatSubmitError::GroupMismatch);
    }
    group_coordinator_route(group).map_err(ConsumerGroupHeartbeatSubmitError::InvalidGroup)
}

pub(super) const fn consumer_group_heartbeat_options(
    deadline: OperationDeadline,
) -> RequestOptions {
    // Retain the causal route token for the existing coordinator-loss transition.
    RequestOptions::new(deadline.transport())
        .with_traffic_class(TrafficClass::Control)
        .with_route_failure_rejection()
        .with_minimum_version(CONSUMER_HEARTBEAT_MIN_VERSION)
        .with_maximum_version(CONSUMER_HEARTBEAT_MAX_VERSION)
}
