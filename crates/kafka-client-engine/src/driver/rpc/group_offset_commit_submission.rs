//! Coordinator-routed tracked submission of one classic-group `OffsetCommit`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{OffsetCommitRequest, OffsetCommitResponse};

use super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

const GROUP_OFFSET_COMMIT_MIN_LEADER_EPOCH_VERSION: ApiVersion = ApiVersion::new(6);
const GROUP_OFFSET_COMMIT_STATIC_MEMBERSHIP_VERSION: ApiVersion = ApiVersion::new(7);
const GROUP_OFFSET_COMMIT_MAX_VERSION: ApiVersion = ApiVersion::new(9);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) enum GroupOffsetCommitSubmitError {
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for GroupOffsetCommitSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidGroup(source) => {
                write!(formatter, "invalid group coordinator key: {source}")
            }
            Self::Driver(source) => write!(formatter, "driver rejected OffsetCommit: {source}"),
        }
    }
}

impl Error for GroupOffsetCommitSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    pub(crate) fn submit_tracked_group_offset_commit(
        &self,
        group: &str,
        request: OffsetCommitRequest,
        deadline: Instant,
        requires_leader_epoch: bool,
        static_membership: bool,
    ) -> Result<RoutedCall<OffsetCommitResponse>, GroupOffsetCommitSubmitError> {
        let route = group_offset_commit_route(group)?;
        self.driver
            .request_tracked_with(
                route,
                request,
                group_offset_commit_options(deadline, requires_leader_epoch, static_membership),
            )
            .map_err(GroupOffsetCommitSubmitError::Driver)
    }
}

pub(super) fn group_offset_commit_route(
    group: &str,
) -> Result<Route, GroupOffsetCommitSubmitError> {
    group_coordinator_route(group).map_err(GroupOffsetCommitSubmitError::InvalidGroup)
}

pub(super) const fn group_offset_commit_options(
    deadline: Instant,
    requires_leader_epoch: bool,
    static_membership: bool,
) -> RequestOptions {
    let options = RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_maximum_version(GROUP_OFFSET_COMMIT_MAX_VERSION);
    if static_membership {
        options.with_minimum_version(GROUP_OFFSET_COMMIT_STATIC_MEMBERSHIP_VERSION)
    } else if requires_leader_epoch {
        options.with_minimum_version(GROUP_OFFSET_COMMIT_MIN_LEADER_EPOCH_VERSION)
    } else {
        options
    }
}
