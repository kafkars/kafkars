//! Tracked group-coordinator submission of one generated name-based `OffsetCommit`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{OffsetCommitRequest, OffsetCommitResponse};

use crate::protocol::admin::group_offset_alter::{
    GROUP_OFFSET_ALTER_MAX_VERSION, OffsetCommitTargetRef, group_offset_alter_minimum_version,
};

use super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) enum GroupOffsetAlterSubmitError {
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for GroupOffsetAlterSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidGroup(source) => {
                write!(formatter, "invalid group coordinator key: {source}")
            }
            Self::Driver(source) => write!(formatter, "driver rejected OffsetCommit: {source}"),
        }
    }
}

impl Error for GroupOffsetAlterSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    pub(super) fn submit_tracked_group_offset_alter(
        &self,
        group: &str,
        targets: &[OffsetCommitTargetRef<'_>],
        request: OffsetCommitRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<OffsetCommitResponse>, GroupOffsetAlterSubmitError> {
        let route =
            group_coordinator_route(group).map_err(GroupOffsetAlterSubmitError::InvalidGroup)?;
        self.driver
            .request_tracked_with(
                route,
                request,
                group_offset_alter_options(targets, deadline),
            )
            .map_err(GroupOffsetAlterSubmitError::Driver)
    }
}

pub(super) fn group_offset_alter_options(
    targets: &[OffsetCommitTargetRef<'_>],
    deadline: Instant,
) -> RequestOptions {
    let minimum = ApiVersion::new(group_offset_alter_minimum_version(targets).value());
    let maximum = ApiVersion::new(GROUP_OFFSET_ALTER_MAX_VERSION.value());
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(minimum)
        .with_maximum_version(maximum)
}
