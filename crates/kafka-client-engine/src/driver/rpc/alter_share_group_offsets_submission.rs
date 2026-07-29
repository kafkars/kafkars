//! Exact-v0 group-coordinator submission policy for API key 91.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsResponse};

use super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

const ALTER_SHARE_GROUP_OFFSETS_VERSION: ApiVersion = ApiVersion::new(0);

/// Definitely-unsent rejection before API-91 request ownership.
#[derive(Debug)]
pub(crate) enum AlterShareGroupOffsetsSubmitError {
    GroupMismatch,
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for AlterShareGroupOffsetsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GroupMismatch => {
                formatter.write_str("API-91 route and request group identities differ")
            }
            Self::InvalidGroup(source) => {
                write!(formatter, "invalid share-group coordinator key: {source}")
            }
            Self::Driver(source) => {
                write!(
                    formatter,
                    "driver rejected AlterShareGroupOffsets: {source}"
                )
            }
        }
    }
}

impl Error for AlterShareGroupOffsetsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
            Self::GroupMismatch => None,
        }
    }
}

impl DriverOwner {
    pub(super) fn submit_tracked_alter_share_group_offsets(
        &self,
        group: &str,
        request: AlterShareGroupOffsetsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<AlterShareGroupOffsetsResponse>, AlterShareGroupOffsetsSubmitError> {
        let route = alter_share_group_offsets_route(group, &request)?;
        self.driver
            .request_tracked_with(route, request, alter_share_group_offsets_options(deadline))
            .map_err(AlterShareGroupOffsetsSubmitError::Driver)
    }
}

pub(super) fn alter_share_group_offsets_route(
    group: &str,
    request: &AlterShareGroupOffsetsRequest,
) -> Result<Route, AlterShareGroupOffsetsSubmitError> {
    if request.group_id.as_str() != group {
        return Err(AlterShareGroupOffsetsSubmitError::GroupMismatch);
    }
    group_coordinator_route(group).map_err(AlterShareGroupOffsetsSubmitError::InvalidGroup)
}

pub(super) const fn alter_share_group_offsets_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(ALTER_SHARE_GROUP_OFFSETS_VERSION)
        .with_maximum_version(ALTER_SHARE_GROUP_OFFSETS_VERSION)
}
