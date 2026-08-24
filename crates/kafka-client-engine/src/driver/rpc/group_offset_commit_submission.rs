//! Coordinator-routed tracked submission of one classic-group `OffsetCommit`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{OffsetCommitRequest, OffsetCommitResponse};

use crate::protocol::consumer::{PreparedGroupOffsetCommit, PreparedGroupOffsetCommitRequest};

use super::{
    super::DriverOwner, group_coordinator_route::group_coordinator_route,
    group_offset_commit_recovery::GroupOffsetCommitPrebuiltAdmissionFailure,
};

const GROUP_OFFSET_COMMIT_MIN_LEADER_EPOCH_VERSION: ApiVersion = ApiVersion::new(6);
const GROUP_OFFSET_COMMIT_STATIC_MEMBERSHIP_VERSION: ApiVersion = ApiVersion::new(7);
const GROUP_OFFSET_COMMIT_CONSUMER_GROUP_VERSION: ApiVersion = ApiVersion::new(9);
const GROUP_OFFSET_COMMIT_MAX_VERSION: ApiVersion = ApiVersion::new(9);

pub(super) struct TrackedGroupOffsetCommitCall {
    pub(super) prepared: PreparedGroupOffsetCommit,
    pub(super) call: RoutedCall<OffsetCommitResponse>,
    pub(super) replacement_used: bool,
}

impl TrackedGroupOffsetCommitCall {
    pub(super) fn into_prepared(self) -> PreparedGroupOffsetCommit {
        let Self {
            prepared,
            call,
            replacement_used: _,
        } = self;
        drop(call);
        prepared
    }
}

/// Preflighted ownership of exactly one bounded group commit call slot.
#[must_use = "a reserved group commit call slot must be submitted or released"]
pub(crate) struct GroupOffsetCommitCallPermit<'a> {
    pub(super) calls: &'a mut Vec<TrackedGroupOffsetCommitCall>,
}

impl GroupOffsetCommitCallPermit<'_> {
    #[allow(
        clippy::result_large_err,
        reason = "driver rejection must return both exact prepared owners"
    )]
    pub(crate) fn submit_prebuilt(
        self,
        driver: &DriverOwner,
        prepared: PreparedGroupOffsetCommit,
        request: PreparedGroupOffsetCommitRequest,
    ) -> Result<kafka_client_core::GroupOffsetCommitInput, GroupOffsetCommitPrebuiltAdmissionFailure>
    {
        self.submit(driver, prepared, request, false)
            .map(|()| kafka_client_core::GroupOffsetCommitInput::DriverAccepted)
    }

    #[allow(
        clippy::result_large_err,
        reason = "driver rejection must return the exact prepared owner"
    )]
    pub(super) fn submit_replacement(
        self,
        driver: &DriverOwner,
        prepared: PreparedGroupOffsetCommit,
        request: PreparedGroupOffsetCommitRequest,
    ) -> Result<(), GroupOffsetCommitPrebuiltAdmissionFailure> {
        self.submit(driver, prepared, request, true)
    }

    #[allow(
        clippy::result_large_err,
        reason = "driver rejection must return the exact prepared owner"
    )]
    fn submit(
        self,
        driver: &DriverOwner,
        prepared: PreparedGroupOffsetCommit,
        request: PreparedGroupOffsetCommitRequest,
        replacement_used: bool,
    ) -> Result<(), GroupOffsetCommitPrebuiltAdmissionFailure> {
        let request = request.into_generated_offset_commit_request();
        let static_membership = request.group_instance_id.is_some();
        let call = match driver.submit_tracked_group_offset_commit(
            prepared.group().as_ref(),
            request,
            prepared.operation_deadline().transport(),
            prepared.requires_leader_epoch(),
            static_membership,
            prepared.requires_consumer_group_version(),
        ) {
            Ok(call) => call,
            Err(source) => {
                return Err(GroupOffsetCommitPrebuiltAdmissionFailure::new(
                    prepared, source,
                ));
            }
        };
        self.calls.push(TrackedGroupOffsetCommitCall {
            prepared,
            call,
            replacement_used,
        });
        Ok(())
    }
}

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
        consumer_group_protocol: bool,
    ) -> Result<RoutedCall<OffsetCommitResponse>, GroupOffsetCommitSubmitError> {
        let route = group_offset_commit_route(group)?;
        self.driver
            .request_tracked_with(
                route,
                request,
                group_offset_commit_options(
                    deadline,
                    requires_leader_epoch,
                    static_membership,
                    consumer_group_protocol,
                ),
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
    consumer_group_protocol: bool,
) -> RequestOptions {
    let options = RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_maximum_version(GROUP_OFFSET_COMMIT_MAX_VERSION);
    if consumer_group_protocol {
        options.with_minimum_version(GROUP_OFFSET_COMMIT_CONSUMER_GROUP_VERSION)
    } else if static_membership {
        options.with_minimum_version(GROUP_OFFSET_COMMIT_STATIC_MEMBERSHIP_VERSION)
    } else if requires_leader_epoch {
        options.with_minimum_version(GROUP_OFFSET_COMMIT_MIN_LEADER_EPOCH_VERSION)
    } else {
        options
    }
}
