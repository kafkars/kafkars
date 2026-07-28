//! One linear catalog entry and its admission lifecycle.

use std::sync::Arc;

use kafka_client_core::{
    ClassicGroupTiming, ClassicHeartbeatPolicy, ClassicProcessingLease,
    ClassicProcessingLeasePolicy, ClassicRejoinPolicy, GroupId, ReadIsolation,
};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::{ClassicGroupExecution, new_classic_group_execution},
    classic_group_fetch::{ClassicGroupFetchBuildError, ClassicGroupFetchOwner},
    classic_group_graceful_revocation::ClassicGroupRevocationOwner,
    classic_group_heartbeat::ClassicHeartbeatExecution,
    classic_group_leave::ClassicGroupLeaveOwner,
    classic_group_owner::ClassicGroupOwner,
    classic_group_position::ClassicGroupPositionExecution,
    classic_group_rediscovery::ClassicCoordinatorRediscovery,
    classic_group_rejoin::ClassicGroupRejoinExecution,
    session_catalog::{GroupSessionCatalog, GroupSessionCatalogError},
};

/// Whether one retained group can still admit new operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerEntryState {
    Active,
    Closing,
}

/// Truthful local construction source before one registry entry exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerEntryBuildError {
    Catalog(GroupSessionCatalogError),
    Fetch(ClassicGroupFetchBuildError),
}

/// One bounded group spelling, session catalog, and close fence.
pub(super) struct GroupConsumerEntry {
    pub(super) state: GroupConsumerEntryState,
    pub(super) catalog: GroupSessionCatalog,
    pub(super) classic: ClassicGroupOwner,
    pub(super) execution: ClassicGroupExecution,
    pub(super) fetch: ClassicGroupFetchOwner,
    pub(super) heartbeat: ClassicHeartbeatExecution,
    pub(super) leave: ClassicGroupLeaveOwner,
    pub(super) read_isolation: ReadIsolation,
    pub(super) position: ClassicGroupPositionExecution,
    pub(super) processing_lease: ClassicProcessingLease,
    pub(super) rejoin: ClassicGroupRejoinExecution,
    pub(super) rediscovery: ClassicCoordinatorRediscovery,
    pub(super) revocation: ClassicGroupRevocationOwner,
    pub(super) fault: Option<ClassicGroupEntryFault>,
}

impl GroupConsumerEntry {
    pub(super) fn try_new(
        group_id: GroupId,
        group: &Arc<str>,
        local_topics: &[Arc<str>],
        timing: ClassicGroupTiming,
        heartbeat_policy: ClassicHeartbeatPolicy,
        rejoin_policy: ClassicRejoinPolicy,
    ) -> Result<Self, GroupConsumerEntryBuildError> {
        Self::try_new_with_processing_policy(
            group_id,
            group,
            local_topics,
            timing,
            heartbeat_policy,
            rejoin_policy,
            default_classic_processing_lease_policy(),
        )
    }

    pub(super) fn try_new_with_processing_policy(
        group_id: GroupId,
        group: &Arc<str>,
        local_topics: &[Arc<str>],
        timing: ClassicGroupTiming,
        heartbeat_policy: ClassicHeartbeatPolicy,
        rejoin_policy: ClassicRejoinPolicy,
        processing_policy: ClassicProcessingLeasePolicy,
    ) -> Result<Self, GroupConsumerEntryBuildError> {
        Self::try_new_with_configuration(
            group_id,
            group,
            None,
            local_topics,
            timing,
            heartbeat_policy,
            rejoin_policy,
            ReadIsolation::ReadUncommitted,
            processing_policy,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one bounded entry receives one explicit immutable membership configuration"
    )]
    pub(super) fn try_new_with_configuration(
        group_id: GroupId,
        group: &Arc<str>,
        group_instance_id: Option<&Arc<str>>,
        local_topics: &[Arc<str>],
        timing: ClassicGroupTiming,
        heartbeat_policy: ClassicHeartbeatPolicy,
        rejoin_policy: ClassicRejoinPolicy,
        read_isolation: ReadIsolation,
        processing_policy: ClassicProcessingLeasePolicy,
    ) -> Result<Self, GroupConsumerEntryBuildError> {
        Ok(Self {
            state: GroupConsumerEntryState::Active,
            catalog: GroupSessionCatalog::try_new_with_group_instance_id(
                group_id,
                Arc::clone(group),
                group_instance_id.cloned(),
                local_topics,
            )
            .map_err(GroupConsumerEntryBuildError::Catalog)?,
            classic: ClassicGroupOwner::new(group_id, timing, heartbeat_policy, rejoin_policy),
            execution: new_classic_group_execution(),
            fetch: ClassicGroupFetchOwner::try_new_with_read_isolation(read_isolation)
                .map_err(GroupConsumerEntryBuildError::Fetch)?,
            heartbeat: ClassicHeartbeatExecution::new(),
            leave: ClassicGroupLeaveOwner::new(),
            read_isolation,
            position: ClassicGroupPositionExecution::new(),
            processing_lease: ClassicProcessingLease::new(processing_policy),
            rejoin: ClassicGroupRejoinExecution::new(),
            rediscovery: ClassicCoordinatorRediscovery::new(),
            revocation: ClassicGroupRevocationOwner::new(),
            fault: None,
        })
    }

    pub(super) const fn group_id(&self) -> GroupId {
        self.catalog.group_id()
    }

    pub(super) fn group_bytes(&self) -> usize {
        self.catalog.retained_identity_bytes()
    }

    pub(super) const fn is_active(&self) -> bool {
        matches!(self.state, GroupConsumerEntryState::Active) && self.fault.is_none()
    }
}

/// Fixed private default used by legacy internal registration helpers.
pub(super) const DEFAULT_CLASSIC_PROCESSING_TIMEOUT_TICKS: u64 = 300_000_000_000;

pub(super) fn default_classic_processing_lease_policy() -> ClassicProcessingLeasePolicy {
    match ClassicProcessingLeasePolicy::try_new(DEFAULT_CLASSIC_PROCESSING_TIMEOUT_TICKS) {
        Ok(policy) => policy,
        Err(_error) => unreachable!("the private processing timeout is positive"),
    }
}
