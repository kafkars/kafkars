//! One linear catalog entry and its admission lifecycle.

use std::sync::Arc;

use kafka_client_core::{
    ClassicGroupTiming, ClassicHeartbeatPolicy, ClassicProcessingLease,
    ClassicProcessingLeasePolicy, ClassicRejoinPolicy, GroupId, GroupPositionMissingOffsetPolicy,
    ReadIsolation,
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
    consumer_group_execution::{ConsumerGroupExecution, ConsumerGroupExecutionBuildError},
    session_catalog::{GroupSessionCatalog, GroupSessionCatalogError},
};
use crate::consumer::{
    GroupConsumerPositionFailureKind, group_registration_request::GroupConsumerProtocol,
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
    Consumer(ConsumerGroupExecutionBuildError),
}

/// One bounded group spelling, session catalog, and close fence.
pub(super) struct GroupConsumerEntry {
    pub(super) state: GroupConsumerEntryState,
    pub(super) protocol: GroupConsumerProtocol,
    pub(super) catalog: GroupSessionCatalog,
    pub(super) consumer: Option<ConsumerGroupExecution>,
    pub(super) classic: ClassicGroupOwner,
    pub(super) execution: ClassicGroupExecution,
    pub(super) fetch: ClassicGroupFetchOwner,
    pub(super) heartbeat: ClassicHeartbeatExecution,
    pub(super) leave: ClassicGroupLeaveOwner,
    pub(super) missing_offset_policy: GroupPositionMissingOffsetPolicy,
    pub(super) read_isolation: ReadIsolation,
    pub(super) position: ClassicGroupPositionExecution,
    pub(super) position_failure_observation: Option<GroupConsumerPositionFailureKind>,
    pub(super) processing_lease: ClassicProcessingLease,
    pub(super) rejoin: ClassicGroupRejoinExecution,
    pub(super) rediscovery: ClassicCoordinatorRediscovery,
    pub(super) revocation: ClassicGroupRevocationOwner,
    pub(super) fault: Option<ClassicGroupEntryFault>,
}

impl GroupConsumerEntry {
    pub(super) fn retain_position_failure_observation(
        &mut self,
        failure: GroupConsumerPositionFailureKind,
    ) {
        self.position_failure_observation = Some(failure);
    }

    pub(super) fn take_position_failure_observation(
        &mut self,
    ) -> Option<GroupConsumerPositionFailureKind> {
        self.position_failure_observation.take()
    }

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
            GroupPositionMissingOffsetPolicy::Error,
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
        missing_offset_policy: GroupPositionMissingOffsetPolicy,
        read_isolation: ReadIsolation,
        processing_policy: ClassicProcessingLeasePolicy,
    ) -> Result<Self, GroupConsumerEntryBuildError> {
        Self::try_new_with_protocol_configuration(
            group_id,
            group,
            group_instance_id,
            local_topics,
            GroupConsumerProtocol::Classic,
            timing,
            heartbeat_policy,
            rejoin_policy,
            missing_offset_policy,
            read_isolation,
            processing_policy,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one bounded entry receives one explicit immutable protocol and policy set"
    )]
    pub(super) fn try_new_with_protocol_configuration(
        group_id: GroupId,
        group: &Arc<str>,
        group_instance_id: Option<&Arc<str>>,
        local_topics: &[Arc<str>],
        protocol: GroupConsumerProtocol,
        timing: ClassicGroupTiming,
        heartbeat_policy: ClassicHeartbeatPolicy,
        rejoin_policy: ClassicRejoinPolicy,
        missing_offset_policy: GroupPositionMissingOffsetPolicy,
        read_isolation: ReadIsolation,
        processing_policy: ClassicProcessingLeasePolicy,
    ) -> Result<Self, GroupConsumerEntryBuildError> {
        Ok(Self {
            state: GroupConsumerEntryState::Active,
            protocol,
            catalog: GroupSessionCatalog::try_new_with_group_instance_id(
                group_id,
                Arc::clone(group),
                group_instance_id.cloned(),
                local_topics,
            )
            .map_err(GroupConsumerEntryBuildError::Catalog)?,
            consumer: if protocol == GroupConsumerProtocol::Consumer {
                Some(
                    ConsumerGroupExecution::try_new(
                        group_id,
                        local_topics.len(),
                        u32::try_from(timing.rebalance_timeout_ms()).unwrap_or_else(|_error| {
                            unreachable!("validated rebalance timeout is positive")
                        }),
                    )
                    .map_err(GroupConsumerEntryBuildError::Consumer)?,
                )
            } else {
                None
            },
            classic: ClassicGroupOwner::new(group_id, timing, heartbeat_policy, rejoin_policy),
            execution: new_classic_group_execution(),
            fetch: ClassicGroupFetchOwner::try_new_with_read_isolation(read_isolation)
                .map_err(GroupConsumerEntryBuildError::Fetch)?,
            heartbeat: ClassicHeartbeatExecution::new(),
            leave: ClassicGroupLeaveOwner::new(),
            missing_offset_policy,
            read_isolation,
            position: ClassicGroupPositionExecution::new(),
            position_failure_observation: None,
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

    pub(super) const fn uses_consumer_group_protocol(&self) -> bool {
        matches!(self.protocol, GroupConsumerProtocol::Consumer)
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
