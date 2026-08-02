//! Capture-independent port registration with lossless caller-name rejection.

use std::sync::Arc;

use kafka_client_core::{
    ClassicGroupTiming, ClassicHeartbeatPolicy, ClassicProcessingLeasePolicy, ClassicRejoinPolicy,
    GroupId, GroupPositionMissingOffsetPolicy, ReadIsolation,
};

use super::{
    classic_group_leave::GroupConsumerCloseAuthority,
    registry::GroupConsumerRegistrationFailureKind,
    registry_entry::default_classic_processing_lease_policy, registry_port::GroupConsumerPort,
    registry_shard::GroupConsumerShardLockError, session_catalog::GroupSessionCatalogError,
};
use crate::consumer::group_registration_request::GroupConsumerProtocol;

pub(in crate::consumer) struct GroupConsumerPortRegistrationAccepted {
    pub(in crate::consumer) group_id: GroupId,
    pub(in crate::consumer) close_authority: Arc<GroupConsumerCloseAuthority>,
}

impl GroupConsumerPort {
    pub(crate) fn try_register(
        &self,
        group: Arc<str>,
        local_topics: Vec<Arc<str>>,
        timing: ClassicGroupTiming,
        heartbeat_policy: ClassicHeartbeatPolicy,
        rejoin_policy: ClassicRejoinPolicy,
    ) -> Result<GroupId, GroupConsumerPortRegistrationFailure> {
        self.try_register_with_processing_policy(
            group,
            local_topics,
            timing,
            heartbeat_policy,
            rejoin_policy,
            default_classic_processing_lease_policy(),
        )
    }

    pub(crate) fn try_register_with_processing_policy(
        &self,
        group: Arc<str>,
        local_topics: Vec<Arc<str>>,
        timing: ClassicGroupTiming,
        heartbeat_policy: ClassicHeartbeatPolicy,
        rejoin_policy: ClassicRejoinPolicy,
        processing_policy: ClassicProcessingLeasePolicy,
    ) -> Result<GroupId, GroupConsumerPortRegistrationFailure> {
        self.try_register_with_configuration(
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
        reason = "one bounded port admission forwards one explicit immutable identity and policy set"
    )]
    pub(crate) fn try_register_with_configuration(
        &self,
        group: Arc<str>,
        group_instance_id: Option<Arc<str>>,
        local_topics: Vec<Arc<str>>,
        timing: ClassicGroupTiming,
        heartbeat_policy: ClassicHeartbeatPolicy,
        rejoin_policy: ClassicRejoinPolicy,
        missing_offset_policy: GroupPositionMissingOffsetPolicy,
        read_isolation: ReadIsolation,
        processing_policy: ClassicProcessingLeasePolicy,
    ) -> Result<GroupId, GroupConsumerPortRegistrationFailure> {
        self.try_register_with_protocol_configuration(
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
        reason = "one bounded port admission forwards one explicit immutable protocol and policy set"
    )]
    pub(crate) fn try_register_with_protocol_configuration(
        &self,
        group: Arc<str>,
        group_instance_id: Option<Arc<str>>,
        local_topics: Vec<Arc<str>>,
        protocol: GroupConsumerProtocol,
        timing: ClassicGroupTiming,
        heartbeat_policy: ClassicHeartbeatPolicy,
        rejoin_policy: ClassicRejoinPolicy,
        missing_offset_policy: GroupPositionMissingOffsetPolicy,
        read_isolation: ReadIsolation,
        processing_policy: ClassicProcessingLeasePolicy,
    ) -> Result<GroupId, GroupConsumerPortRegistrationFailure> {
        self.try_register_controlled(
            group,
            group_instance_id,
            local_topics,
            protocol,
            timing,
            heartbeat_policy,
            rejoin_policy,
            missing_offset_policy,
            read_isolation,
            processing_policy,
        )
        .map(|accepted| accepted.group_id)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one exact bounded group registration and close-control authority"
    )]
    pub(in crate::consumer) fn try_register_controlled(
        &self,
        group: Arc<str>,
        group_instance_id: Option<Arc<str>>,
        local_topics: Vec<Arc<str>>,
        protocol: GroupConsumerProtocol,
        timing: ClassicGroupTiming,
        heartbeat_policy: ClassicHeartbeatPolicy,
        rejoin_policy: ClassicRejoinPolicy,
        missing_offset_policy: GroupPositionMissingOffsetPolicy,
        read_isolation: ReadIsolation,
        processing_policy: ClassicProcessingLeasePolicy,
    ) -> Result<GroupConsumerPortRegistrationAccepted, GroupConsumerPortRegistrationFailure> {
        if self.shared.admission_is_closed() {
            return Err(registration_failure(
                GroupConsumerPortRegistrationFailureKind::CLOSED,
                group,
                group_instance_id,
                local_topics,
            ));
        }
        let mut registry = match self.shared.try_registry() {
            Ok(registry) => registry,
            Err(error) => {
                return Err(registration_failure(
                    GroupConsumerPortRegistrationFailureKind::lock(error),
                    group,
                    group_instance_id,
                    local_topics,
                ));
            }
        };
        if self.shared.admission_is_closed() {
            return Err(registration_failure(
                GroupConsumerPortRegistrationFailureKind::CLOSED,
                group,
                group_instance_id,
                local_topics,
            ));
        }
        let group_id = registry
            .try_register_with_protocol_configuration(
                group,
                group_instance_id,
                local_topics,
                protocol,
                timing,
                heartbeat_policy,
                rejoin_policy,
                missing_offset_policy,
                read_isolation,
                processing_policy,
            )
            .map_err(|failure| GroupConsumerPortRegistrationFailure {
                kind: GroupConsumerPortRegistrationFailureKind::registry(failure.kind),
                group: failure.group,
                group_instance_id: failure.group_instance_id,
                local_topics: failure.local_topics,
            })?;
        let close_authority = registry
            .entry(group_id)
            .unwrap_or_else(|| unreachable!("accepted group remains in its registry"))
            .close_authority();
        Ok(GroupConsumerPortRegistrationAccepted {
            group_id,
            close_authority,
        })
    }
}

#[must_use = "registration rejection retains the exact caller-owned names"]
pub(crate) struct GroupConsumerPortRegistrationFailure {
    pub(crate) kind: GroupConsumerPortRegistrationFailureKind,
    pub(crate) group: Arc<str>,
    pub(crate) group_instance_id: Option<Arc<str>>,
    pub(crate) local_topics: Vec<Arc<str>>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct GroupConsumerPortRegistrationFailureKind {
    kind: GroupConsumerPortRegistrationFailureReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupConsumerPortRegistrationCategory {
    Closed,
    Contended,
    Backpressure,
    InvalidInput,
    InternalInvariant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GroupConsumerPortRegistrationFailureReason {
    Closed,
    Lock(GroupConsumerShardLockError),
    Registry(GroupConsumerRegistrationFailureKind),
}

impl GroupConsumerPortRegistrationFailureKind {
    pub(crate) const CLOSED: Self = Self {
        kind: GroupConsumerPortRegistrationFailureReason::Closed,
    };

    const fn lock(error: GroupConsumerShardLockError) -> Self {
        Self {
            kind: GroupConsumerPortRegistrationFailureReason::Lock(error),
        }
    }

    const fn registry(error: GroupConsumerRegistrationFailureKind) -> Self {
        Self {
            kind: GroupConsumerPortRegistrationFailureReason::Registry(error),
        }
    }

    pub(crate) const fn public_category(self) -> GroupConsumerPortRegistrationCategory {
        match self.kind {
            GroupConsumerPortRegistrationFailureReason::Closed
            | GroupConsumerPortRegistrationFailureReason::Registry(
                GroupConsumerRegistrationFailureKind::Closed,
            ) => GroupConsumerPortRegistrationCategory::Closed,
            GroupConsumerPortRegistrationFailureReason::Registry(
                GroupConsumerRegistrationFailureKind::Capacity
                | GroupConsumerRegistrationFailureKind::RetainedBytes,
            ) => GroupConsumerPortRegistrationCategory::Backpressure,
            GroupConsumerPortRegistrationFailureReason::Lock(
                GroupConsumerShardLockError::Contended,
            ) => GroupConsumerPortRegistrationCategory::Contended,
            GroupConsumerPortRegistrationFailureReason::Registry(
                GroupConsumerRegistrationFailureKind::Catalog(
                    GroupSessionCatalogError::EmptyGroup
                    | GroupSessionCatalogError::GroupBytes { .. }
                    | GroupSessionCatalogError::EmptyGroupInstance
                    | GroupSessionCatalogError::GroupInstanceBytes { .. }
                    | GroupSessionCatalogError::EmptyTopic
                    | GroupSessionCatalogError::TopicBytes { .. }
                    | GroupSessionCatalogError::RetainedTopicCapacity { .. }
                    | GroupSessionCatalogError::RetainedTopicBytes { .. }
                    | GroupSessionCatalogError::DuplicateTopic,
                ),
            ) => GroupConsumerPortRegistrationCategory::InvalidInput,
            GroupConsumerPortRegistrationFailureReason::Lock(
                GroupConsumerShardLockError::Poisoned,
            )
            | GroupConsumerPortRegistrationFailureReason::Registry(
                GroupConsumerRegistrationFailureKind::IdentityExhausted
                | GroupConsumerRegistrationFailureKind::Fetch(_)
                | GroupConsumerRegistrationFailureKind::Consumer(_)
                | GroupConsumerRegistrationFailureKind::Catalog(
                    GroupSessionCatalogError::EmptyMember
                    | GroupSessionCatalogError::MemberBytes { .. }
                    | GroupSessionCatalogError::RetainedTopicBytesOverflow
                    | GroupSessionCatalogError::TopicIdentityExhausted
                    | GroupSessionCatalogError::Allocation
                    | GroupSessionCatalogError::UnknownTopic(_)
                    | GroupSessionCatalogError::MemberMismatch
                    | GroupSessionCatalogError::SessionProtocolMismatch,
                ),
            ) => GroupConsumerPortRegistrationCategory::InternalInvariant,
        }
    }
}

impl core::fmt::Debug for GroupConsumerPortRegistrationFailureKind {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.kind {
            GroupConsumerPortRegistrationFailureReason::Closed => formatter.write_str("Closed"),
            GroupConsumerPortRegistrationFailureReason::Lock(error) => {
                formatter.debug_tuple("Lock").field(&error).finish()
            }
            GroupConsumerPortRegistrationFailureReason::Registry(error) => {
                formatter.debug_tuple("Registry").field(&error).finish()
            }
        }
    }
}

fn registration_failure(
    kind: GroupConsumerPortRegistrationFailureKind,
    group: Arc<str>,
    group_instance_id: Option<Arc<str>>,
    local_topics: Vec<Arc<str>>,
) -> GroupConsumerPortRegistrationFailure {
    GroupConsumerPortRegistrationFailure {
        kind,
        group,
        group_instance_id,
        local_topics,
    }
}
