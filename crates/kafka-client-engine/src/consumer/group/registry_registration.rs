//! Bounded classic-group registry registration and lossless rejection ownership.

use std::sync::Arc;

use kafka_client_core::{
    ClassicGroupTiming, ClassicHeartbeatPolicy, ClassicProcessingLeasePolicy, ClassicRejoinPolicy,
    GroupId, ReadIsolation,
};

use super::{
    classic_group_fetch::ClassicGroupFetchBuildError,
    registry::{
        GROUP_CONSUMER_CAPACITY, GROUP_CONSUMER_RETAINED_NAME_BYTES, GroupConsumerRegistry,
    },
    registry_entry::{
        GroupConsumerEntry, GroupConsumerEntryBuildError, default_classic_processing_lease_policy,
    },
    session_catalog::GroupSessionCatalogError,
};

/// Registration rejection retaining the exact caller-owned group spelling.
#[must_use = "group registration rejection retains the caller group spelling"]
pub(super) struct GroupConsumerRegistrationFailure {
    pub(super) kind: GroupConsumerRegistrationFailureKind,
    pub(super) group: Arc<str>,
    pub(super) group_instance_id: Option<Arc<str>>,
    pub(super) local_topics: Vec<Arc<str>>,
}

/// Bounded local reason a group catalog could not be registered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerRegistrationFailureKind {
    Closed,
    Capacity,
    RetainedBytes,
    IdentityExhausted,
    Catalog(GroupSessionCatalogError),
    Fetch(ClassicGroupFetchBuildError),
}

impl GroupConsumerRegistry {
    pub(super) fn try_register(
        &mut self,
        group: Arc<str>,
        local_topics: Vec<Arc<str>>,
        timing: ClassicGroupTiming,
        heartbeat_policy: ClassicHeartbeatPolicy,
        rejoin_policy: ClassicRejoinPolicy,
    ) -> Result<GroupId, GroupConsumerRegistrationFailure> {
        self.try_register_with_processing_policy(
            group,
            local_topics,
            timing,
            heartbeat_policy,
            rejoin_policy,
            default_classic_processing_lease_policy(),
        )
    }

    pub(super) fn try_register_with_processing_policy(
        &mut self,
        group: Arc<str>,
        local_topics: Vec<Arc<str>>,
        timing: ClassicGroupTiming,
        heartbeat_policy: ClassicHeartbeatPolicy,
        rejoin_policy: ClassicRejoinPolicy,
        processing_policy: ClassicProcessingLeasePolicy,
    ) -> Result<GroupId, GroupConsumerRegistrationFailure> {
        self.try_register_with_configuration(
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
        reason = "one bounded registration forwards one explicit immutable identity and policy set"
    )]
    pub(super) fn try_register_with_configuration(
        &mut self,
        group: Arc<str>,
        group_instance_id: Option<Arc<str>>,
        local_topics: Vec<Arc<str>>,
        timing: ClassicGroupTiming,
        heartbeat_policy: ClassicHeartbeatPolicy,
        rejoin_policy: ClassicRejoinPolicy,
        read_isolation: ReadIsolation,
        processing_policy: ClassicProcessingLeasePolicy,
    ) -> Result<GroupId, GroupConsumerRegistrationFailure> {
        if !self.accepting {
            return Err(registration_failure(
                GroupConsumerRegistrationFailureKind::Closed,
                group,
                group_instance_id,
                local_topics,
            ));
        }
        if self.entries.len() == GROUP_CONSUMER_CAPACITY {
            return Err(registration_failure(
                GroupConsumerRegistrationFailureKind::Capacity,
                group,
                group_instance_id,
                local_topics,
            ));
        }
        let Some(group_id) = self.next_group_id else {
            return Err(registration_failure(
                GroupConsumerRegistrationFailureKind::IdentityExhausted,
                group,
                group_instance_id,
                local_topics,
            ));
        };
        let entry = match GroupConsumerEntry::try_new_with_configuration(
            group_id,
            &group,
            group_instance_id.as_ref(),
            &local_topics,
            timing,
            heartbeat_policy,
            rejoin_policy,
            read_isolation,
            processing_policy,
        ) {
            Ok(entry) => entry,
            Err(GroupConsumerEntryBuildError::Catalog(error)) => {
                return Err(registration_failure(
                    GroupConsumerRegistrationFailureKind::Catalog(error),
                    group,
                    group_instance_id,
                    local_topics,
                ));
            }
            Err(GroupConsumerEntryBuildError::Fetch(error)) => {
                return Err(registration_failure(
                    GroupConsumerRegistrationFailureKind::Fetch(error),
                    group,
                    group_instance_id,
                    local_topics,
                ));
            }
        };
        let next_bytes = match self.retained_group_bytes.checked_add(entry.group_bytes()) {
            Some(bytes) if bytes <= GROUP_CONSUMER_RETAINED_NAME_BYTES => bytes,
            _ => {
                return Err(registration_failure(
                    GroupConsumerRegistrationFailureKind::RetainedBytes,
                    group,
                    group_instance_id,
                    local_topics,
                ));
            }
        };
        self.next_group_id = group_id
            .get()
            .checked_add(1)
            .and_then(GroupId::try_from_raw);
        self.retained_group_bytes = next_bytes;
        self.entries.push(entry);
        Ok(group_id)
    }
}

fn registration_failure(
    kind: GroupConsumerRegistrationFailureKind,
    group: Arc<str>,
    group_instance_id: Option<Arc<str>>,
    local_topics: Vec<Arc<str>>,
) -> GroupConsumerRegistrationFailure {
    GroupConsumerRegistrationFailure {
        kind,
        group,
        group_instance_id,
        local_topics,
    }
}
