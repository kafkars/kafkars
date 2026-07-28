//! Sole bounded registry owner for private classic-group consumers.

use std::sync::Arc;

use kafka_client_core::{
    ClassicGroupTiming, ClassicHeartbeatPolicy, ClassicProcessingLeasePolicy, ClassicRejoinPolicy,
    GroupId,
};

use crate::driver::classic_group::{
    ClassicCoordinatorInvalidationShutdownRecovery, ClassicCoordinatorInvalidations,
    ClassicHeartbeatShutdownRecovery, JoinGroupShutdownRecovery,
    RecoveredClassicHeartbeatOwnership, RecoveredJoinGroupOwnership, RecoveredSyncGroupOwnership,
    SyncGroupShutdownRecovery, TrackedClassicHeartbeatCalls, TrackedJoinGroupCalls,
    TrackedSyncGroupCalls,
};
use crate::driver::{
    GroupPositionOffsetFetchShutdownRecovery, TrackedGroupPositionOffsetFetchCalls,
};

use super::{
    classic_group_fetch::ClassicGroupFetchBuildError,
    classic_group_position::ClassicGroupPositionRecoveryFault,
    offset_commit::GroupOffsetCommitHost,
    registry_entry::{
        GroupConsumerEntry, GroupConsumerEntryBuildError, default_classic_processing_lease_policy,
    },
    session_catalog::{GroupSessionCatalogError, MAX_KAFKA_GROUP_STRING_BYTES},
};

pub(super) const GROUP_CONSUMER_CAPACITY: usize = 8;
pub(super) const GROUP_CONSUMER_RETAINED_NAME_BYTES: usize =
    GROUP_CONSUMER_CAPACITY * MAX_KAFKA_GROUP_STRING_BYTES;

/// Registration rejection retaining the exact caller-owned group spelling.
#[must_use = "group registration rejection retains the caller group spelling"]
pub(super) struct GroupConsumerRegistrationFailure {
    pub(super) kind: GroupConsumerRegistrationFailureKind,
    pub(super) group: Arc<str>,
    pub(super) local_topics: Vec<Arc<str>>,
}

/// Bounded local reason a group catalog could not be registered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerRegistrationFailureKind {
    Closed,
    EntryFault,
    Capacity,
    RetainedBytes,
    IdentityExhausted,
    Catalog(GroupSessionCatalogError),
    Fetch(ClassicGroupFetchBuildError),
}

/// Many bounded catalogs sharing one global bounded offset-commit host.
pub(crate) struct GroupConsumerRegistry {
    pub(super) entries: Vec<GroupConsumerEntry>,
    pub(super) next_group_id: Option<GroupId>,
    pub(super) retained_group_bytes: usize,
    pub(super) accepting: bool,
    pub(super) join_calls: Option<TrackedJoinGroupCalls>,
    pub(super) sync_calls: Option<TrackedSyncGroupCalls>,
    pub(super) heartbeat_calls: Option<TrackedClassicHeartbeatCalls>,
    pub(super) position_calls: Option<TrackedGroupPositionOffsetFetchCalls>,
    pub(super) coordinator_invalidations: Option<ClassicCoordinatorInvalidations>,
    pub(super) join_shutdown_recovery: Option<JoinGroupShutdownRecovery>,
    pub(super) sync_shutdown_recovery: Option<SyncGroupShutdownRecovery>,
    pub(super) heartbeat_shutdown_recovery: Option<ClassicHeartbeatShutdownRecovery>,
    pub(super) position_shutdown_recovery: Option<GroupPositionOffsetFetchShutdownRecovery>,
    pub(super) coordinator_invalidation_shutdown_recovery:
        Option<ClassicCoordinatorInvalidationShutdownRecovery>,
    pub(super) join_recovery_fault: Option<RecoveredJoinGroupOwnership>,
    pub(super) sync_recovery_fault: Option<RecoveredSyncGroupOwnership>,
    pub(super) heartbeat_recovery_fault: Option<RecoveredClassicHeartbeatOwnership>,
    pub(super) position_recovery_fault: Option<ClassicGroupPositionRecoveryFault>,
    pub(super) offset_commits: GroupOffsetCommitHost,
}

impl GroupConsumerRegistry {
    pub(crate) fn start() -> std::io::Result<Self> {
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(GROUP_CONSUMER_CAPACITY)
            .map_err(|_error| std::io::Error::other("group consumer entry reservation failed"))?;
        let join_calls = TrackedJoinGroupCalls::try_new(GROUP_CONSUMER_CAPACITY)
            .map_err(|_error| std::io::Error::other("JoinGroup call reservation failed"))?;
        let sync_calls = TrackedSyncGroupCalls::try_new(GROUP_CONSUMER_CAPACITY)
            .map_err(|_error| std::io::Error::other("SyncGroup call reservation failed"))?;
        let heartbeat_calls = TrackedClassicHeartbeatCalls::try_new(GROUP_CONSUMER_CAPACITY)
            .map_err(|_error| std::io::Error::other("Heartbeat call reservation failed"))?;
        let position_calls = TrackedGroupPositionOffsetFetchCalls::try_new(GROUP_CONSUMER_CAPACITY)
            .map_err(|_error| std::io::Error::other("group position call reservation failed"))?;
        let coordinator_invalidations = ClassicCoordinatorInvalidations::try_new(
            GROUP_CONSUMER_CAPACITY,
        )
        .map_err(|_error| std::io::Error::other("coordinator invalidation reservation failed"))?;
        Ok(Self {
            entries,
            next_group_id: GroupId::try_from_raw(1),
            retained_group_bytes: 0,
            accepting: true,
            join_calls: Some(join_calls),
            sync_calls: Some(sync_calls),
            heartbeat_calls: Some(heartbeat_calls),
            position_calls: Some(position_calls),
            coordinator_invalidations: Some(coordinator_invalidations),
            join_shutdown_recovery: None,
            sync_shutdown_recovery: None,
            heartbeat_shutdown_recovery: None,
            position_shutdown_recovery: None,
            coordinator_invalidation_shutdown_recovery: None,
            join_recovery_fault: None,
            sync_recovery_fault: None,
            heartbeat_recovery_fault: None,
            position_recovery_fault: None,
            offset_commits: GroupOffsetCommitHost::start_group_offset_commit_host()?,
        })
    }

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
        if !self.accepting {
            return Err(registration_failure(
                GroupConsumerRegistrationFailureKind::Closed,
                group,
                local_topics,
            ));
        }
        if self.has_entry_fault() {
            return Err(registration_failure(
                GroupConsumerRegistrationFailureKind::EntryFault,
                group,
                local_topics,
            ));
        }
        if self.entries.len() == GROUP_CONSUMER_CAPACITY {
            return Err(registration_failure(
                GroupConsumerRegistrationFailureKind::Capacity,
                group,
                local_topics,
            ));
        }
        let Some(group_id) = self.next_group_id else {
            return Err(registration_failure(
                GroupConsumerRegistrationFailureKind::IdentityExhausted,
                group,
                local_topics,
            ));
        };
        let entry = match GroupConsumerEntry::try_new_with_processing_policy(
            group_id,
            &group,
            &local_topics,
            timing,
            heartbeat_policy,
            rejoin_policy,
            processing_policy,
        ) {
            Ok(entry) => entry,
            Err(GroupConsumerEntryBuildError::Catalog(error)) => {
                return Err(registration_failure(
                    GroupConsumerRegistrationFailureKind::Catalog(error),
                    group,
                    local_topics,
                ));
            }
            Err(GroupConsumerEntryBuildError::Fetch(error)) => {
                return Err(registration_failure(
                    GroupConsumerRegistrationFailureKind::Fetch(error),
                    group,
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

    pub(super) fn entry(&self, group_id: GroupId) -> Option<&GroupConsumerEntry> {
        self.entries
            .iter()
            .find(|entry| entry.group_id() == group_id)
    }

    pub(super) fn registered_group_count(&self) -> usize {
        self.entries.len()
    }

    pub(super) const fn retained_group_bytes(&self) -> usize {
        self.retained_group_bytes
    }

    pub(super) fn has_entry_fault(&self) -> bool {
        self.entries.iter().any(|entry| entry.fault.is_some())
    }
}

fn registration_failure(
    kind: GroupConsumerRegistrationFailureKind,
    group: Arc<str>,
    local_topics: Vec<Arc<str>>,
) -> GroupConsumerRegistrationFailure {
    GroupConsumerRegistrationFailure {
        kind,
        group,
        local_topics,
    }
}
