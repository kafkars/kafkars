//! Sole bounded registry owner for private classic-group consumers.

use kafka_client_core::GroupId;

use crate::consumer::group_recv::GroupConsumerRecvNotificationResources;
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
    classic_group_fetch::ClassicGroupFetchShutdownRecovery,
    classic_group_position::ClassicGroupPositionRecoveryFault,
    offset_commit::GroupOffsetCommitHost, registry_entry::GroupConsumerEntry,
    session_catalog::MAX_KAFKA_GROUP_STRING_BYTES,
};

pub(super) use super::registry_registration::GroupConsumerRegistrationFailureKind;

pub(super) const GROUP_CONSUMER_CAPACITY: usize = 8;
pub(super) const GROUP_CONSUMER_RETAINED_NAME_BYTES: usize =
    GROUP_CONSUMER_CAPACITY * 2 * MAX_KAFKA_GROUP_STRING_BYTES;

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
    pub(super) fetch_shutdown_recoveries: Vec<(GroupId, ClassicGroupFetchShutdownRecovery)>,
    pub(super) offset_commits: GroupOffsetCommitHost,
    pub(super) recv_notifications: Option<GroupConsumerRecvNotificationResources>,
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
        let mut fetch_shutdown_recoveries = Vec::new();
        fetch_shutdown_recoveries
            .try_reserve_exact(GROUP_CONSUMER_CAPACITY)
            .map_err(|_error| std::io::Error::other("group Fetch recovery reservation failed"))?;
        let mut offset_commits = GroupOffsetCommitHost::start_group_offset_commit_host()?;
        let recv_notifications = match GroupConsumerRecvNotificationResources::start() {
            Ok(resources) => resources,
            Err(error) => {
                if let Some(join) = offset_commits.take_notifier() {
                    let _result = join.join();
                }
                return Err(error);
            }
        };
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
            fetch_shutdown_recoveries,
            offset_commits,
            recv_notifications: Some(recv_notifications),
        })
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
}
