//! Exact unsettled accounting and deadline observation for classic membership.

use kafka_client_core::ClassicGroupPhase;

use crate::driver::classic_group::{
    ClassicCoordinatorInvalidationShutdownRecovery, ClassicCoordinatorInvalidations,
    TrackedClassicHeartbeatCalls, TrackedJoinGroupCalls, TrackedSyncGroupCalls,
};

use super::{
    classic_group_recovery::recovery_unsettled_count,
    registry::GroupConsumerRegistry,
    registry_entry::{GroupConsumerEntry, GroupConsumerEntryState},
};

impl GroupConsumerRegistry {
    pub(super) fn membership_unsettled(&self) -> usize {
        let entries: usize = self
            .entries
            .iter()
            .map(GroupConsumerEntry::membership_unsettled)
            .sum();
        let joins = self
            .join_calls
            .as_ref()
            .map_or(0, TrackedJoinGroupCalls::retained_join_group_count);
        let syncs = self
            .sync_calls
            .as_ref()
            .map_or(0, TrackedSyncGroupCalls::retained_sync_group_count);
        let heartbeats = self.heartbeat_calls.as_ref().map_or(
            0,
            TrackedClassicHeartbeatCalls::retained_classic_heartbeat_count,
        );
        let invalidations = self
            .coordinator_invalidations
            .as_ref()
            .map_or(0, ClassicCoordinatorInvalidations::retained_count);
        let invalidation_recovery = self
            .coordinator_invalidation_shutdown_recovery
            .as_ref()
            .map_or(
                0,
                ClassicCoordinatorInvalidationShutdownRecovery::retained_count,
            );
        let recovery = recovery_unsettled_count(
            self.heartbeat_shutdown_recovery.as_ref(),
            self.join_shutdown_recovery.as_ref(),
            self.sync_shutdown_recovery.as_ref(),
            self.heartbeat_recovery_fault.as_ref(),
            self.join_recovery_fault.as_ref(),
            self.sync_recovery_fault.as_ref(),
        );
        entries
            .saturating_add(joins)
            .saturating_add(syncs)
            .saturating_add(heartbeats)
            .saturating_add(invalidations)
            .saturating_add(invalidation_recovery)
            .saturating_add(recovery)
    }

    pub(super) fn membership_next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        self.entries
            .iter()
            .filter_map(|entry| {
                let route_blocked = entry.rediscovery.blocks_join()
                    || self
                        .coordinator_invalidations
                        .as_ref()
                        .is_some_and(|owner| owner.blocks_join(entry.group_id()));
                [
                    entry.execution.next_deadline(),
                    entry.heartbeat.next_deadline(),
                    if route_blocked {
                        None
                    } else {
                        entry.rejoin.next_deadline()
                    },
                ]
                .into_iter()
                .flatten()
                .min()
            })
            .min()
    }
}

impl GroupConsumerEntry {
    fn membership_unsettled(&self) -> usize {
        if let Some(fault) = &self.fault {
            return fault
                .retained_owner_count()
                .saturating_add(self.execution.unsettled())
                .saturating_add(self.heartbeat.unsettled())
                .saturating_add(self.rejoin.unsettled())
                .saturating_add(self.rediscovery.unsettled());
        }
        if self.state == GroupConsumerEntryState::Closing
            && self.classic.machine().phase() != ClassicGroupPhase::Closed
        {
            return 1usize
                .saturating_add(self.heartbeat.unsettled())
                .saturating_add(self.rejoin.unsettled())
                .saturating_add(self.rediscovery.unsettled());
        }
        self.execution
            .unsettled()
            .saturating_add(self.heartbeat.unsettled())
            .saturating_add(self.rejoin.unsettled())
            .saturating_add(self.rediscovery.unsettled())
    }
}
