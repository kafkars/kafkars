//! Exact unsettled accounting and deadline observation for classic membership.

use kafka_client_core::ClassicGroupPhase;

use crate::driver::classic_group::{
    ClassicCoordinatorInvalidationShutdownRecovery, ClassicCoordinatorInvalidations,
    TrackedClassicHeartbeatCalls, TrackedJoinGroupCalls, TrackedSyncGroupCalls,
};
use crate::driver::{
    GroupPositionOffsetFetchShutdownRecovery, TrackedGroupPositionOffsetFetchCalls,
};

use super::{
    classic_group_position::ClassicGroupPositionRecoveryFault,
    classic_group_recovery::recovery_unsettled_count,
    registry::GroupConsumerRegistry,
    registry_entry::{GroupConsumerEntry, GroupConsumerEntryState},
};

impl GroupConsumerRegistry {
    pub(super) fn position_unsettled(&self) -> usize {
        let entries = self
            .entries
            .iter()
            .map(|entry| entry.position.unsettled())
            .sum::<usize>();
        entries
            .saturating_add(self.position_calls.as_ref().map_or(
                0,
                TrackedGroupPositionOffsetFetchCalls::retained_group_position_offset_fetch_count,
            ))
            .saturating_add(
                self.position_shutdown_recovery
                    .as_ref()
                    .map_or(0, GroupPositionOffsetFetchShutdownRecovery::retained_count),
            )
            .saturating_add(
                self.position_recovery_fault
                    .as_ref()
                    .map_or(0, ClassicGroupPositionRecoveryFault::retained_owner_count),
            )
    }

    pub(super) fn position_next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        self.entries
            .iter()
            .filter(|entry| {
                entry.fault.is_none() || entry.state == GroupConsumerEntryState::Closing
            })
            .filter_map(|entry| entry.position.next_deadline())
            .min()
    }

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
            .filter(|entry| {
                entry.fault.is_none() || entry.state == GroupConsumerEntryState::Closing
            })
            .filter_map(|entry| {
                if let Some(consumer) = entry.consumer.as_ref() {
                    return [consumer.next_deadline(), entry.leave.next_deadline()]
                        .into_iter()
                        .flatten()
                        .min();
                }
                let route_blocked = entry.rediscovery.blocks_join()
                    || self
                        .coordinator_invalidations
                        .as_ref()
                        .is_some_and(|owner| owner.blocks_join(entry.group_id()));
                let rejoin_blocked = route_blocked || entry.classic_reconciliation.is_some();
                [
                    entry.execution.next_deadline(),
                    entry.heartbeat.next_deadline(),
                    entry.leave.next_deadline(),
                    if rejoin_blocked {
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
        let classic_reconciliation = usize::from(self.classic_reconciliation.is_some());
        if let Some(consumer) = self.consumer.as_ref() {
            let closing_membership = usize::from(
                self.state == GroupConsumerEntryState::Closing
                    && consumer.machine().phase()
                        != kafka_client_core::ConsumerGroupHeartbeatPhase::Closed,
            );
            return consumer
                .unsettled()
                .max(closing_membership)
                .saturating_add(usize::from(self.consumer_revocation.is_some()))
                .saturating_add(usize::from(self.consumer_reconciliation.is_some()))
                .saturating_add(classic_reconciliation);
        }
        if let Some(fault) = &self.fault {
            return fault
                .retained_owner_count()
                .saturating_add(self.execution.unsettled())
                .saturating_add(self.heartbeat.unsettled())
                .saturating_add(self.leave.unsettled())
                .saturating_add(self.rejoin.unsettled())
                .saturating_add(self.rediscovery.unsettled())
                .saturating_add(classic_reconciliation);
        }
        if self.state == GroupConsumerEntryState::Closing
            && self.classic.machine().phase() != ClassicGroupPhase::Closed
        {
            return 1usize
                .saturating_add(self.heartbeat.unsettled())
                .saturating_add(self.leave.unsettled())
                .saturating_add(self.rejoin.unsettled())
                .saturating_add(self.rediscovery.unsettled())
                .saturating_add(classic_reconciliation);
        }
        self.execution
            .unsettled()
            .saturating_add(self.heartbeat.unsettled())
            .saturating_add(self.leave.unsettled())
            .saturating_add(self.rejoin.unsettled())
            .saturating_add(self.rediscovery.unsettled())
            .saturating_add(classic_reconciliation)
    }
}
