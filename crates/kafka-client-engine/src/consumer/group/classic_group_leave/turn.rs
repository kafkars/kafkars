//! One-at-a-time registry scheduling for explicit-close broker departure.

use std::sync::Arc;

use kafka_client_core::Moment;

use crate::driver::DriverOwner;

use super::owner::ClassicGroupLeaveOwnerTurn;
use crate::consumer::group::{
    classic_group_join::ClassicGroupExecutionState,
    registry::GroupConsumerRegistry,
    registry_entry::{GroupConsumerEntry, GroupConsumerEntryState},
};

/// Aggregate registry turn for broker-side classic-group departure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupLeaveTurn {
    Idle,
    Progress,
    Blocked,
}

impl GroupConsumerRegistry {
    pub(in crate::consumer::group) fn turn_one_classic_group_leave(
        &mut self,
        now: Moment,
        driver: &DriverOwner,
    ) -> ClassicGroupLeaveTurn {
        let mut blocked = false;
        for index in 0..self.entries.len() {
            let entry = &mut self.entries[index];
            if entry.state != GroupConsumerEntryState::Closing || entry.leave.allows_local_close() {
                continue;
            }
            let group = Arc::clone(entry.catalog.group());
            let group_instance_id = entry.catalog.group_instance_id().cloned();
            let member = known_member(entry);
            let membership_call_pending = membership_call_pending(entry);
            let turn = entry.leave.turn_owned_with_instance(
                now,
                group,
                member,
                group_instance_id,
                membership_call_pending,
                driver,
            );
            match turn {
                ClassicGroupLeaveOwnerTurn::Progress => {
                    return ClassicGroupLeaveTurn::Progress;
                }
                ClassicGroupLeaveOwnerTurn::Blocked => blocked = true,
                ClassicGroupLeaveOwnerTurn::Idle => {}
                ClassicGroupLeaveOwnerTurn::Rediscover { route, fallback } => {
                    let group_id = self.entries[index].group_id();
                    let (entries, invalidations) =
                        (&mut self.entries, &mut self.coordinator_invalidations);
                    let Some(invalidations) = invalidations.as_mut() else {
                        route.accept();
                        let _accepted = entries[index].leave.reject_rediscovery_transfer(fallback);
                        return ClassicGroupLeaveTurn::Progress;
                    };
                    let permit = match invalidations.try_reserve(group_id) {
                        Ok(permit) => permit,
                        Err(_failure) => {
                            route.accept();
                            let _accepted =
                                entries[index].leave.reject_rediscovery_transfer(fallback);
                            return ClassicGroupLeaveTurn::Progress;
                        }
                    };
                    let pending = match route.into_coordinator_invalidation(group_id) {
                        Ok(pending) => pending,
                        Err(route) => {
                            drop(permit);
                            route.accept();
                            let _accepted =
                                entries[index].leave.reject_rediscovery_transfer(fallback);
                            return ClassicGroupLeaveTurn::Progress;
                        }
                    };
                    if let Err(failure) = permit.install(pending) {
                        failure.discard();
                        let _accepted = entries[index].leave.reject_rediscovery_transfer(fallback);
                        return ClassicGroupLeaveTurn::Progress;
                    }
                    let _accepted = entries[index].leave.confirm_rediscovery_transfer();
                    return ClassicGroupLeaveTurn::Progress;
                }
            }
        }
        if blocked {
            ClassicGroupLeaveTurn::Blocked
        } else {
            ClassicGroupLeaveTurn::Idle
        }
    }

    pub(in crate::consumer::group) fn recover_classic_group_leaves_after_driver_shutdown(
        &mut self,
    ) {
        for entry in &mut self.entries {
            entry.leave.recover_after_driver_shutdown();
        }
    }
}

pub(in crate::consumer::group) fn resolve_local_leave_without_member(
    entry: &mut GroupConsumerEntry,
) -> bool {
    if known_member(entry).is_some() || membership_call_pending(entry) {
        return false;
    }
    entry.leave.resolve_no_member()
}

fn known_member(entry: &GroupConsumerEntry) -> Option<Arc<str>> {
    entry.catalog.current_member().cloned().or_else(|| {
        entry
            .classic
            .pending()
            .map(|pending| Arc::clone(pending.local_member()))
    })
}

fn membership_call_pending(entry: &GroupConsumerEntry) -> bool {
    let state = entry.execution.borrow_execution_state();
    matches!(
        state,
        ClassicGroupExecutionState::JoinHandoff(_)
            | ClassicGroupExecutionState::JoinDriverOwned(_)
            | ClassicGroupExecutionState::JoinConfirmationPending { .. }
            | ClassicGroupExecutionState::PartitionCountHandoff { .. }
            | ClassicGroupExecutionState::PartitionCountDriverOwned { .. }
            | ClassicGroupExecutionState::PartitionCountCompletionFault { .. }
            | ClassicGroupExecutionState::SyncHandoff(_)
            | ClassicGroupExecutionState::SyncDriverOwned(_)
            | ClassicGroupExecutionState::SyncConfirmationPending(_)
    )
}
