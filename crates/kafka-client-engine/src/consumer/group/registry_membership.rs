//! One bounded per-entry membership timeout or close transition per registry turn.

use kafka_client_core::{ClassicGroupPhase, Moment};

use crate::driver::{
    DriverOwner,
    classic_group::{TrackedJoinGroupCalls, TrackedSyncGroupCalls},
};

use super::{
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_execution_close::ClassicGroupCloseProgress,
    classic_group_join_execution::ClassicGroupJoinSubmissionTurn,
    classic_group_join_settlement::ClassicGroupJoinSettlementTurn,
    classic_group_recovery::recovery_unsettled_count,
    classic_group_sync_settlement::ClassicGroupSyncSettlementTurn,
    classic_group_sync_submission::ClassicGroupSyncSubmissionTurn,
    registry::GroupConsumerRegistry,
    registry_entry::{GroupConsumerEntry, GroupConsumerEntryState},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerMembershipTurn {
    Idle,
    Progress,
    Blocked,
}

impl GroupConsumerRegistry {
    pub(super) fn turn_membership(
        &mut self,
        now: Moment,
        driver: &DriverOwner,
    ) -> Result<GroupConsumerMembershipTurn, ClassicGroupExecutionError> {
        if self.entries.iter().any(|entry| entry.fault.is_some()) {
            return Err(ClassicGroupExecutionError::EntryFault);
        }
        if self.settle_one_classic_sync(now)? == ClassicGroupSyncSettlementTurn::Progress {
            return Ok(GroupConsumerMembershipTurn::Progress);
        }
        let join_blocked = match self.settle_one_classic_join(now)? {
            ClassicGroupJoinSettlementTurn::Progress => {
                return Ok(GroupConsumerMembershipTurn::Progress);
            }
            ClassicGroupJoinSettlementTurn::Blocked => true,
            ClassicGroupJoinSettlementTurn::Idle => false,
        };
        let local = self.turn_local_membership(now)?;
        if local != GroupConsumerMembershipTurn::Idle {
            return Ok(local);
        }
        match self.submit_one_classic_sync(driver)? {
            ClassicGroupSyncSubmissionTurn::Progress => {
                return Ok(GroupConsumerMembershipTurn::Progress);
            }
            ClassicGroupSyncSubmissionTurn::Blocked => {
                return Ok(GroupConsumerMembershipTurn::Blocked);
            }
            ClassicGroupSyncSubmissionTurn::Idle => {}
        }
        Ok(match self.submit_one_classic_join(driver)? {
            ClassicGroupJoinSubmissionTurn::Idle if join_blocked => {
                GroupConsumerMembershipTurn::Blocked
            }
            ClassicGroupJoinSubmissionTurn::Idle => GroupConsumerMembershipTurn::Idle,
            ClassicGroupJoinSubmissionTurn::Progress => GroupConsumerMembershipTurn::Progress,
            ClassicGroupJoinSubmissionTurn::Blocked => GroupConsumerMembershipTurn::Blocked,
        })
    }

    pub(super) fn turn_local_membership(
        &mut self,
        now: Moment,
    ) -> Result<GroupConsumerMembershipTurn, ClassicGroupExecutionError> {
        if self.entries.iter().any(|entry| entry.fault.is_some()) {
            return Err(ClassicGroupExecutionError::EntryFault);
        }
        let mut driver_owned_close = false;
        for entry in &mut self.entries {
            if entry.state != GroupConsumerEntryState::Closing {
                continue;
            }
            match entry
                .execution
                .close_if_local(&mut entry.classic, &mut entry.catalog)?
            {
                ClassicGroupCloseProgress::Progress => {
                    return Ok(GroupConsumerMembershipTurn::Progress);
                }
                ClassicGroupCloseProgress::DriverOwned => driver_owned_close = true,
                ClassicGroupCloseProgress::AlreadyClosed => {}
            }
        }
        for entry in &mut self.entries {
            if entry.execution.expire_if_due(&mut entry.classic, now)? {
                return Ok(GroupConsumerMembershipTurn::Progress);
            }
        }
        Ok(if driver_owned_close {
            GroupConsumerMembershipTurn::Blocked
        } else {
            GroupConsumerMembershipTurn::Idle
        })
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
        let recovery = recovery_unsettled_count(
            self.join_shutdown_recovery.as_ref(),
            self.sync_shutdown_recovery.as_ref(),
            self.join_recovery_fault.as_ref(),
            self.sync_recovery_fault.as_ref(),
        );
        entries
            .saturating_add(joins)
            .saturating_add(syncs)
            .saturating_add(recovery)
    }

    pub(super) fn membership_next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        self.entries
            .iter()
            .filter_map(|entry| entry.execution.next_deadline())
            .min()
    }
}

impl GroupConsumerEntry {
    fn membership_unsettled(&self) -> usize {
        if let Some(fault) = &self.fault {
            return fault
                .retained_owner_count()
                .saturating_add(self.execution.unsettled());
        }
        if self.state == GroupConsumerEntryState::Closing
            && self.classic.machine().phase() != ClassicGroupPhase::Closed
        {
            return 1;
        }
        self.execution.unsettled()
    }
}
