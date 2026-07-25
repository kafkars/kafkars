//! One bounded per-entry membership timeout or close transition per registry turn.

use kafka_client_core::{ClassicGroupPhase, Moment};

use super::{
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_execution_close::ClassicGroupCloseProgress,
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
    ) -> Result<GroupConsumerMembershipTurn, ClassicGroupExecutionError> {
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
        self.entries
            .iter()
            .map(GroupConsumerEntry::membership_unsettled)
            .sum()
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
        if self.state == GroupConsumerEntryState::Closing
            && self.classic.machine().phase() != ClassicGroupPhase::Closed
        {
            1
        } else {
            self.execution.unsettled()
        }
    }
}
