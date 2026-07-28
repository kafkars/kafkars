//! Per-group close fences and whole-registry shutdown ownership.

use kafka_client_core::{GroupId, Moment};

use crate::completion::NotifierJoin;

use super::{
    registry::GroupConsumerRegistry, registry_entry::GroupConsumerEntryState,
    registry_host_error::GroupConsumerHostError, registry_membership::GroupConsumerMembershipTurn,
};

/// A requested group close could not move an active entry to closing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerCloseError {
    UnknownGroup,
    AlreadyClosing,
}

impl GroupConsumerRegistry {
    pub(super) fn close_group(&mut self, group_id: GroupId) -> Result<(), GroupConsumerCloseError> {
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .ok_or(GroupConsumerCloseError::UnknownGroup)?;
        if !entry.is_active() {
            return Err(GroupConsumerCloseError::AlreadyClosing);
        }
        mark_closing(entry);
        Ok(())
    }

    pub(crate) fn close_admission(&mut self) {
        self.accepting = false;
        for entry in &mut self.entries {
            mark_closing(entry);
        }
        let offset_commits = &mut self.offset_commits;
        offset_commits.close_admission();
    }

    pub(crate) fn recover_after_driver_shutdown(&mut self) -> Result<(), GroupConsumerHostError> {
        self.close_admission();
        let membership = match self.recover_classic_calls_after_driver_shutdown() {
            Ok(()) => self.recover_local_membership().err(),
            Err(error) => Some(error),
        };
        if membership.is_none() {
            self.recover_fetch_after_driver_shutdown();
        }
        let offset_commits = &mut self.offset_commits;
        let offset_commit = offset_commits
            .recover_after_driver_shutdown()
            .err()
            .map(GroupConsumerHostError::from);
        membership.map_or_else(
            || offset_commit.map_or(Ok(()), Err),
            |error| Err(GroupConsumerHostError::membership(error)),
        )
    }

    pub(crate) fn finish_shutdown(&mut self) -> Result<NotifierJoin, GroupConsumerHostError> {
        let membership = self
            .membership_unsettled()
            .saturating_add(self.position_unsettled());
        if membership != 0 {
            return Err(GroupConsumerHostError::membership_unsettled(membership));
        }
        let fetch = self.fetch_unsettled();
        if fetch != 0 {
            return Err(GroupConsumerHostError::fetch_unsettled(fetch));
        }
        let processing = self.processing_unsettled();
        if processing != 0 {
            return Err(GroupConsumerHostError::processing_unsettled(processing));
        }
        let offset_commits = &mut self.offset_commits;
        offset_commits
            .finish_shutdown()
            .map_err(GroupConsumerHostError::from)
    }

    fn recover_local_membership(
        &mut self,
    ) -> Result<(), super::classic_group_execution::ClassicGroupExecutionError> {
        let turn_limit = self.entries.len().saturating_mul(2).saturating_add(1);
        for _turn in 0..turn_limit {
            match self.turn_local_membership(Moment::from_tick(u64::MAX))? {
                GroupConsumerMembershipTurn::Progress => {}
                GroupConsumerMembershipTurn::Idle | GroupConsumerMembershipTurn::Blocked => {
                    break;
                }
            }
        }
        if self.position_unsettled() != 0 {
            return Err(
                super::classic_group_execution::ClassicGroupExecutionError::PositionPending,
            );
        }
        let retained_entry_faults = self
            .entries
            .iter()
            .filter_map(|entry| entry.fault.as_ref())
            .map(super::classic_group_entry_fault::ClassicGroupEntryFault::retained_owner_count)
            .sum::<usize>();
        if self
            .membership_unsettled()
            .saturating_sub(retained_entry_faults)
            != 0
        {
            return Err(
                super::classic_group_execution::ClassicGroupExecutionError::HandoffIncomplete,
            );
        }
        Ok(())
    }
}

fn mark_closing(entry: &mut super::registry_entry::GroupConsumerEntry) {
    entry.state = GroupConsumerEntryState::Closing;
}
