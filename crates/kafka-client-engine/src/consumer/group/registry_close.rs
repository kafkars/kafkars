//! Per-group close fences and whole-registry shutdown ownership.

use kafka_client_core::{GroupId, Moment};

use crate::completion::NotifierJoin;

use super::{
    registry::GroupConsumerRegistry, registry_entry::GroupConsumerEntryState,
    registry_host::GroupConsumerHostError, registry_membership::GroupConsumerMembershipTurn,
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
        let membership = self.recover_local_membership().err();
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
        let membership = self.membership_unsettled();
        if membership != 0 {
            return Err(GroupConsumerHostError::membership_unsettled(membership));
        }
        let offset_commits = &mut self.offset_commits;
        offset_commits
            .finish_shutdown()
            .map_err(GroupConsumerHostError::from)
    }

    fn recover_local_membership(
        &mut self,
    ) -> Result<(), super::classic_group_execution::ClassicGroupExecutionError> {
        let turn_limit = self.entries.len().saturating_add(1);
        for _turn in 0..turn_limit {
            match self.turn_membership(Moment::from_tick(u64::MAX))? {
                GroupConsumerMembershipTurn::Progress => {}
                GroupConsumerMembershipTurn::Idle | GroupConsumerMembershipTurn::Blocked => {
                    break;
                }
            }
        }
        Ok(())
    }
}

fn mark_closing(entry: &mut super::registry_entry::GroupConsumerEntry) {
    entry.state = GroupConsumerEntryState::Closing;
}
