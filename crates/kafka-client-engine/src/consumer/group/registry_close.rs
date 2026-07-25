//! Per-group close fences and whole-registry shutdown ownership.

use kafka_client_core::GroupId;

use crate::completion::NotifierJoin;

use super::{
    registry::GroupConsumerRegistry, registry_entry::GroupConsumerEntryState,
    registry_host::GroupConsumerHostError,
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
        let offset_commits = &mut self.offset_commits;
        offset_commits
            .recover_after_driver_shutdown()
            .map_err(GroupConsumerHostError::from)
    }

    pub(crate) fn finish_shutdown(&mut self) -> Result<NotifierJoin, GroupConsumerHostError> {
        let offset_commits = &mut self.offset_commits;
        offset_commits
            .finish_shutdown()
            .map_err(GroupConsumerHostError::from)
    }
}

fn mark_closing(entry: &mut super::registry_entry::GroupConsumerEntry) {
    entry.state = GroupConsumerEntryState::Closing;
}
