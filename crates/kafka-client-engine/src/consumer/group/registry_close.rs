//! Per-group close fences and whole-registry shutdown ownership.

use std::sync::Arc;

use kafka_client_core::{ClassicGroupPhase, GroupId, Moment};

use crate::{clock::OperationDeadline, completion::NotifierJoin};

use super::{
    classic_group_leave::GroupConsumerCloseCompletion, registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntryState, registry_host_error::GroupConsumerHostError,
    registry_membership::GroupConsumerMembershipTurn,
};

/// A requested group close could not move an active entry to closing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupRegistryCloseError {
    UnknownGroup,
    AlreadyClosing,
    EntryFault,
}

/// Exact invariant preventing physical release of a drained group entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerRemovalError {
    RetainedBytesInvariant,
    TerminalInvariant,
}

impl GroupConsumerRegistry {
    pub(super) fn close_group(&mut self, group_id: GroupId) -> Result<(), GroupRegistryCloseError> {
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .ok_or(GroupRegistryCloseError::UnknownGroup)?;
        if !entry.is_active() {
            return Err(GroupRegistryCloseError::AlreadyClosing);
        }
        mark_closing(entry);
        Ok(())
    }

    pub(super) fn close_group_explicit(
        &mut self,
        group_id: GroupId,
        deadline: OperationDeadline,
        completion: Arc<GroupConsumerCloseCompletion>,
    ) -> Result<(), GroupRegistryCloseError> {
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .ok_or(GroupRegistryCloseError::UnknownGroup)?;
        if entry.state != GroupConsumerEntryState::Active {
            return Err(GroupRegistryCloseError::AlreadyClosing);
        }
        if entry.leave.begin(deadline, completion).is_err() {
            return Err(GroupRegistryCloseError::EntryFault);
        }
        mark_closing(entry);
        Ok(())
    }

    /// Physically releases at most one fully drained explicit-close entry.
    pub(super) fn remove_one_closed_group(&mut self) -> Result<bool, GroupConsumerRemovalError> {
        let Some(index) = self.entries.iter().position(group_close_is_drained) else {
            return Ok(false);
        };
        let bytes = self.entries[index].group_bytes();
        let retained_group_bytes = self
            .retained_group_bytes
            .checked_sub(bytes)
            .ok_or(GroupConsumerRemovalError::RetainedBytesInvariant)?;
        let mut removed = self.entries.remove(index);
        debug_assert_eq!(removed.group_bytes(), bytes);
        if !removed.leave.publish_terminal() {
            self.entries.insert(index, removed);
            return Err(GroupConsumerRemovalError::TerminalInvariant);
        }
        self.retained_group_bytes = retained_group_bytes;
        Ok(true)
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
        self.recover_classic_group_leaves_after_driver_shutdown();
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
        while self
            .remove_one_closed_group()
            .map_err(GroupConsumerHostError::close)?
        {}
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
        let revocation = self.graceful_revocation_unsettled();
        if revocation != 0 {
            return Err(GroupConsumerHostError::graceful_revocation_unsettled(
                revocation,
            ));
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
    let _lost = entry.revocation.lose_owner();
}

fn group_close_is_drained(entry: &super::registry_entry::GroupConsumerEntry) -> bool {
    entry.state == GroupConsumerEntryState::Closing
        && entry.classic.machine().phase() == ClassicGroupPhase::Closed
        && entry.classic.pending().is_none()
        && entry.catalog.live_assignment().is_none()
        && entry.execution.is_idle()
        && entry.heartbeat.is_dormant()
        && entry.position.is_dormant()
        && entry.processing_lease.active_schedule().is_none()
        && entry.processing_lease.pending_expiration().is_none()
        && entry.rejoin.is_dormant()
        && !entry.rediscovery.blocks_join()
        && entry.fetch.is_idle()
        && entry.leave.allows_local_close()
        && entry.revocation.is_dormant()
}
