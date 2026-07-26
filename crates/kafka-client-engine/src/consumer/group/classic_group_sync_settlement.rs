//! One bounded Sync terminal interpretation, catalog install, or confirmation.

use kafka_client_core::Moment;

use crate::driver::classic_group::{SyncGroupPoll, SyncGroupTerminal, TrackedSyncGroupCalls};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_rediscovery_transfer::confirm_sync_rediscovery,
    classic_group_sync_interpret::interpret_sync, registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupSyncSettlementTurn {
    Idle,
    Progress,
}

impl GroupConsumerRegistry {
    pub(super) fn settle_one_classic_sync(
        &mut self,
        now: Moment,
    ) -> Result<ClassicGroupSyncSettlementTurn, ClassicGroupExecutionError> {
        let poll = self
            .sync_calls
            .as_mut()
            .ok_or(ClassicGroupExecutionError::CallRegistryUnavailable)?
            .poll_sync_group()
            .map_err(|_observation| ClassicGroupExecutionError::CallCompletion)?;
        let key = match poll {
            SyncGroupPoll::Idle => return Ok(ClassicGroupSyncSettlementTurn::Idle),
            SyncGroupPoll::TerminalReady { key } | SyncGroupPoll::ConfirmationPending { key } => {
                key
            }
        };
        let index = self
            .entries
            .iter()
            .position(|entry| entry.group_id() == key.group_id())
            .ok_or(ClassicGroupExecutionError::CallIdentityMismatch)?;
        let calls = self
            .sync_calls
            .as_mut()
            .ok_or(ClassicGroupExecutionError::CallRegistryUnavailable)?;
        let entry = &mut self.entries[index];
        let accepted = entry
            .execution
            .sync_driver_owner()
            .ok_or(ClassicGroupExecutionError::CallIdentityMismatch)?
            .accepted();
        if accepted.key() != key {
            return Err(ClassicGroupExecutionError::CallIdentityMismatch);
        }
        if matches!(poll, SyncGroupPoll::ConfirmationPending { .. }) {
            if entry.rediscovery.awaits_route_transfer() {
                let permit = self
                    .coordinator_invalidations
                    .as_mut()
                    .ok_or(ClassicGroupExecutionError::CallRegistryUnavailable)?
                    .try_reserve(key.group_id())
                    .map_err(|_error| ClassicGroupExecutionError::CoordinatorInvalidationReserve)?;
                confirm_sync_rediscovery(entry, calls, permit)?;
            } else {
                entry.execution.confirm_sync(calls)?;
            }
            return Ok(ClassicGroupSyncSettlementTurn::Progress);
        }
        let terminal = calls
            .begin_sync_group_settlement(accepted)
            .map_err(|_error| ClassicGroupExecutionError::CallIdentityMismatch)?;
        match interpret_sync(entry, now, terminal) {
            Ok(()) => Ok(ClassicGroupSyncSettlementTurn::Progress),
            Err(error) => {
                let (kind, terminal) = error.into_parts();
                if let Some(terminal) = terminal {
                    restore_terminal(entry, calls, terminal)?;
                }
                Err(kind)
            }
        }
    }
}

fn restore_terminal(
    entry: &mut GroupConsumerEntry,
    calls: &mut TrackedSyncGroupCalls,
    terminal: SyncGroupTerminal,
) -> Result<(), ClassicGroupExecutionError> {
    match calls.restore_sync_group_settlement(terminal) {
        Ok(()) => Ok(()),
        Err(failure) => {
            entry.fault = Some(ClassicGroupEntryFault::SyncTerminal(failure));
            Err(ClassicGroupExecutionError::CallIdentityMismatch)
        }
    }
}
